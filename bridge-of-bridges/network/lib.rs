use async_channel::TrySendError;
use nes_network::protocol::{ConnectionIdentifier, TupleBuffer};
use nes_network::receiver::network_service::ReceiverNetworkService;
use nes_network::sender::data_channel_handler::{ChannelControlMessage, ChannelControlQueue};
use nes_network::sender::network_service::SenderNetworkService;
use once_cell::sync;
use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;

const NUM_WORKER_THREADS_IN_SENDER: usize = 2;
const NUM_WORKER_THREADS_IN_RECEIVER: usize = 2;

#[cxx::bridge]
pub mod ffi {
    enum SendResult {
        Ok,
        Error,
        Full,
    }
    struct SerializedTupleBufferHeader {
        sequence_number: usize,
        origin_id: usize,
        chunk_number: usize,
        number_of_tuples: usize,
        watermark: usize,
        last_chunk: bool,
    }

    unsafe extern "C++" {
        include!("Bridge.hpp");
        type TupleBufferBuilder;
        fn set_metadata(self: Pin<&mut TupleBufferBuilder>, meta: &SerializedTupleBufferHeader);
        fn set_data(self: Pin<&mut TupleBufferBuilder>, data: &[u8]);
        fn add_child_buffer(self: Pin<&mut TupleBufferBuilder>, data: &[u8]);
    }

    // Functions that can be called from C++ via FFI
    extern "Rust" {
        type ReceiverService;
        type SenderService;
        type SenderChannel;
        type ReceiverChannel;

        // Lazily initialize sender/receiver on first usage request
        fn init_sender_service();
        fn init_receiver_service(connection_id: String);
        
        fn sender_instance() -> Result<Box<SenderService>>;
        fn receiver_instance() -> Result<Box<ReceiverService>>;

        // Receiver
        fn register_receiver_channel(
            server: &mut ReceiverService,
            channel_id: String,
        ) -> Box<ReceiverChannel>;
        fn interrupt_receiver(receiver_channel: &ReceiverChannel) -> bool;
        fn receive_buffer(
            receiver_channel: &ReceiverChannel,
            builder: Pin<&mut TupleBufferBuilder>,
        ) -> bool;
        fn close_receiver_channel(channel: Box<ReceiverChannel>);

        // Sender
        fn register_sender_channel(
            server: &SenderService,
            connection_id: String,
            channel_id: String,
        ) -> Box<SenderChannel>;
        fn send_buffer(
            channel: &SenderChannel,
            metadata: SerializedTupleBufferHeader,
            data: &[u8],
            children: &[&[u8]],
        ) -> SendResult;
        fn sender_has_pending_writes(channel: &SenderChannel) -> bool;
        fn flush_channel(channel: &SenderChannel);
        fn close_sender_channel(channel: Box<SenderChannel>);
    }
}

static RECEIVER: sync::OnceCell<Arc<ReceiverNetworkService>> = sync::OnceCell::new();
static SENDER: sync::OnceCell<Arc<SenderNetworkService>> = sync::OnceCell::new();

pub struct ReceiverService {
    handle: Arc<ReceiverNetworkService>,
}
struct SenderService {
    handle: Arc<SenderNetworkService>,
}

struct SenderChannel {
    data_queue: ChannelControlQueue,
}
struct ReceiverChannel {
    data_queue: Box<async_channel::Receiver<TupleBuffer>>,
}

fn init_sender_service() {
    SENDER.get_or_init(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .thread_name("net-receiver")
            .worker_threads(NUM_WORKER_THREADS_IN_SENDER)
            .enable_io()
            .enable_time()
            .build()
            .expect("Cannot create tokio runtime");
        SenderNetworkService::start(rt)
    });
}
fn init_receiver_service(connection_id: ConnectionIdentifier) {
    RECEIVER.get_or_init(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .thread_name("net-sender")
            .worker_threads(NUM_WORKER_THREADS_IN_RECEIVER)
            .enable_io()
            .enable_time()
            .build()
            .expect("Cannot create tokio runtime");
        ReceiverNetworkService::start(rt, connection_id)
    });
}

fn receiver_instance() -> Result<Box<ReceiverService>, Box<dyn Error>> {
    Ok(Box::new(ReceiverService {
        handle: RECEIVER
            .get()
            .ok_or("Receiver server has not been initialized yet.")?
            .clone(),
    }))
}
fn sender_instance() -> Result<Box<SenderService>, Box<dyn Error>> {
    Ok(Box::new(SenderService {
        handle: SENDER
            .get()
            .ok_or("Sender server has not been initialized yet.")?
            .clone(),
    }))
}

// ========================================= RECEIVER ==============================================

// Forward a register channel request to the receiver service.
// Upon successful registration, the register_channel fn will return the receiving end of a queue
// to receive TupleBuffer's from, and return this in a Box.
fn register_receiver_channel(
    receiver_service: &mut ReceiverService,
    channel_id: String,
) -> Box<ReceiverChannel> {
    let queue = receiver_service
        .handle
        .register_channel(channel_id.clone())
        .unwrap();

    Box::new(ReceiverChannel {
        data_queue: Box::new(queue),
    })
}

// Interrupt the receiver by closing the receiving end of the data queue.
// In the corresponding data channel handler, the stop signal will be sent back to the upstream host
// This is a graceful stop, meaning that more in-flight tuple buffers may arrive before the data 
// channel is actually closed.
fn interrupt_receiver(receiver_channel: &ReceiverChannel) -> bool {
    receiver_channel.data_queue.close()
}

// Receive a buffer from the data queue and place its data into the builder
fn receive_buffer(
    receiver_channel: &ReceiverChannel,
    mut builder: Pin<&mut ffi::TupleBufferBuilder>,
) -> bool {
    let Ok(buffer) = receiver_channel.data_queue.recv_blocking() else {
        return false;
    };

    builder.as_mut().set_metadata(&ffi::SerializedTupleBufferHeader {
        sequence_number: buffer.sequence_number as usize,
        origin_id: buffer.origin_id as usize,
        watermark: buffer.watermark as usize,
        chunk_number: buffer.chunk_number as usize,
        number_of_tuples: buffer.number_of_tuples as usize,
        last_chunk: buffer.last_chunk,
    });

    builder.as_mut().set_data(&buffer.data);

    for child_buffer in buffer.child_buffers.iter() {
        assert!(!child_buffer.is_empty());
        builder.as_mut().add_child_buffer(child_buffer);
    }

    true
}

// CXX requires the usage of Boxed types
#[allow(clippy::boxed_local)]
fn close_receiver_channel(channel: Box<ReceiverChannel>) {
    channel.data_queue.close();
}

// =========================================== SENDER ==============================================

fn register_sender_channel(
    server: &SenderService,
    connection_id: String,
    channel_id: String,
) -> Box<SenderChannel> {
    let data_queue = server
        .handle
        .register_channel(connection_id, channel_id)
        .unwrap();
    Box::new(SenderChannel { data_queue })
}

// Attempt to send a buffer to the internal async channel
// 
fn send_buffer(
    channel: &SenderChannel,
    metadata: ffi::SerializedTupleBufferHeader,
    data: &[u8],
    children: &[&[u8]],
) -> ffi::SendResult {
    let buffer = TupleBuffer {
        sequence_number: metadata.sequence_number as u64,
        origin_id: metadata.origin_id as u64,
        chunk_number: metadata.chunk_number as u64,
        number_of_tuples: metadata.number_of_tuples as u64,
        watermark: metadata.watermark as u64,
        last_chunk: metadata.last_chunk,
        data: Vec::from(data),
        child_buffers: children.iter().map(|bytes| Vec::from(*bytes)).collect(),
    };
    match channel
        .data_queue
        .try_send(ChannelControlMessage::Data(buffer))
    {
        Ok(()) => ffi::SendResult::Ok,
        Err(TrySendError::Full(_)) => ffi::SendResult::Full,
        Err(TrySendError::Closed(_)) => ffi::SendResult::Error,
    }
}

fn sender_has_pending_writes(channel: &SenderChannel) -> bool {
    if channel.data_queue.is_closed() {
        return false;
    }
    !channel.data_queue.is_empty()
}

fn flush_channel(channel: &SenderChannel) {
    let (tx, _) = tokio::sync::oneshot::channel();
    let _ = channel
        .data_queue
        .send_blocking(ChannelControlMessage::Flush(tx));
}

// CXX requires the usage of Boxed types
#[allow(clippy::boxed_local)]
fn close_sender_channel(channel: Box<SenderChannel>) {
    let (tx, rx) = tokio::sync::oneshot::channel();

    if channel
        .data_queue
        .send_blocking(ChannelControlMessage::Flush(tx))
        .is_err()
    {
        // already terminated
        return;
    }

    let _ = rx.blocking_recv();
    let _ = channel
        .data_queue
        .send_blocking(ChannelControlMessage::Terminate);
}
