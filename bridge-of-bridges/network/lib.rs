use async_channel::TrySendError;
use nes_network::protocol::{ConnectionIdentifier, TupleBuffer};
use nes_network::receiver::receiver::ReceiverNetworkService;
use nes_network::sender::data_channel_handler::{ChannelControlMessage, ChannelControlQueue};
use nes_network::sender::network_service::SenderNetworkService;
use once_cell::sync;
use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;

#[cxx::bridge]
pub mod ffi {
    enum SendResult {
        Ok,
        Error,
        Full,
    }
    struct SerializedTupleBuffer {
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
        fn set_metadata(self: Pin<&mut TupleBufferBuilder>, meta: &SerializedTupleBuffer);
        fn set_data(self: Pin<&mut TupleBufferBuilder>, data: &[u8]);
        fn add_child_buffer(self: Pin<&mut TupleBufferBuilder>, data: &[u8]);
    }

    extern "Rust" {
        type ReceiverServer;
        type SenderServer;
        type SenderChannel;
        type ReceiverChannel;

        fn receiver_instance() -> Result<Box<ReceiverServer>>;
        fn init_receiver_server(connection_identifier: String);
        fn init_sender_server();
        fn sender_instance() -> Result<Box<SenderServer>>;

        fn register_receiver_channel(
            server: &mut ReceiverServer,
            channel_identifier: String,
        ) -> Box<ReceiverChannel>;
        fn interrupt_receiver(receiver_channel: &ReceiverChannel) -> bool;
        fn receive_buffer(
            receiver_channel: &ReceiverChannel,
            builder: Pin<&mut TupleBufferBuilder>,
        ) -> bool;

        fn close_receiver_channel(channel: Box<ReceiverChannel>);

        fn register_sender_channel(
            server: &SenderServer,
            connection_identifier: String,
            channel_identifier: String,
        ) -> Box<SenderChannel>;

        fn close_sender_channel(channel: Box<SenderChannel>);
        fn sender_writes_pending(channel: &SenderChannel) -> bool;

        fn flush_channel(channel: &SenderChannel);
        fn send_channel(
            channel: &SenderChannel,
            metadata: SerializedTupleBuffer,
            data: &[u8],
            children: &[&[u8]],
        ) -> SendResult;
    }
}

static RECEIVER: sync::OnceCell<Arc<ReceiverNetworkService>> = sync::OnceCell::new();
static SENDER: sync::OnceCell<Arc<SenderNetworkService>> = sync::OnceCell::new();

pub struct ReceiverServer {
    handle: Arc<ReceiverNetworkService>,
}
struct SenderServer {
    handle: Arc<SenderNetworkService>,
}
struct SenderChannel {
    data_queue: ChannelControlQueue,
}

struct ReceiverChannel {
    data_queue: Box<async_channel::Receiver<TupleBuffer>>,
}

fn init_sender_server() {
    SENDER.get_or_init(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .thread_name("net-receiver")
            .worker_threads(2)
            .enable_io()
            .enable_time()
            .worker_threads(2)
            .build()
            .unwrap();
        SenderNetworkService::start(rt)
    });
}
fn init_receiver_server(connection_identifier: ConnectionIdentifier) {
    RECEIVER.get_or_init(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .thread_name("net-sender")
            .worker_threads(2)
            .enable_io()
            .worker_threads(2)
            .enable_time()
            .build()
            .unwrap();
        ReceiverNetworkService::start(rt, connection_identifier)
    });
}

fn receiver_instance() -> Result<Box<ReceiverServer>, Box<dyn Error>> {
    Ok(Box::new(ReceiverServer {
        handle: RECEIVER
            .get()
            .ok_or("Receiver server has not been initialized yet.")?
            .clone(),
    }))
}
fn sender_instance() -> Result<Box<SenderServer>, Box<dyn Error>> {
    Ok(Box::new(SenderServer {
        handle: SENDER
            .get()
            .ok_or("Sender server has not been initialized yet.")?
            .clone(),
    }))
}

fn register_receiver_channel(
    server: &mut ReceiverServer,
    channel_identifier: String,
) -> Box<ReceiverChannel> {
    let queue = server
        .handle
        .register_channel(channel_identifier.clone())
        .unwrap();

    Box::new(ReceiverChannel {
        data_queue: Box::new(queue),
    })
}

fn interrupt_receiver(receiver_channel: &ReceiverChannel) -> bool {
    receiver_channel.data_queue.close()
}

fn receive_buffer(
    receiver_channel: &ReceiverChannel,
    mut builder: Pin<&mut ffi::TupleBufferBuilder>,
) -> bool {
    let Ok(buffer) = receiver_channel.data_queue.recv_blocking() else {
        return false;
    };

    builder.as_mut().set_metadata(&ffi::SerializedTupleBuffer {
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
fn register_sender_channel(
    server: &SenderServer,
    connection_identifier: String,
    channel_identifier: String,
) -> Box<SenderChannel> {
    let data_queue = server
        .handle
        .register_channel(connection_identifier, channel_identifier)
        .unwrap();
    Box::new(SenderChannel { data_queue })
}
fn send_channel(
    channel: &SenderChannel,
    metadata: ffi::SerializedTupleBuffer,
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
fn sender_writes_pending(channel: &SenderChannel) -> bool {
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
