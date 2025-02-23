use async_channel::TrySendError;
use distributed::protocol::{ChannelIdentifier, TupleBuffer};
use distributed::*;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use tokio_util::bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tracing::info;

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

        fn receiver_instance() -> Box<ReceiverServer>;
        fn sender_instance() -> Box<SenderServer>;

        fn enable_logging();

        fn register_receiver_channel(
            server: &mut ReceiverServer,
            channel_identifier: String,
        ) -> Box<ReceiverChannel>;
        fn receive_buffer(
            receiver_channel: &mut ReceiverChannel,
            builder: Pin<&mut TupleBufferBuilder>,
        ) -> Result<()>;

        fn close_receiver_channel(server: &mut ReceiverServer, channel_identifier: String);

        fn register_sender_channel(
            server: &SenderServer,
            connection_identifier: u16,
            channel_identifier: String,
        ) -> Box<SenderChannel>;

        fn close_sender_channel(channel: Box<SenderChannel>);
        fn sender_writes_pending(channel: &SenderChannel) -> bool;
        fn send_channel(
            channel: &SenderChannel,
            metadata: SerializedTupleBuffer,
            data: &[u8],
            children: &[&[u8]],
        ) -> SendResult;
    }
}
lazy_static! {
    static ref SENDER: Arc<sender::NetworkService> = {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .thread_name("sender")
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        sender::NetworkService::start(rt)
    };
    static ref RECEIVER: Arc<receiver::NetworkService> = {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .thread_name("receiver")
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        receiver::NetworkService::start(rt, 9090)
    };
}
pub struct ReceiverServer {
    handle: Arc<receiver::NetworkService>,
    cancellation_tokens: HashMap<ChannelIdentifier, CancellationToken>,
}
struct SenderServer {
    handle: Arc<sender::NetworkService>,
}
struct SenderChannel {
    cancellation_token: CancellationToken,
    data_queue: async_channel::Sender<TupleBuffer>,
}

struct ReceiverChannel {
    cancellation_token: CancellationToken,
    data_queue: Box<async_channel::Receiver<TupleBuffer>>,
}

fn receiver_instance() -> Box<ReceiverServer> {
    Box::new(ReceiverServer {
        handle: RECEIVER.clone(),
        cancellation_tokens: HashMap::new(),
    })
}
fn sender_instance() -> Box<SenderServer> {
    Box::new(SenderServer {
        handle: SENDER.clone(),
    })
}

fn register_receiver_channel(
    server: &mut ReceiverServer,
    channel_identifier: String,
) -> Box<ReceiverChannel> {
    info!("register_receiver_channel({})", channel_identifier);
    let (queue, token) = server
        .handle
        .register_channel(channel_identifier.clone())
        .unwrap();

    Box::new(ReceiverChannel {
        cancellation_token: token,
        data_queue: Box::new(queue),
    })
}

fn receive_buffer(
    receiver_channel: &mut ReceiverChannel,
    mut builder: Pin<&mut ffi::TupleBufferBuilder>,
) -> Result<(), Box<dyn Error>> {
    let buffer = receiver_channel.data_queue.recv_blocking()?;
    builder.as_mut().set_metadata(&ffi::SerializedTupleBuffer {
        sequence_number: buffer.sequence_number as usize,
        origin_id: buffer.origin_id as usize,
        chunk_number: buffer.chunk_number as usize,
        number_of_tuples: buffer.number_of_tuples as usize,
        last_chunk: buffer.last_chunk,
    });

    builder.as_mut().set_data(&buffer.data);
    
    for child_buffer in buffer.child_buffers.iter() {
        assert!(!child_buffer.is_empty());
        builder.as_mut().add_child_buffer(child_buffer);
    }

    Ok(())
}
fn close_receiver_channel(server: &mut ReceiverServer, channel_identifier: String) {
    server
        .cancellation_tokens
        .remove(&channel_identifier)
        .unwrap()
        .cancel();
}
fn register_sender_channel(
    server: &SenderServer,
    connection_identifier: u16,
    channel_identifier: String,
) -> Box<SenderChannel> {
    let (cancellation_token, data_queue) = server
        .handle
        .register_channel(connection_identifier, channel_identifier)
        .unwrap();
    Box::new(SenderChannel {
        cancellation_token,
        data_queue,
    })
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
        last_chunk: metadata.last_chunk,
        data: Vec::from(data),
        child_buffers: children
            .iter()
            .map(|bytes| Vec::from(*bytes))
            .collect(),
    };
    match channel.data_queue.try_send(buffer) {
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
fn close_sender_channel(channel: Box<SenderChannel>) {
    channel.cancellation_token.cancel();
    while !(channel.data_queue.is_closed() || channel.data_queue.is_empty()) {}
}

fn enable_logging() {
    tracing_subscriber::fmt::init();
}
