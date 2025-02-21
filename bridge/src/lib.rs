use crate::ffi::{emit, SerializedTupleBuffer};
use distributed::protocol::{ChannelIdentifier, ConnectionIdentifier, TupleBuffer};
use distributed::sender::DataQueue;
use distributed::*;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::error::Error;
use std::str::from_utf8;
use std::sync::Arc;
use tokio_util::bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tracing::info;

use std::io;

use tracing::{subscriber::fmt, Level};
use tracing_subscriber::fmt::SubscriberBuilder;
use chrono::Utc;


#[cxx::bridge]
pub mod ffi {
    struct SerializedTupleBuffer {
        sequence_number: usize,
        origin_id: usize,
        chunk_number: usize,
        number_of_tuples: usize,
        last_chunk: bool,
    }

    unsafe extern "C++" {
        include!("Bridge.hpp");
        type Emitter;
        fn emit(
            emitter: Pin<&mut Emitter>,
            metadata: SerializedTupleBuffer,
            data: &[u8],
            children: &[&[u8]],
        ) -> bool;
    }
    extern "Rust" {
        type ReceiverServer;
        type SenderServer;
        type Channel;
        fn receiverInstance() -> Box<ReceiverServer>;
        fn senderInstance() -> Box<SenderServer>;

        fn registerReceiverChannel(
            server: &mut ReceiverServer,
            channel_identifier: String,
            emitter: UniquePtr<Emitter>,
        );
        fn closeReceiverChannel(server: &mut ReceiverServer, channel_identifier: String);
        fn registerSenderChannel(
            server: &SenderServer,
            connection_identifier: u16,
            channel_identifier: String,
        ) -> Box<Channel>;
        fn closeSenderChannel(channel: Box<Channel>);
        fn sendChannel(
            channel: &Channel,
            metadata: SerializedTupleBuffer,
            data: &[u8],
            children: &[&[u8]],
        ) -> Result<()>;

        fn enable_logging();
    }
}
unsafe impl Send for ffi::Emitter {}
unsafe impl Sync for ffi::Emitter {}

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
struct Channel {
    cancellation_token: CancellationToken,
    data_queue: DataQueue,
}
fn receiverInstance() -> Box<ReceiverServer> {
    Box::new(ReceiverServer {
        handle: RECEIVER.clone(),
        cancellation_tokens: HashMap::new(),
    })
}
fn senderInstance() -> Box<SenderServer> {
    Box::new(SenderServer {
        handle: SENDER.clone(),
    })
}

fn sendChannel(
    channel: &Channel,
    metadata: SerializedTupleBuffer,
    data: &[u8],
    children: &[&[u8]],
) -> Result<(), Box<dyn Error>> {
    info!("Sending data through channel");
    let buffer = TupleBuffer {
        sequence_number: metadata.sequence_number as u64,
        origin_id: metadata.origin_id as u64,
        chunk_number: metadata.chunk_number as u64,
        number_of_tuples: metadata.number_of_tuples as u64,
        last_chunk: metadata.last_chunk,
        data: Bytes::copy_from_slice(data),
        child_buffers: children
            .iter()
            .map(|bytes| Bytes::copy_from_slice(bytes))
            .collect(),
    };
    channel.data_queue.try_send(buffer)?;
    Ok(())
}

fn registerReceiverChannel(
    server: &mut ReceiverServer,
    channel_identifier: String,
    mut emitter: cxx::UniquePtr<ffi::Emitter>,
) {
    info!("registerReceiverChannel({})", channel_identifier);
    let token = server
        .handle
        .register_channel(
            channel_identifier.clone(),
            Box::new(move |buffer| {
                let meta = ffi::SerializedTupleBuffer {
                    sequence_number: buffer.sequence_number as usize,
                    origin_id: buffer.origin_id as usize,
                    chunk_number: buffer.chunk_number as usize,
                    number_of_tuples: buffer.number_of_tuples as usize,
                    last_chunk: buffer.last_chunk,
                };

                let child_buffers: Vec<_> = buffer
                    .child_buffers
                    .iter()
                    .map(|buffer| buffer.as_ref())
                    .collect();

                emit(emitter.pin_mut(), meta, &buffer.data, &child_buffers)
            }),
        )
        .unwrap();
    server.cancellation_tokens.insert(channel_identifier, token);
}
fn closeReceiverChannel(server: &mut ReceiverServer, channel_identifier: String) {
    server
        .cancellation_tokens
        .remove(&channel_identifier)
        .unwrap()
        .cancel();
}
fn registerSenderChannel(
    server: &SenderServer,
    connection_identifier: u16,
    channel_identifier: String,
) -> Box<Channel> {
    let (cancellation_token, data_queue) = server
        .handle
        .register_channel(connection_identifier, channel_identifier)
        .unwrap();
    Box::new(Channel {
        cancellation_token,
        data_queue,
    })
}
fn closeSenderChannel(channel: Box<Channel>) {
    channel.cancellation_token.cancel();
    while !(channel.data_queue.is_closed() || channel.data_queue.is_empty()) {}
}

fn enable_logging() {
    SubscriberBuilder::default()
        .with_env("RUST_LOG=info")
        .with_timer("chrono")
        .fmt(
            "[{timestamp:.6?}] [{level}] [thread {thread_id}] [{file}:{line}] {message}",
        )
        .color_backtrace()
        .color_level()
        .init();
}
