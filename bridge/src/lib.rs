use crate::ffi::{emit, SerializedTupleBuffer};
use distributed::*;
use lazy_static::lazy_static;
use std::str::from_utf8;
use std::sync::Arc;
use tracing::info;

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
            server: &ReceiverServer,
            channel_identifier: String,
            emitter: UniquePtr<Emitter>,
        );
        fn closeReceiverChannel(server: &ReceiverServer, channel_identifier: String);
        fn registerSenderChannel(server: &SenderServer, channel_identifier: String)
            -> Box<Channel>;
        fn closeSenderChannel(server: &SenderServer, channel_identifier: String);
        fn sendChannel(
            channel: &Channel,
            metadata: SerializedTupleBuffer,
            data: &[u8],
            children: &[&[u8]],
        ) -> bool;

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
        receiver::NetworkService::start(rt, 8080)
    };
}
pub struct ReceiverServer {
    handle: Arc<receiver::NetworkService>,
}
struct SenderServer {
    handle: Arc<sender::NetworkService>,
}
struct Channel {}
fn receiverInstance() -> Box<ReceiverServer> {
    Box::new(ReceiverServer {
        handle: RECEIVER.clone(),
    })
}
fn senderInstance() -> Box<SenderServer> {
    Box::new(SenderServer {
        handle: SENDER.clone(),
    })
}

fn sendChannel(
    _channel: &Channel,
    _metadata: SerializedTupleBuffer,
    _data: &[u8],
    _children: &[&[u8]],
) -> bool {
    true
}

fn registerReceiverChannel(
    server: &ReceiverServer,
    channel_identifier: String,
    mut emitter: cxx::UniquePtr<ffi::Emitter>,
) {
    info!("registerReceiverChannel({})", channel_identifier);
    server
        .handle
        .register_channel(
            channel_identifier,
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
}
fn closeReceiverChannel(server: &ReceiverServer, channel_identifier: String) {}
fn registerSenderChannel(server: &SenderServer, channel_identifier: String) -> Box<Channel> {
    Box::new(Channel {})
}
fn closeSenderChannel(server: &SenderServer, channel_identifier: String) {
    info!("Closing sender channel {}", channel_identifier);
}

fn enable_logging() {
    tracing_subscriber::fmt().init();
}
