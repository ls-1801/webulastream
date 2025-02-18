use distributed::*;
use lazy_static::lazy_static;
use std::sync::Arc;
use tracing::info;
#[cxx::bridge]
pub mod ffi {
    unsafe extern "C++" {
        include!("Bridge.hpp");
        type Emitter;
        fn emit(emitter: &Emitter, data: String);
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

        fn enable_logging();
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

fn registerReceiverChannel(
    server: &ReceiverServer,
    channel_identifier: String,
    emitter: cxx::UniquePtr<ffi::Emitter>,
) {
    info!("registerReceiverChannel({})", channel_identifier);
}
fn closeReceiverChannel(server: &ReceiverServer, channel_identifier: String) {
    info!("Closing receiver channel {}", channel_identifier);
}
fn registerSenderChannel(server: &SenderServer, channel_identifier: String) -> Box<Channel> {
    Box::new(Channel {})
}
fn closeSenderChannel(server: &SenderServer, channel_identifier: String) {
    info!("Closing sender channel {}", channel_identifier);
}

fn enable_logging() {
    tracing_subscriber::fmt().init();
}