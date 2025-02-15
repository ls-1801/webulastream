mod engine;
mod network;

use crate::engine::{Data, ExecutablePipeline, Node, PipelineContext, SourceNode};
use crate::network::EmitFn;
use bytes::{Bytes, BytesMut};
use clap::Parser;
use log::warn;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::hash_map::OccupiedEntry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::from_utf8;
use std::sync;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info};

// mod inter_node {
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error>;
struct ChannelParameter {
    port: u16,
}

// #[derive(Debug)]
// struct Channel {
//     rx: tokio::sync::mpsc::Receiver<Data>,
// }
//
// struct ChannelHandle {
//     tx: tokio::sync::mpsc::Sender<Data>,
//     max_buffer_size: usize,
// }
//
// struct UpstreamChannel {}
//
// struct DownstreamChannel {}
//
// enum Commands {
//     Open(String, tokio::sync::oneshot::Sender<Channel>),
//     Close(Channel),
// }
//
// struct TransmitBuffer {
//     name: String,
//     buffer: BytesMut,
// }
//
// enum Control {
//     Register(
//         u16,
//         tokio::sync::oneshot::Sender<tokio::sync::mpsc::Sender<TransmitBuffer>>,
//     ),
//     Stop(String),
// }
//
// enum ChannelControl {
//     DataChannel(tokio::sync::oneshot::Sender<tokio::sync::mpsc::Sender<TransmitBuffer>>),
// }
// struct OutgoingServer {
//     register_tx: tokio::sync::mpsc::Sender<Control>,
//     runtime: Runtime,
// }
//
// async fn outgoing_server(
//     connection: u16,
//     mut data: Receiver<TransmitBuffer>,
//     control: Receiver<ChannelControl>,
// ) {
//     info!("I would send data to {connection} if someone implemented it");
//     loop {
//         if let Some(data) = data.recv().await {
//             info!(
//                 "I received data for {}. Message: {:?}",
//                 data.name, data.buffer
//             )
//         }
//     }
// }
//
// impl OutgoingServer {
//     fn run() -> Arc<OutgoingServer> {
//         let (mut tx, mut rx) = tokio::sync::mpsc::channel(100);
//
//         let rt = tokio::runtime::Builder::new_multi_thread()
//             .enable_io()
//             .build()
//             .unwrap();
//
//         rt.spawn(async move {
//             let mut channels: HashMap<
//                 u16,
//                 (
//                     tokio::sync::mpsc::Sender<TransmitBuffer>,
//                     tokio::sync::mpsc::Sender<ChannelControl>,
//                 ),
//             > = HashMap::default();
//             loop {
//                 if let Some(control) = rx.recv().await {
//                     match control {
//                         Control::Register(name, sender) => {
//                             let (data, _) = channels.entry(name).or_insert_with(|| {
//                                 let (mut tx_data, mut rx_data) = tokio::sync::mpsc::channel(100);
//                                 let (mut tx_control, mut rx_control) =
//                                     tokio::sync::mpsc::channel(100);
//                                 tokio::spawn(
//                                     async move { outgoing_server(name, rx_data, rx_control) },
//                                 );
//                                 (tx_data, tx_control)
//                             });
//                             sender.send(data.clone()).unwrap();
//                         }
//                         Control::Stop(_) => {}
//                     }
//                 };
//             }
//         });
//
//         Arc::new(OutgoingServer {
//             register_tx: tx,
//             runtime: rt,
//         })
//     }
//
//     fn register(self: Arc<OutgoingServer>, port: u16) -> Sender<TransmitBuffer> {
//         let (tx, rx) = tokio::sync::oneshot::channel();
//         self.runtime
//             .block_on(self.register_tx.send(Control::Register(port, tx)))
//             .unwrap();
//         rx.blocking_recv().unwrap()
//     }
// }
//
// struct IncomingServer {
//     command: tokio::sync::mpsc::Sender<Commands>,
// }
// struct Data {
//     pub bytes: BytesMut,
// }
//
// impl IncomingServer {
//     async fn open(self: sync::Arc<Self>, name: String) -> Result<Channel> {
//         let (tx, rx) = tokio::sync::oneshot::channel();
//         self.command.send(Commands::Open(name, tx)).await?;
//         rx.await.map_err(|_| "could not establish channel".into())
//     }
//
//     async fn close(self: sync::Arc<Self>, channel: Channel) -> Result<()> {
//         self.command.send(Commands::Close(channel)).await?;
//         Ok(())
//     }
//
//     async fn handle_connection(
//         mut tcp_stream: TcpStream,
//         channels: &RwLock<HashMap<String, ChannelHandle>>,
//     ) -> Result<()> {
//         loop {
//             let mut buf = BytesMut::with_capacity(8192);
//             let len = tcp_stream.read_buf(&mut buf).await?;
//             if len == 0 {
//                 return Ok(());
//             }
//             assert_eq!(buf[len - 1], b'\n');
//             let name = from_utf8(&buf[0..len - 1]).unwrap();
//             let targetChannel = if let Some(sender) = channels.read().await.get(name) {
//                 tcp_stream.write_all("OK\n".as_bytes()).await?;
//                 sender.clone()
//             } else {
//                 tcp_stream.write_all("NOT OK\n".as_bytes()).await?;
//                 continue;
//             };
//
//             let mut buf = BytesMut::with_capacity(targetChannel.max_buffer_size);
//             let len = tcp_stream.read_buf(&mut buf).await?;
//             buf.resize(len, 0);
//             targetChannel.tx.send(Data { bytes: buf }).await?;
//         }
//     }
//
//     async fn run(mut rx: tokio::sync::mpsc::Receiver<Commands>, port: u16) -> Result<()> {
//         info!("Starting at data port: {}", port);
//         let server = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
//         let channels = sync::Arc::new(tokio::sync::RwLock::new(HashMap::default()));
//
//         loop {
//             tokio::select! {
//                 Ok((socket, addr)) = server.accept() => {
//                     info!("Connection from {}", addr);
//                     tokio::spawn({
//                         let channels = channels.clone();
//                         async move {
//                             match Self::handle_connection(socket, &channels).await {
//                                 Ok(_) => {info!("Connection is terminated")},
//                                 Err(e) => {error!("Connection is terminated: {e}")}
//                             }
//                         }
//                         });
//                 },
//                 Some(command) = rx.recv() => {
//                     match command {
//                     Commands::Open(name, response) => {
//                             let (tx, rx) = tokio::sync::mpsc::channel(10);
//                             channels.write().await.insert(name, tx);
//                             response.send(Channel{rx}).expect("Could not send");
//                         },
//                         Commands::Close(_) => {}}
//                 }
//             }
//         }
//         Ok(())
//     }
// }
// }
#[derive(Parser)]
struct CLIArgs {
    data_port: u16,
}

struct NetworkSink {}

impl ExecutablePipeline for NetworkSink {
    fn execute(&self, data: engine::Data, context: &mut dyn PipelineContext) {}
}

struct NetworkSource {}

impl engine::SourceImpl for NetworkSource {
    fn start(&self, emit: crate::engine::EmitFn) {}

    fn stop(&self) {}
}

fn basic_emit(d: Data) -> bool {
    println!("Received data: {:?}", from_utf8(d.bytes.as_ref()));
    return true;
}

fn main() {
    tracing_subscriber::fmt().init();
    let args = CLIArgs::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()
        .unwrap();
    let service = crate::network::NetworkService::start(rt);
    service
        .register_channel(
            "hello".to_string(),
            Box::new(|data| basic_emit(data)) as EmitFn,
        )
        .expect("Could not register channel");

    sleep(Duration::from_secs(1));
    service.shutdown().unwrap();

    // // let (tx, rx) = tokio::sync::mpsc::channel(20);
    // // let server = Arc::new(IncomingServer { command: tx });
    //
    // let rt = tokio::runtime::Builder::new_multi_thread()
    //     .enable_io()
    //     .build()
    //     .expect("Could not create rt");
    //
    // let port = args.data_port;
    // // rt.spawn(async move {
    // //     IncomingServer::run(rx, port).await.expect("Server Failed");
    // // });
    // //
    // // rt.block_on(async {
    // //     let mut channel = server.open("test".into()).await.expect("Yes");
    // //     loop {
    // //         let data = channel.rx.recv().await.expect("Could not get data");
    // //         info!(
    // //             "Received data from channel {}. Msg: {:?}",
    // //             "test", data.bytes
    // //         );
    // //     }
    // // });
    //
    // let engine = engine::QueryEngine::start();
    // let sink = Node::new(None, Arc::new(NetworkSink {}));
    //
    // let id = engine.startQuery(engine::Query::new(vec![SourceNode::new(
    //     sink,
    //     Box::new(NetworkSource {}),
    // )]));
    // sleep(Duration::from_secs(5));
    // engine.stopQuery(id);
}
