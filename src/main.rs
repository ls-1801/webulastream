mod config;
mod engine;

use crate::config::Command;
use crate::engine::{
    Data, EmitFn, ExecutablePipeline, Node, PipelineContext, Query, QueryEngine, SourceImpl,
    SourceNode,
};
use async_channel::TrySendError;
use bytes::{Bytes, BytesMut};
use clap::{Parser, Subcommand};
use distributed::protocol::{ChannelIdentifier, ConnectionIdentifier, TupleBuffer};
use distributed::{receiver, sender};
use log::warn;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::VecDeque;
use std::fmt::{Debug, Write};
use std::str::from_utf8;
use std::sync;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::{sleep, spawn, Thread};
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

// mod inter_node {
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error>;
struct ChannelParameter {
    port: u16,
}

#[derive(Parser)]
struct CLIArgs {
    file: String,
    index: usize,
}

#[derive(Subcommand)]
enum Commands {}

struct PrintSink {}

impl ExecutablePipeline for PrintSink {
    fn execute(&self, data: Data, _context: &mut dyn PipelineContext) {
        println!("{:?}", from_utf8(&data.bytes));
    }

    fn stop(&self) {
        //NOP
    }
}

struct NetworkSink {
    service: Arc<sender::NetworkService>,
    connection: ConnectionIdentifier,
    channel: ChannelIdentifier,
    queue: sync::RwLock<Option<(CancellationToken, sender::DataQueue)>>,
    buffer: std::sync::RwLock<VecDeque<TupleBuffer>>,
}

impl NetworkSink {
    pub fn new(
        service: Arc<sender::NetworkService>,
        connection: ConnectionIdentifier,
        channel: ChannelIdentifier,
    ) -> Self {
        Self {
            service,
            connection,
            channel,
            queue: std::sync::RwLock::new(None),
            buffer: std::sync::RwLock::new(VecDeque::new()),
        }
    }
}

impl ExecutablePipeline for NetworkSink {
    fn execute(&self, data: Data, _context: &mut dyn PipelineContext) {
        let data = TupleBuffer {
            sequence_number: 1,
            data: data.bytes.freeze(),
        };
        if self.queue.read().unwrap().is_none() {
            let mut write_locked = self.queue.write().unwrap();
            if write_locked.is_none() {
                info!("Network Sink Setup");
                write_locked.replace(
                    self.service
                        .register_channel(self.connection, self.channel.clone())
                        .unwrap(),
                );
                info!("Network Sink Setup Done");
            }
        }

        if !self.buffer.read().unwrap().is_empty() {
            let mut locked = self.buffer.write().unwrap();
            if !locked.is_empty() {
                locked.push_back(data);
                loop {
                    let front = locked.pop_front().unwrap();
                    match self
                        .queue
                        .read()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .1
                        .try_send(front)
                    {
                        Err(TrySendError::Full(data)) => {
                            locked.push_front(data);
                            return;
                        }
                        Err(TrySendError::Closed(_)) => {
                            panic!("Channel should not be closed");
                        }
                        _ => {}
                    }
                }
            }
        }

        match self
            .queue
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .1
            .try_send(data)
        {
            Err(TrySendError::Full(data)) => {
                self.buffer.write().unwrap().push_back(data);
            }
            Err(TrySendError::Closed(_)) => {
                panic!("Channel should not be closed");
            }
            _ => {}
        }
    }

    fn stop(&self) {
        info!("Cancelling Sink");
        self.queue.write().unwrap().take().unwrap().0.cancel();
    }
}

struct NetworkSource {
    channel: ChannelIdentifier,
    service: Arc<receiver::NetworkService>,
    token: std::sync::Mutex<Option<CancellationToken>>,
}

impl NetworkSource {
    pub fn new(channel: ChannelIdentifier, service: Arc<receiver::NetworkService>) -> Self {
        Self {
            channel,
            service,
            token: sync::Mutex::default(),
        }
    }
}

impl engine::SourceImpl for NetworkSource {
    fn start(&self, emit: engine::EmitFn) {
        self.token.lock().unwrap().replace(
            self.service
                .register_channel(
                    self.channel.clone(),
                    Box::new(move |data| {
                        emit(data.data.try_into_mut().unwrap());
                        true
                    }),
                )
                .unwrap(),
        );
    }

    fn stop(&self) {
        info!("Cancelling Source");
        self.token.lock().unwrap().take().unwrap().cancel();
    }
}

struct GeneratorSource {
    thread: sync::RwLock<Option<std::thread::JoinHandle<()>>>,
    interval: Duration,
    stopped: Arc<AtomicBool>,
}

impl SourceImpl for GeneratorSource {
    fn start(&self, emit: EmitFn) {
        self.thread.write().unwrap().replace(std::thread::spawn({
            let interval = self.interval;
            let stopped = self.stopped.clone();
            move || {
                let mut counter = 0_usize;
                while !stopped.load(std::sync::atomic::Ordering::Acquire) {
                    let mut buffer = BytesMut::with_capacity(1024);
                    buffer
                        .write_fmt(format_args!(
                            "This is a generated buffer with id{counter}\n"
                        ))
                        .unwrap();
                    counter += 1;
                    emit(buffer);
                    sleep(interval);
                }
                info!("Generator stopped");
            }
        }));
    }

    fn stop(&self) {
        self.stopped
            .as_ref()
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

impl GeneratorSource {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            thread: sync::RwLock::new(None),
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }
}

fn generator(
    downstream_connection: ConnectionIdentifier,
    downstream_channel: ChannelIdentifier,
    sender: Arc<sender::NetworkService>,
    engine: Arc<QueryEngine>,
) -> usize {
    let query = engine.start_query(Query::new(vec![SourceNode::new(
        Node::new(
            None,
            Arc::new(NetworkSink::new(
                sender.clone(),
                downstream_connection,
                downstream_channel,
            )),
        ),
        Box::new(GeneratorSource::new(Duration::from_millis(50))),
    )]));

    query
}

fn sink(
    channel: ChannelIdentifier,
    engine: Arc<QueryEngine>,
    receiver: Arc<receiver::NetworkService>,
) -> usize {
    engine.start_query(Query::new(vec![SourceNode::new(
        Node::new(None, Arc::new(PrintSink {})),
        Box::new(NetworkSource::new(channel, receiver.clone())),
    )]))
}

fn bridge(
    input_channel: ChannelIdentifier,
    downstream_channel: ChannelIdentifier,
    downstream_connection: ConnectionIdentifier,
    engine: Arc<QueryEngine>,
    receiver: Arc<receiver::NetworkService>,
    sender: Arc<sender::NetworkService>,
) -> usize {
    let query = engine.start_query(Query::new(vec![SourceNode::new(
        Node::new(
            None,
            Arc::new(NetworkSink::new(
                sender.clone(),
                downstream_connection,
                downstream_channel,
            )),
        ),
        Box::new(NetworkSource::new(input_channel, receiver.clone())),
    )]));
    query
}

fn main() {
    tracing_subscriber::fmt().init();
    let args = CLIArgs::parse();

    let config = config::load_config(std::path::Path::new(&args.file), args.index);

    let engine = engine::QueryEngine::start();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("sender")
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    let sender = sender::NetworkService::start(rt);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("receiver")
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    let receiver = receiver::NetworkService::start(rt, config.connection);

    for command in config.commands.iter() {
        match command {
            Command::StartQuery { q } => {
                match q {
                    config::Query::Source {
                        downstream_channel,
                        downstream_connection,
                    } => generator(
                        *downstream_connection,
                        downstream_channel.clone(),
                        sender.clone(),
                        engine.clone(),
                    ),
                    config::Query::Bridge {
                        input_channel,
                        downstream_channel,
                        downstream_connection,
                    } => bridge(
                        input_channel.clone(),
                        downstream_channel.clone(),
                        *downstream_connection,
                        engine.clone(),
                        receiver.clone(),
                        sender.clone(),
                    ),
                    config::Query::Sink { input_channel } => {
                        sink(input_channel.clone(), engine.clone(), receiver.clone())
                    }
                };
            }
            Command::StopQuery { id } => {
                engine.stop_query(*id);
            }
            Command::Wait { millis } => {
                sleep(Duration::from_millis(*millis as u64));
            }
        };
    }
}
