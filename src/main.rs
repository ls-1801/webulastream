mod config;
mod engine;
mod network;

use crate::engine::{
    Data, EmitFn, ExecutablePipeline, Node, PipelineContext, Query, QueryEngine, SourceImpl,
    SourceNode,
};
use crate::network::protocol::{ChannelIdentifier, ConnectionIdentifier};
use crate::network::{receiver, sender};
use async_channel::TrySendError;
use bytes::{Bytes, BytesMut};
use clap::{Parser, Subcommand};
use log::warn;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::VecDeque;
use std::fmt::{Debug, Write};
use std::str::from_utf8;
use std::sync;
use std::sync::Arc;
use std::thread::{sleep, spawn, Thread};
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{mpsc, RwLock};
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
}

struct NetworkSink {
    service: Arc<sender::NetworkService>,
    connection: network::protocol::ConnectionIdentifier,
    channel: network::protocol::ChannelIdentifier,
    queue: std::sync::RwLock<Option<sender::DataQueue>>,
    buffer: std::sync::RwLock<VecDeque<Data>>,
}

impl NetworkSink {
    pub fn new(
        service: Arc<sender::NetworkService>,
        connection: network::protocol::ConnectionIdentifier,
        channel: network::protocol::ChannelIdentifier,
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
        if self.queue.read().unwrap().is_none() {
            let mut write_locked = self.queue.write().unwrap();
            if write_locked.is_none() {
                write_locked.replace(
                    self.service
                        .register_channel(self.connection, self.channel.clone())
                        .unwrap(),
                );
            }
        }

        if !self.buffer.read().unwrap().is_empty() {
            let mut locked = self.buffer.write().unwrap();
            if !locked.is_empty() {
                locked.push_back(data);
                loop {
                    let front = locked.pop_front().unwrap();
                    match self.queue.read().unwrap().as_ref().unwrap().try_send(front) {
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

        match self.queue.read().unwrap().as_ref().unwrap().try_send(data) {
            Err(TrySendError::Full(data)) => {
                self.buffer.write().unwrap().push_back(data);
            }
            Err(TrySendError::Closed(_)) => {
                panic!("Channel should not be closed");
            }
            _ => {}
        }
    }
}

struct NetworkSource {
    channel: network::protocol::ChannelIdentifier,
    service: Arc<receiver::NetworkService>,
}

impl NetworkSource {
    pub fn new(
        channel: network::protocol::ChannelIdentifier,
        service: Arc<receiver::NetworkService>,
    ) -> Self {
        Self { channel, service }
    }
}

impl engine::SourceImpl for NetworkSource {
    fn start(&self, emit: engine::EmitFn) {
        self.service
            .register_channel(
                self.channel.clone(),
                Box::new(move |data| {
                    emit(data.bytes);
                    true
                }),
            )
            .unwrap()
    }

    fn stop(&self) {}
}

struct GeneratorSource {
    thread: sync::RwLock<Option<std::thread::JoinHandle<()>>>,
    interval: Duration,
}

impl SourceImpl for GeneratorSource {
    fn start(&self, emit: EmitFn) {
        self.thread.write().unwrap().replace(std::thread::spawn({
            let interval = self.interval;
            move || {
                let mut counter = 0_usize;
                loop {
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
            }
        }));
    }

    fn stop(&self) {
        todo!()
    }
}

impl GeneratorSource {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            thread: sync::RwLock::new(None),
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
        Box::new(GeneratorSource::new(Duration::from_secs(1))),
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

    for query in config.queries.iter() {
        match query {
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
    
    sleep(Duration::from_secs(1000));
}
