mod config;
mod engine;

use crate::config::Command;
use crate::engine::{
    EmitFn, ExecutablePipeline, Node, PipelineContext, Query, QueryEngine, SourceImpl, SourceNode,
};
use async_channel::{RecvError, TryRecvError, TrySendError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use clap::{Parser, Subcommand};
use distributed::protocol::{ChannelIdentifier, ConnectionIdentifier, TupleBuffer};
use distributed::{receiver, sender};
use log::error;
use std::collections::VecDeque;
use std::io::prelude::*;
use std::io::Cursor;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use std::{sync, thread};
use tokio_util::sync::CancellationToken;
use tracing::info;

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

struct PrintSink;

fn verify_tuple_buffer(received: &TupleBuffer) -> bool {
    let counter = (received.sequence_number - 1) as usize;

    // Reconstruct main buffer
    let mut expected_buffer = BytesMut::with_capacity(16);
    let value_bytes = counter.to_le_bytes();

    while expected_buffer.len() + size_of::<usize>() <= expected_buffer.capacity() {
        expected_buffer.put(&value_bytes[..]);
    }

    // Verify main buffer
    if received.data != expected_buffer.freeze() {
        return false;
    }

    // Reconstruct child buffers
    let mut expected_child_buffers = vec![];
    for idx in 0..(counter % 3) {
        let buffer = vec![0u8; ((idx + 1) * (10 * counter)) % 16];
        let mut cursor = Cursor::new(buffer);
        while cursor.has_remaining() {
            cursor.write_all(&counter.to_le_bytes()).unwrap();
        }
        expected_child_buffers.push(cursor.into_inner());
    }

    // Verify child buffers
    if received.child_buffers.len() != expected_child_buffers.len() {
        return false;
    }

    for (received_child, expected_child) in received
        .child_buffers
        .iter()
        .zip(expected_child_buffers.iter())
    {
        if received_child != expected_child {
            return false;
        }
    }

    true
}

impl ExecutablePipeline for PrintSink {
    fn execute(&self, data: &TupleBuffer, _context: &mut dyn PipelineContext) {
        info!(
            "Buffer {} is {}",
            data.sequence_number,
            if verify_tuple_buffer(data) {
                "OK"
            } else {
                "NOT OK"
            }
        );
    }

    fn stop(&self) {
        //NOP
    }
}

struct NetworkSink {
    service: Arc<sender::NetworkService>,
    connection: ConnectionIdentifier,
    channel: ChannelIdentifier,
    queue: sync::RwLock<Option<(CancellationToken, sender::ChannelControlQueue)>>,
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
    fn execute(&self, data: &TupleBuffer, _context: &mut dyn PipelineContext) {
        if self.queue.read().unwrap().is_none() {
            let mut write_locked = self.queue.write().unwrap();
            if write_locked.is_none() {
                info!("Network Sink Setup");
                write_locked.replace(
                    self.service
                        .register_channel(self.connection.clone(), self.channel.clone())
                        .unwrap(),
                );
                info!("Network Sink Setup Done");
            }
        }

        if !self.buffer.read().unwrap().is_empty() {
            let mut locked = self.buffer.write().unwrap();
            if !locked.is_empty() {
                locked.push_back(data.clone());
                while !locked.is_empty() {
                    let front = locked.pop_front().unwrap();
                    match self
                        .queue
                        .read()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .1
                        .try_send(sender::ChannelControlMessage::Data(front))
                    {
                        Err(TrySendError::Full(sender::ChannelControlMessage::Data(data))) => {
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
            .try_send(sender::ChannelControlMessage::Data(data.clone()))
        {
            Err(TrySendError::Full(sender::ChannelControlMessage::Data(data))) => {
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
    ingestion_rate: Option<Duration>,
    token: std::sync::Mutex<Option<CancellationToken>>,
    thread: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl NetworkSource {
    pub fn new(
        channel: ChannelIdentifier,
        ingestion_rate: Option<Duration>,
        service: Arc<receiver::NetworkService>,
    ) -> Self {
        Self {
            channel,
            service,
            ingestion_rate,
            token: sync::Mutex::default(),
            thread: sync::Mutex::default(),
        }
    }
}

impl engine::SourceImpl for NetworkSource {
    fn start(&self, emit: engine::EmitFn) {
        let ingestion_rate = self.ingestion_rate.unwrap_or(Duration::from_millis(0));
        let (queue, token) = self.service.register_channel(self.channel.clone()).unwrap();
        self.token.lock().unwrap().replace(token);

        self.thread
            .lock()
            .unwrap()
            .replace(thread::spawn(move || loop {
                match queue.recv_blocking() {
                    Ok(d) => {
                        emit(d);
                    }
                    Err(e) => {
                        error!("Source stopped {e}");
                        return;
                    }
                };
                sleep(ingestion_rate);
            }));
    }

    fn stop(&self) {
        info!("Cancelling Source");
        self.token.lock().unwrap().take().unwrap().cancel();
    }
}

struct Thread<T> {
    stopped: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<T>>,
}

impl<T> Drop for Thread<T> {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
        if !self
            .handle
            .as_ref()
            .expect("BUG: Dropped multiple times")
            .is_finished()
        {
            self.handle
                .take()
                .expect("BUG: Dropped multiple times")
                .join()
                .unwrap();
        }
    }
}

impl<T> Thread<T> {
    pub fn spawn<F>(function: F) -> Self
    where
        F: 'static + Send + FnOnce(&AtomicBool) -> T,
        T: 'static + Send,
    {
        let stopped: Arc<AtomicBool> = Arc::default();
        let handle = thread::spawn({
            let token = stopped.clone();
            move || function(token.as_ref())
        });

        Thread {
            stopped,
            handle: Some(handle),
        }
    }
}

struct GeneratorSource {
    thread: sync::RwLock<Option<Thread<()>>>,
    interval: Duration,
}

impl SourceImpl for GeneratorSource {
    fn start(&self, emit: EmitFn) {
        self.thread.write().unwrap().replace(Thread::spawn({
            let interval = self.interval;
            move |stopped| {
                let mut counter = 0_usize;
                while !stopped.load(Acquire) {
                    let buffer = vec![0u8; 16];
                    let mut cursor = Cursor::new(buffer);
                    while cursor.has_remaining() {
                        cursor.write_all(&counter.to_le_bytes()).unwrap();
                    }

                    let mut child_buffers = vec![];
                    for idx in 0..(counter % 3) {
                        let buffer = vec![0u8; ((idx + 1) * (10 * counter)) % 16];
                        let mut cursor = Cursor::new(buffer);
                        while cursor.has_remaining() {
                            cursor.write_all(&counter.to_le_bytes()).unwrap();
                        }
                        child_buffers.push(cursor.into_inner());
                    }

                    counter += 1;
                    emit(TupleBuffer {
                        sequence_number: counter as u64,
                        origin_id: 1,
                        chunk_number: 1,
                        number_of_tuples: 1,
                        last_chunk: true,
                        data: cursor.into_inner(),
                        child_buffers,
                    });
                    sleep(interval);
                }
                info!("Generator stopped");
            }
        }));
    }

    fn stop(&self) {
        let _ = self.thread.write().unwrap().take();
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
    ingestion_rate_in_milliseconds: Option<u64>,
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
        Box::new(GeneratorSource::new(Duration::from_millis(
            ingestion_rate_in_milliseconds.unwrap_or(250),
        ))),
    )]));

    query
}

fn sink(
    channel: ChannelIdentifier,
    ingestion_rate_in_milliseconds: Option<u64>,
    engine: Arc<QueryEngine>,
    receiver: Arc<receiver::NetworkService>,
) -> usize {
    engine.start_query(Query::new(vec![SourceNode::new(
        Node::new(None, Arc::new(PrintSink {})),
        Box::new(NetworkSource::new(
            channel,
            ingestion_rate_in_milliseconds.map(|millis| Duration::from_millis(millis)),
            receiver.clone(),
        )),
    )]))
}

fn bridge(
    input_channel: ChannelIdentifier,
    downstream_channel: ChannelIdentifier,
    downstream_connection: ConnectionIdentifier,
    ingestion_rate_in_milliseconds: Option<u64>,
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
        Box::new(NetworkSource::new(
            input_channel,
            ingestion_rate_in_milliseconds.map(|millis| Duration::from_millis(millis)),
            receiver.clone(),
        )),
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

    for command in config.commands.into_iter() {
        match command {
            Command::StartQuery { q } => {
                match q {
                    config::Query::Source {
                        downstream_channel,
                        downstream_connection,
                        ingestion_rate_in_milliseconds,
                    } => generator(
                        downstream_connection,
                        downstream_channel,
                        ingestion_rate_in_milliseconds,
                        sender.clone(),
                        engine.clone(),
                    ),
                    config::Query::Bridge {
                        input_channel,
                        downstream_channel,
                        downstream_connection,
                        ingestion_rate_in_milliseconds,
                    } => bridge(
                        input_channel,
                        downstream_channel,
                        downstream_connection,
                        ingestion_rate_in_milliseconds,
                        engine.clone(),
                        receiver.clone(),
                        sender.clone(),
                    ),
                    config::Query::Sink {
                        input_channel,
                        ingestion_rate_in_milliseconds,
                    } => sink(
                        input_channel,
                        ingestion_rate_in_milliseconds,
                        engine.clone(),
                        receiver.clone(),
                    ),
                };
            }
            Command::StopQuery { id } => {
                engine.stop_query(id);
            }
            Command::Wait { millis } => {
                sleep(Duration::from_millis(millis as u64));
            }
        };
    }
}
