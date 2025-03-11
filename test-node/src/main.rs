mod config;
mod engine;

use crate::config::Command;
use crate::engine::{
    EmitFn, ExecutablePipeline, Node, PipelineContext, Query, QueryEngine, SourceImpl, SourceNode,
};
use async_channel::TrySendError;
use bytes::{Buf, BufMut, BytesMut};
use clap::{Parser, Subcommand};
use log::error;
use nes_network::protocol::{ChannelIdentifier, ConnectionIdentifier, TupleBuffer};
use nes_network::sender::ChannelControlMessage;
use nes_network::{receiver, sender};
use std::collections::{HashSet, VecDeque};
use std::io::Cursor;
use std::io::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{sync, thread};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

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

struct PrintSink {
    sequence_tracker: Mutex<MissingSequenceTracker>,
}

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

/// A data structure that tracks seen positive integers and finds the lowest unseen number.
#[derive(Default)]
pub struct MissingSequenceTracker {
    seen: HashSet<u64>,
    highest_seen: u64,
    lowest_removed: u64,
}

impl MissingSequenceTracker {
    /// Creates a new empty tracker.
    pub fn new() -> Self {
        Self {
            seen: HashSet::new(),
            highest_seen: 0,
            lowest_removed: 0,
        }
    }

    /// Adds a positive number to the tracker.
    /// Panics if the number is zero or already seen.
    pub fn add(&mut self, num: u64) {
        // Assert that the number is valid (positive)
        assert!(num > 0, "Only positive numbers are allowed");
        assert!(
            !self.seen.contains(&num),
            "Number {} has already been seen",
            num
        );

        if num > self.highest_seen {
            self.highest_seen = num;
        }

        self.seen.insert(num);

        if self.seen.len() > 1000 {
            let lowest = self.query();
            self.seen.retain(|&x| x > lowest);
            self.lowest_removed = lowest - 1;
        }
    }

    /// Returns the lowest positive integer that hasn't been seen yet.
    pub fn query(&self) -> u64 {
        if self.seen.len() as u64 == self.highest_seen - self.lowest_removed {
            return self.highest_seen + 1;
        }

        for i in 1..=self.highest_seen {
            if !self.seen.contains(&(i + self.lowest_removed)) {
                return i + self.lowest_removed;
            }
        }

        unreachable!()
    }
}

#[test]
fn testSequenceTrackerMissingOne() {
    let mut tracker = MissingSequenceTracker::default();
    for i in 1..1004 {
        if i == 100 || i == 193 || i == 210 {
            continue;
        }
        tracker.add(i);
    }

    assert_eq!(tracker.query(), 100);
    tracker.add(100);
    tracker.add(1004);
    assert_eq!(tracker.query(), 193);
    tracker.add(193);
    assert_eq!(tracker.query(), 210);
    tracker.add(1005);
}
#[test]
fn testSequenceTrackerMissing() {
    let tracker = MissingSequenceTracker::default();

    assert_eq!(tracker.query(), 1);
}
#[test]
fn testSequenceTrackerOOMissing() {
    let mut tracker = MissingSequenceTracker::default();
    tracker.add(1);
    tracker.add(4);
    tracker.add(2);

    assert_eq!(tracker.query(), 3);
}
#[test]
fn testSequenceTrackerOO() {
    let mut tracker = MissingSequenceTracker::default();
    tracker.add(1);
    tracker.add(3);
    tracker.add(2);

    assert_eq!(tracker.query(), 4);
}
#[test]
fn testSequenceTracker() {
    let mut tracker = MissingSequenceTracker::default();
    tracker.add(1);
    tracker.add(2);
    tracker.add(3);

    assert_eq!(tracker.query(), 4);
}

impl ExecutablePipeline for PrintSink {
    fn execute(&self, data: &TupleBuffer, _context: &mut dyn PipelineContext) {
        if !verify_tuple_buffer(data) {
            error!("Invalid data received");
        }
        self.sequence_tracker
            .lock()
            .unwrap()
            .add(data.sequence_number);
    }

    fn stop(&self) {
        info!(
            "Sink stopped. Last Sequence: {}",
            self.sequence_tracker.lock().unwrap().query() - 1
        );
    }
}

struct NetworkSink {
    service: Arc<sender::NetworkService>,
    connection: ConnectionIdentifier,
    channel: ChannelIdentifier,
    queue: sync::RwLock<Option<sender::ChannelControlQueue>>,
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
        info!("Closing Network Sink");
        let (tx, rx) = tokio::sync::oneshot::channel();
        let queue = self.queue.write().unwrap().take().unwrap();
        if queue
            .send_blocking(ChannelControlMessage::Flush(tx))
            .is_err()
        {
            warn!("Network sink was already closed");
            return;
        }

        if rx.blocking_recv().is_err() {
            warn!("Network sink was already closed");
            return;
        }

        if queue
            .send_blocking(ChannelControlMessage::Terminate)
            .is_err()
        {
            warn!("Network sink was already closed");
            return;
        }
        queue.close();
    }
}

struct NetworkSource {
    channel: ChannelIdentifier,
    service: Arc<receiver::NetworkService>,
    ingestion_rate: Option<Duration>,
    thread: std::sync::Mutex<Option<Thread<()>>>,
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
            thread: sync::Mutex::default(),
        }
    }
}

impl engine::SourceImpl for NetworkSource {
    fn start(&self, emit: engine::EmitFn) {
        let ingestion_rate = self.ingestion_rate.unwrap_or(Duration::from_millis(0));
        let queue = self.service.register_channel(self.channel.clone()).unwrap();

        self.thread
            .lock()
            .unwrap()
            .replace(Thread::spawn(move |stopped: &AtomicBool| {
                while !stopped.load(Ordering::Relaxed) {
                    match queue.recv_blocking() {
                        Ok(d) => {
                            emit(d);
                        }
                        Err(_) => {
                            info!("External source stop");
                            return;
                        }
                    };
                    thread::sleep(ingestion_rate);
                }
                info!("Internal source stop");
                queue.close();
            }));
    }

    fn stop(&self) {
        info!("Cancelling Source");
        let _ = self.thread.lock().unwrap().take();
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
                while !stopped.load(Ordering::Relaxed) {
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
                        watermark: 2,
                        chunk_number: 1,
                        number_of_tuples: 1,
                        last_chunk: true,
                        data: cursor.into_inner(),
                        child_buffers,
                    });
                    thread::sleep(interval);
                }
                info!("Source stopped. Last Sequence: {counter}");
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
        Node::new(
            None,
            Arc::new(PrintSink {
                sequence_tracker: Default::default(),
            }),
        ),
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
                thread::sleep(Duration::from_millis(millis as u64));
            }
        };
    }
}
