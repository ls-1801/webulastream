use crate::protocol::*;
use futures::SinkExt;
use tokio::select;
use tokio::task::{AbortHandle, JoinHandle, JoinSet};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::net::TcpSocket;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, info_span, warn, Instrument};

pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub struct NetworkService {
    runtime: Mutex<Option<Runtime>>,
    cancellation_token: CancellationToken,
    connections: Mutex<HashMap<SocketAddr, (CancellationToken, NetworkingConnectionController, async_channel::Sender<ChannelControlMessage>)>>,
    join_handles: Mutex<Vec<JoinHandle<()>>>,
}

#[derive(Debug)]
pub enum NetworkingConnectionControlMessage {
    RegisterChannel(ChannelIdentifier, async_channel::Receiver<ChannelControlMessage>),
    RetryChannel(
        ChannelIdentifier,
        CancellationToken,
        channel_handler::ChannelControlQueueListener,
    ),
}
pub type ChannelControlMessage = channel_handler::ChannelControlMessage;
pub type ChannelControlQueue = channel_handler::ChannelControlQueue;
type NetworkingConnectionController = async_channel::Sender<NetworkingConnectionControlMessage>;
type NetworkingConnectionControlListener =
    async_channel::Receiver<NetworkingConnectionControlMessage>;

#[derive(Debug)]
enum ChannelHandlerResult {
    Cancelled,
    ConnectionLost(Box<dyn std::error::Error + Send + Sync>),
    Closed,
}

mod channel_handler {
    use crate::protocol;
    use crate::protocol::{
        DataChannelRequest, DataChannelResponse, DataChannelSenderReader, DataChannelSenderWriter,
        TupleBuffer,
    };
    use futures::SinkExt;
    use std::collections::{HashMap, VecDeque};
    use tokio::net::TcpStream;
    use tokio::select;
    use tokio::sync::oneshot;
    use tokio_stream::StreamExt;
    use tracing::{info, trace, warn};

    const MAX_PENDING_ACKS: usize = 64;

    pub enum ChannelControlMessage {
        Data(TupleBuffer),
        Flush(oneshot::Sender<()>),
        Terminate,
    }
    pub type ChannelControlQueue = async_channel::Sender<ChannelControlMessage>;
    pub(super) type ChannelControlQueueListener = async_channel::Receiver<ChannelControlMessage>;
    pub(super) enum ChannelHandlerError {
        ClosedByOtherSide,
        ConnectionLost(Box<dyn std::error::Error + Send + Sync>),
        Protocol(Box<dyn std::error::Error + Send + Sync>),
        Cancelled,
        Terminated,
    }
    pub(super) struct ChannelHandler {
        pending_writes: VecDeque<TupleBuffer>,
        wait_for_ack: HashMap<u64, TupleBuffer>,
        writer: DataChannelSenderWriter,
        reader: DataChannelSenderReader,
        queue: ChannelControlQueueListener,
    }

    type Result<T> = core::result::Result<T, ChannelHandlerError>;

    impl ChannelHandler {
        pub fn new(
            stream: TcpStream,
            queue: ChannelControlQueueListener,
        ) -> Self {
            let (reader, writer) = protocol::data_channel_sender(stream);

            Self {
                pending_writes: Default::default(),
                wait_for_ack: Default::default(),
                reader,
                writer,
                queue,
            }
        }

        async fn handle_request(
            &mut self,
            channel_control_message: ChannelControlMessage,
        ) -> Result<()> {
            match channel_control_message {
                ChannelControlMessage::Data(data) => self.pending_writes.push_back(data),
                ChannelControlMessage::Flush(done) => {
                    self.flush().await?;
                    let _ = done.send(());
                }
                ChannelControlMessage::Terminate => {
                    let _ = self.writer.send(DataChannelRequest::Close).await;
                    assert!(self.queue.is_empty(), "send queue shall be empty after receiving Terminate!");
                    self.queue.close();
                    return Err(ChannelHandlerError::Terminated);
                }
            }
            Ok(())
        }
        fn handle_response(&mut self, response: DataChannelResponse) -> Result<()> {
            match response {
                DataChannelResponse::Close => {
                    info!("Channel Closed by other receiver");
                    return Err(ChannelHandlerError::ClosedByOtherSide);
                }
                DataChannelResponse::NAckData(seq) => {
                    if let Some(write) = self.wait_for_ack.remove(&seq) {
                        warn!("NAck for {seq}");
                        self.pending_writes.push_back(write);
                    } else {
                        panic!("Protocol Error: expected Seq {seq}");
                    }
                }
                DataChannelResponse::AckData(seq) => {
                    let Some(_) = self.wait_for_ack.remove(&seq) else {
                        panic!("Protocol Error: expected Seq {seq}");
                    };
                    trace!("Ack for {seq}");
                }
            }

            Ok(())
        }

        async fn send_pending(
            writer: &mut DataChannelSenderWriter,
            pending_writes: &mut VecDeque<TupleBuffer>,
            wait_for_ack: &mut HashMap<u64, TupleBuffer>,
        ) -> Result<()> {
            if pending_writes.is_empty() {
                return Ok(());
            }

            let next_buffer = pending_writes.front().expect("BUG: check value earlier");

            if writer
                .send(DataChannelRequest::Data(next_buffer.clone()))
                .await
                .is_ok()
            {
                wait_for_ack.insert(
                    next_buffer.sequence_number,
                    pending_writes
                        .pop_front()
                        .expect("BUG: checked value earlier"),
                );
            }
            Ok(())
        }

        async fn flush(&mut self) -> Result<()> {
            while !self.pending_writes.is_empty() || !self.wait_for_ack.is_empty() {
                self.writer
                    .flush()
                    .await
                    .map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?;
                if self.pending_writes.is_empty() || self.wait_for_ack.len() >= MAX_PENDING_ACKS {
                    select! {
                        response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                    }
                } else {
                    select! {
                        response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                        send_result = Self::send_pending(&mut self.writer, &mut self.pending_writes, &mut self.wait_for_ack) => send_result?,
                    }
                }
            }

            Ok(())
        }

        pub(super) async fn run(&mut self) -> Result<()> {
            loop {
                if self.pending_writes.is_empty() || self.wait_for_ack.len() >= MAX_PENDING_ACKS {
                    select! {
                        response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                        request = self.queue.recv() => self.handle_request(request.map_err(|_| ChannelHandlerError::Cancelled)?).await?,
                    }
                } else {
                    select! {
                        response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                        request = self.queue.recv() => self.handle_request(request.map_err(|_| ChannelHandlerError::Cancelled)?).await?,
                        send_result = Self::send_pending(&mut self.writer, &mut self.pending_writes, &mut self.wait_for_ack) => send_result?,
                    }
                }
            }
        }
    }
}


async fn channel_handler(
    channel_address: SocketAddr,
    queue: channel_handler::ChannelControlQueueListener,
) -> ChannelHandlerResult {
    let socket = match TcpSocket::new_v4() {
        Ok(socket) => socket,
        Err(e) => {
            return ChannelHandlerResult::ConnectionLost(
                format!("Could not create socket {e:?}").into(),
            );
        }
    };

    let connection = match socket.connect(channel_address).await {
        Err(e) => return ChannelHandlerResult::ConnectionLost(e.into()),
        Ok(connection) => connection,
    };

    let mut handler = channel_handler::ChannelHandler::new(connection, queue);
    match handler.run().await {
        Ok(_) => ChannelHandlerResult::Closed,
        Err(channel_handler::ChannelHandlerError::Terminated) => ChannelHandlerResult::Closed,
        Err(channel_handler::ChannelHandlerError::ClosedByOtherSide) => {
            ChannelHandlerResult::Closed
        }
        Err(channel_handler::ChannelHandlerError::Cancelled) => ChannelHandlerResult::Cancelled,
        Err(channel_handler::ChannelHandlerError::ConnectionLost(e)) => {
            ChannelHandlerResult::ConnectionLost(e)
        }
        Err(channel_handler::ChannelHandlerError::Protocol(e)) => {
            ChannelHandlerResult::ConnectionLost(e)
        }
    }
}

async fn connection_handler(
    connection_cancellation_token: CancellationToken,
    receiver_addr: SocketAddr,
    controller: NetworkingConnectionController,
    listener: NetworkingConnectionControlListener,
) -> Result<()> {

    // JoinSets abort their tasks on drop
    let mut channel_handlers = JoinSet::new();
    // we abort these because they else might send to closed controller channel
    let mut retries = JoinSet::new();

    // TODO maybe make retry counter per_channel property
    let mut retry = 1;
    loop {
        select! {
            _ = connection_cancellation_token.cancelled() => {
                // wait on all handlers to ensure that they drained their queues.
                let _ = channel_handlers.join_all();
                return Ok(());
            },
            ctrl_msg = listener.recv() => {
                match ctrl_msg {
                    Err(e) => panic!("broken close_channel or shutdown logic?! Error is {:?}", e),
                    Ok(NetworkingConnectionControlMessage::RegisterChannel(channel_id, submissing_queue)) => {
                        let socket = TcpSocket::new_v4()?;
                        let connection = match socket.connect(receiver_addr).await {
                            Ok(connection) => connection,
                            Err(e) => {
                                warn!("Could not establish connection {}. Retry in {} s", e, retry);
                                let controller = controller.clone();
                                retries.spawn(async move {
                                    tokio::time::sleep(Duration::from_secs(retry)).await;
                                    controller.send(NetworkingConnectionControlMessage::RegisterChannel(channel_id, submissing_queue)).await.unwrap();
                                });
                                retry += 1;
                                continue;
                            }
                        };

                        retry = 1;

                        let (mut reader, mut writer) = control_channel_sender(connection);

                        if let Err(e) = writer.send(ControlChannelRequest::ChannelRequest(channel_id.clone())).await {
                            warn!("Could not send channel creation request: {}", e);
                            continue;
                        }

                        let port = match reader.next().await {
                            Some(Ok(ControlChannelResponse::OkChannelResponse(port))) => port,
                            Some(Ok(ControlChannelResponse::DenyChannelResponse)) => panic!("why deny?!"),
                            Some(Err(e)) => {
                                warn!("Error receiving port from receiver node: {}", e);
                                continue;
                            },
                            None => {
                                warn!("connection to receiver node closed.");
                                continue;
                            }
                        };

                        let chan_addr = SocketAddr::new(receiver_addr.ip(), port);

                        channel_handlers.spawn({
                            async move {
                                let chan_res = channel_handler(chan_addr, submissing_queue.clone()).await;
                                info!("channel handler terminated with {:?}, ordering restart", chan_res);
                            }
                        });
                    }

                    Ok(NetworkingConnectionControlMessage::RetryChannel(_, _, _)) => {
                        panic!("not implemented");
                    }
                }
            }
        }
    }
}

impl NetworkService {
    pub fn start(runtime: Runtime) -> Arc<NetworkService> {
        let cancellation_token = CancellationToken::new();

        Arc::new(NetworkService {
            runtime: Mutex::new(Some(runtime)),
            cancellation_token,
            connections: Mutex::new(HashMap::new()),
            join_handles: Mutex::new(Vec::new()),
        })
    }

    pub fn register_channel(
        self: &Arc<NetworkService>,
        receiver_addr: ConnectionIdentifier,
        channel: ChannelIdentifier,
    ) -> Result<channel_handler::ChannelControlQueue> {
        let receiver_addr: SocketAddr = receiver_addr.parse()?;
        let (submission_queue_tx, submission_queue_rx) = async_channel::bounded::<ChannelControlMessage>(100);

        let mut connections = self.connections.lock().unwrap();
        let conn_controller = match connections.get(&receiver_addr) {
            None => {
                let (tx, rx) = async_channel::bounded::<NetworkingConnectionControlMessage>(1024);
                let control = tx.clone();
                let token = self.cancellation_token.clone();
                let mut locked_rt = self.runtime.lock().unwrap();
                let rt = locked_rt.take().unwrap();
                // TODO hier JoinSet?
                let handle = rt.spawn({
                        let token = token.clone();
                        let receiver_addr = receiver_addr.clone();
                        async move {
                            let x = connection_handler(token, receiver_addr, control, rx).await;
                            info!("Connection is terminated: {:?}", x);
                        }
                    }
                );
                locked_rt.replace(rt);
                connections.insert(receiver_addr.clone(), (token, tx.clone(), submission_queue_tx.clone()));
                self.join_handles.lock().unwrap().push(handle);
                tx
            },
            Some((_, tx, _)) => tx.clone()
        };
        conn_controller.send_blocking(NetworkingConnectionControlMessage::RegisterChannel(channel, submission_queue_rx))?;
        Ok(submission_queue_tx)
    }

    pub fn shutdown(self: Arc<NetworkService>) -> Result<()> {
        let connections = self.connections.lock().unwrap();
        for (_, (_, _, submission_queue)) in connections.iter() {
            submission_queue.send_blocking(channel_handler::ChannelControlMessage::Terminate).unwrap();
        }

        let runtime = self
            .runtime
            .lock()
            .expect("BUG: Nothing should panic while holding the lock")
            .take()
            .ok_or("Networking Service was stopped")?;
        self.cancellation_token.cancel();

        // wait until all connection_handlers completely stopped
        for handle in self.join_handles.lock().unwrap().iter_mut() {
            runtime.block_on(handle).unwrap();
        }

        thread::sleep(Duration::from_secs(1));
        assert!(runtime.metrics().num_alive_tasks() == 0);
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}

impl NetworkService {}
