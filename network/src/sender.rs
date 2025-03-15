use crate::protocol::*;
use futures::SinkExt;
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
    controller: NetworkingServiceController,
    cancellation_token: CancellationToken,
}

enum NetworkingServiceControlMessage {
    RegisterChannel(
        ConnectionIdentifier,
        ChannelIdentifier,
        oneshot::Sender<ChannelControlQueue>,
    ),
}
enum NetworkingConnectionControlMessage {
    RegisterChannel(ChannelIdentifier, oneshot::Sender<ChannelControlQueue>),
    RetryChannel(
        ChannelIdentifier,
        CancellationToken,
        channel_handler::ChannelControlQueueListener,
    ),
}
pub type ChannelControlMessage = channel_handler::ChannelControlMessage;
pub type ChannelControlQueue = channel_handler::ChannelControlQueue;
type NetworkingServiceController = async_channel::Sender<NetworkingServiceControlMessage>;
type NetworkingServiceControlListener = async_channel::Receiver<NetworkingServiceControlMessage>;
type NetworkingConnectionController = async_channel::Sender<NetworkingConnectionControlMessage>;
type NetworkingConnectionControlListener =
    async_channel::Receiver<NetworkingConnectionControlMessage>;

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
    use tokio_util::sync::CancellationToken;
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
        cancellation_token: CancellationToken,
        pending_writes: VecDeque<TupleBuffer>,
        wait_for_ack: HashMap<u64, TupleBuffer>,
        writer: DataChannelSenderWriter,
        reader: DataChannelSenderReader,
        queue: ChannelControlQueueListener,
    }

    type Result<T> = core::result::Result<T, ChannelHandlerError>;

    impl ChannelHandler {
        pub fn new(
            cancellation_token: CancellationToken,
            stream: TcpStream,
            queue: ChannelControlQueueListener,
        ) -> Self {
            let (reader, writer) = protocol::data_channel_sender(stream);

            Self {
                cancellation_token,
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
                        return Err(ChannelHandlerError::Protocol(
                            format!("Protocol Error. Unknown Seq {seq}").into(),
                        ));
                    }
                }
                DataChannelResponse::AckData(seq) => {
                    let Some(_) = self.wait_for_ack.remove(&seq) else {
                        return Err(ChannelHandlerError::Protocol(
                            format!("Protocol Error. Unknown Seq {seq}").into(),
                        ));
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
                .feed(DataChannelRequest::Data(next_buffer.clone()))
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
                if self.cancellation_token.is_cancelled() {
                    return Err(ChannelHandlerError::Cancelled);
                }

                if self.pending_writes.is_empty() || self.wait_for_ack.len() >= MAX_PENDING_ACKS {
                    select! {
                        _ = self.cancellation_token.cancelled() => {return Err(ChannelHandlerError::Cancelled);},
                        response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                    }
                } else {
                    select! {
                        _ = self.cancellation_token.cancelled() => {return Err(ChannelHandlerError::Cancelled);},
                        response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                        send_result = Self::send_pending(&mut self.writer, &mut self.pending_writes, &mut self.wait_for_ack) => send_result?,
                    }
                }
            }

            Ok(())
        }

        pub(super) async fn run(&mut self) -> Result<()> {
            loop {
                if self.cancellation_token.is_cancelled() {
                    return Err(ChannelHandlerError::Cancelled);
                }

                if self.pending_writes.is_empty() || self.wait_for_ack.len() >= MAX_PENDING_ACKS {
                    select! {
                        _ = self.cancellation_token.cancelled() => {return Err(ChannelHandlerError::Cancelled);},
                        response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                        request = self.queue.recv() => self.handle_request(request.map_err(|_| ChannelHandlerError::Cancelled)?).await?,
                    }
                } else {
                    select! {
                        _ = self.cancellation_token.cancelled() => {return Err(ChannelHandlerError::Cancelled);},
                        response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                        request = self.queue.recv() => self.handle_request(request.map_err(|_| ChannelHandlerError::Cancelled)?).await?,
                        send_result = Self::send_pending(&mut self.writer, &mut self.pending_writes, &mut self.wait_for_ack) => send_result?,
                    }
                }
            }
        }
    }
}


#[tracing::instrument]
async fn channel_handler(
    cancellation_token: CancellationToken,
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

    let connection = match cancellation_token
        .run_until_cancelled(socket.connect(channel_address))
        .await
    {
        None => return ChannelHandlerResult::Cancelled,
        Some(Err(e)) => return ChannelHandlerResult::ConnectionLost(e.into()),
        Some(Ok(connection)) => connection,
    };

    let mut handler = channel_handler::ChannelHandler::new(cancellation_token, connection, queue);
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

enum EstablishChannelResult {
    Ok(CancellationToken),
    ChannelReject(
        ChannelIdentifier,
        channel_handler::ChannelControlQueueListener,
    ),
    BadConnection(
        ChannelIdentifier,
        channel_handler::ChannelControlQueueListener,
        Error,
    ),
    Cancelled,
}

#[tracing::instrument]
async fn establish_channel(
    control_channel_sender_writer: &mut ControlChannelSenderWriter,
    control_channel_sender_reader: &mut ControlChannelSenderReader,
    connection_identifier: &ConnectionIdentifier,
    channel: ChannelIdentifier,
    channel_cancellation_token: CancellationToken,
    queue: channel_handler::ChannelControlQueueListener,
    controller: NetworkingConnectionController,
) -> EstablishChannelResult {
    match channel_cancellation_token
        .run_until_cancelled(
            control_channel_sender_writer
                .send(ControlChannelRequest::ChannelRequest(channel.clone())),
        )
        .await
    {
        None => return EstablishChannelResult::Cancelled,
        Some(Err(e)) => {
            return EstablishChannelResult::BadConnection(
                channel,
                queue,
                format!("Could not send channel creation request {}", e).into(),
            );
        }
        Some(Ok(())) => {}
    };

    let port = match channel_cancellation_token
        .run_until_cancelled(control_channel_sender_reader.next())
        .await
    {
        Some(Some(Ok(ControlChannelResponse::OkChannelResponse(port)))) => port,
        Some(Some(Ok(ControlChannelResponse::DenyChannelResponse))) => {
            return EstablishChannelResult::ChannelReject(channel, queue);
        }
        Some(Some(Err(e))) => {
            return EstablishChannelResult::BadConnection(channel, queue, e.into());
        }
        Some(None) => {
            return EstablishChannelResult::BadConnection(
                channel,
                queue,
                "Connection closed".into(),
            );
        }
        None => return EstablishChannelResult::Cancelled,
    };

    let channel_address = connection_identifier.parse::<SocketAddr>().expect("The Connection identifier should not be an invalid address, as the connection should have already been established");
    let channel_address = SocketAddr::new(channel_address.ip(), port);

    tokio::spawn(
        {
            let channel = channel.clone();
            let channel_cancellation_token = channel_cancellation_token.clone();
            async move {
                match channel_handler(
                    channel_cancellation_token.clone(),
                    channel_address,
                    queue.clone(),
                )
                .await
                {
                    ChannelHandlerResult::Cancelled => {}
                    ChannelHandlerResult::ConnectionLost(e) => {
                        warn!("Connection Lost: {e:?}");
                        match &channel_cancellation_token
                            .run_until_cancelled(controller.send(
                                NetworkingConnectionControlMessage::RetryChannel(
                                    channel,
                                    channel_cancellation_token.clone(),
                                    queue.clone(),
                                ),
                            ))
                            .await
                        {
                            None => {}
                            Some(Ok(())) => {
                                info!("Channel connection lost.Reopening channel");
                            }
                            Some(Err(_)) => {}
                        }
                    }
                    ChannelHandlerResult::Closed => {
                        // TODO restart channel: re-call establish channel one layer up?
                        // since  this is spawned, it needs to send a retry request to some channel
                        info!("Channel closed");
                    }
                }
            }
        }
        .instrument(info_span!("channel handler", channel = %channel, port = %port)),
    );
    EstablishChannelResult::Ok(channel_cancellation_token)
}

async fn accept_channel_requests(
    cancellation_token: CancellationToken,
    timeout: Duration,
    active_channel: &mut HashMap<ChannelIdentifier, CancellationToken>,
    listener: &mut NetworkingConnectionControlListener,
) -> Option<
    Vec<(
        ChannelIdentifier,
        CancellationToken,
        channel_handler::ChannelControlQueueListener,
    )>,
> {
    let mut pending = vec![];
    let mut requests = vec![];

    loop {
        match cancellation_token
            .run_until_cancelled(tokio::time::timeout(timeout, listener.recv()))
            .await
        {
            None => return None,             // cancelled
            Some(Err(_)) => break,           // timeout
            Some(Ok(Err(_))) => return None, // queue closed
            Some(Ok(Ok(request))) => {
                requests.push(request);
            }
        }
    }

    for request in requests {
        match request {
            NetworkingConnectionControlMessage::RegisterChannel(channel, response) => {
                let (sender, queue) = async_channel::bounded(100);
                let channel_cancellation = CancellationToken::new();
                if response.send(sender).is_err() {
                    warn!("Channel registration was canceled");
                    continue;
                }
                pending.push((channel, channel_cancellation, queue));
            }
            NetworkingConnectionControlMessage::RetryChannel(
                active,
                channel_cancellation,
                queue,
            ) => {
                active_channel.remove(&active);
                pending.push((active, channel_cancellation, queue));
            }
        }
    }

    Some(pending)
}

#[tracing::instrument]
async fn connection_handler(
    connection_cancellation_token: CancellationToken,
    connection_identifier: ConnectionIdentifier,
    controller: NetworkingConnectionController,
    mut listener: NetworkingConnectionControlListener,
) -> Result<()> {
    let mut pending_channels: Vec<(
        ChannelIdentifier,
        CancellationToken,
        channel_handler::ChannelControlQueueListener,
    )> = vec![];
    let mut active_channel: HashMap<ChannelIdentifier, CancellationToken> = HashMap::default();
    let on_cancel = |active_channel: HashMap<ChannelIdentifier, CancellationToken>| {
        active_channel
            .into_iter()
            .for_each(|(_, token)| token.cancel());
        Ok(())
    };

    let mut retry = 0;
    'connection: loop {
        info!("connection_handler 'connection loop");
        if let Some(mut new_pending_channels) = accept_channel_requests(
            connection_cancellation_token.clone(),
            Duration::from_millis(10),
            &mut active_channel,
            &mut listener,
        )
        .await
        {
            pending_channels.append(&mut new_pending_channels);
        } else {
            return on_cancel(active_channel);
        }

        let socket = TcpSocket::new_v4()?;
        let connection = match connection_cancellation_token
            .run_until_cancelled(socket.connect(connection_identifier.parse()?))
            .await
        {
            None => return on_cancel(active_channel),
            Some(Ok(connection)) => connection,
            Some(Err(e)) => {
                retry += 1;
                warn!("Could not establish connection {}.Retry in {} s", e, retry);
                tokio::time::sleep(Duration::from_secs(retry)).await;
                continue;
            }
        };

        let (mut reader, mut writer) = control_channel_sender(connection);

        retry = 0;

        info!("connection_handler inner loop");
        loop {
            if let Some(mut new_pending_channels) = accept_channel_requests(
                connection_cancellation_token.clone(),
                Duration::from_millis(10),
                &mut active_channel,
                &mut listener,
            )
            .await
            {
                pending_channels.append(&mut new_pending_channels);
            } else {
                return on_cancel(active_channel);
            }

            let mut updated_pending_channels = vec![];
            let mut connection_failure = false;
            for (channel, channel_cancellation_token, queue) in pending_channels {
                if channel_cancellation_token.is_cancelled() {
                    continue;
                }
                if connection_failure {
                    updated_pending_channels.push((channel, channel_cancellation_token, queue));
                    continue;
                }
                match connection_cancellation_token
                    .run_until_cancelled(establish_channel(
                        &mut writer,
                        &mut reader,
                        &connection_identifier,
                        channel.clone(),
                        channel_cancellation_token.clone(),
                        queue,
                        controller.clone(),
                    ))
                    .await
                {
                    None | Some(EstablishChannelResult::Cancelled) => {
                        return on_cancel(active_channel);
                    }
                    Some(EstablishChannelResult::Ok(token)) => {
                        active_channel.insert(channel, token);
                    }
                    Some(EstablishChannelResult::ChannelReject(channel, queue)) => {
                        warn!("Channel {} was rejected", channel);
                        updated_pending_channels.push((
                            channel,
                            channel_cancellation_token.clone(),
                            queue,
                        ));
                    }
                    Some(EstablishChannelResult::BadConnection(channel, queue, e)) => {
                        warn!("Could not establish channel: {e:?}");
                        connection_failure = true;
                        updated_pending_channels.push((
                            channel,
                            channel_cancellation_token.clone(),
                            queue,
                        ));
                    }
                }
            }
            pending_channels = updated_pending_channels;
            if connection_failure {
                continue 'connection;
            }
        }
    }
}
#[tracing::instrument]
async fn create_connection(
    connection: &ConnectionIdentifier,
) -> Result<(CancellationToken, NetworkingConnectionController)> {
    let (tx, rx) = async_channel::bounded::<NetworkingConnectionControlMessage>(1024);
    let control = tx.clone();
    let token = CancellationToken::new();
    tokio::spawn(
        {
            let token = token.clone();
            let connection = connection.clone();
            async move {
                info!(
                    "Connection is terminated: {:?}",
                    connection_handler(token, connection, control, rx).await
                );
            }
        }
        .instrument(info_span!(
            "connection_handler",
            connection = connection.clone()
        )),
    );
    Ok((token, tx))
}

async fn on_cancel(active_channel: HashMap<ConnectionIdentifier, (CancellationToken, NetworkingConnectionController)>) -> Result<()> {
    active_channel
        .into_iter()
        .for_each(|(_, (token, _))| token.cancel());
    return Ok(());
}

#[tracing::instrument]
async fn network_sender_dispatcher(
    cancellation_token: CancellationToken,
    control: NetworkingServiceControlListener,
) -> Result<()> {
    let mut connections: HashMap<
        ConnectionIdentifier,
        (CancellationToken, NetworkingConnectionController),
    > = HashMap::default();

    loop {
        info!("network_sender_dispatcher loop");
        match cancellation_token.run_until_cancelled(control.recv()).await {
            None => return on_cancel(connections).await,
            Some(Err(_)) => return Err("Queue was closed".into()),
            Some(Ok(NetworkingServiceControlMessage::RegisterChannel(connection, channel, tx))) => {
                info!("received: RegisterChannel {} {}", connection, channel);
                if !connections.contains_key(&connection) {
                    match cancellation_token
                        .run_until_cancelled(create_connection(&connection))
                        .await
                    {
                        None => return on_cancel(connections).await,
                        Some(Ok((token, controller))) => {
                            connections.insert(connection.clone(), (token, controller));
                        }
                        Some(Err(e)) => {
                            return Err(e);
                        }
                    }
                }

                match cancellation_token
                    .run_until_cancelled(
                        connections
                            .get(&connection)
                            .expect("BUG: Just inserted the key")
                            .1
                            .send(NetworkingConnectionControlMessage::RegisterChannel(
                                channel, tx,
                            )),
                    )
                    .await
                {
                    None => return on_cancel(connections).await,
                    Some(Err(e)) => {
                        return Err(e)?;
                    }
                    Some(Ok(())) => {}
                }
            }
        }
    }
}

impl NetworkService {
    #[tracing::instrument]
    pub fn start(runtime: Runtime) -> Arc<NetworkService> {
        let (controller, listener) = async_channel::bounded(5);
        let cancellation_token = CancellationToken::new();
        runtime.spawn({
            let token = cancellation_token.clone();
            async move {
                info!("Starting sender network service");
                info!(
                    "sender network service stopped: {:?}",
                    network_sender_dispatcher(token, listener).await
                );
            }
        });

        Arc::new(NetworkService {
            runtime: Mutex::new(Some(runtime)),
            cancellation_token,
            controller,
        })
    }

    pub fn register_channel(
        self: &Arc<NetworkService>,
        connection: ConnectionIdentifier,
        channel: ChannelIdentifier,
    ) -> Result<channel_handler::ChannelControlQueue> {
        let (tx, rx) = oneshot::channel();
        let Ok(_) =
            self.controller
                .send_blocking(NetworkingServiceControlMessage::RegisterChannel(
                    connection, channel, tx,
                ))
        else {
            return Err("Network Service Closed".into());
        };

        rx.blocking_recv()
            .map_err(|_| "Network Service Closed".into())
    }

    // TODO close channel when query terminated

    pub fn shutdown(self: Arc<NetworkService>) -> Result<()> {
        let runtime = self
            .runtime
            .lock()
            .expect("BUG: Nothing should panic while holding the lock")
            .take()
            .ok_or("Networking Service was stopped")?;
        self.cancellation_token.cancel();
        thread::sleep(Duration::from_secs(2));
        assert!(runtime.metrics().num_alive_tasks() == 0);
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}

impl NetworkService {}
