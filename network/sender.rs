use crate::protocol::*;
use crate::sender::ChannelHandlerResult::ConnectionLost;
use crate::sender::EstablishChannelResult::{BadConnection, BadProtocol, ChannelReject};
use async_channel::RecvError;
use bytes::{Buf, Bytes, BytesMut};
use log::{info, warn};
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::error::{SendError, TryRecvError};
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{error, info_span, instrument, trace, Instrument};
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::format;

pub type DataQueue = async_channel::Sender<TupleBuffer>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub struct NetworkService {
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    controller: NetworkingServiceController,
    cancellation_token: CancellationToken,
}

enum NetworkingServiceControlMessage {
    RegisterChannel(
        ConnectionIdentifier,
        ChannelIdentifier,
        tokio::sync::oneshot::Sender<(CancellationToken, DataQueue)>,
    ),
}
enum NetworkingConnectionControlMessage {
    RegisterChannel(
        ChannelIdentifier,
        tokio::sync::oneshot::Sender<(CancellationToken, DataQueue)>,
    ),
    RetryChannel(
        ChannelIdentifier,
        CancellationToken,
        async_channel::Receiver<TupleBuffer>,
    ),
}
type NetworkingServiceController = tokio::sync::mpsc::Sender<NetworkingServiceControlMessage>;
type NetworkingServiceControlListener =
    tokio::sync::mpsc::Receiver<NetworkingServiceControlMessage>;
type NetworkingConnectionController = tokio::sync::mpsc::Sender<NetworkingConnectionControlMessage>;
type NetworkingConnectionControlListener =
    tokio::sync::mpsc::Receiver<NetworkingConnectionControlMessage>;

enum ChannelHandlerResult {
    Cancelled,
    ConnectionLost(Box<dyn std::error::Error + Send + Sync>),
    Closed,
}

enum DataResponse {
    Ok(u64),
    NotOk(u64),
}

fn try_read_response(
    mut reader: &mut TcpStream,
    buffer: &mut BytesMut,
) -> Result<Vec<DataResponse>> {
    let mut responses = vec![];
    loop {
        let n = match reader.try_read_buf(buffer) {
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                break;
            }
            Err(e) => {
                error!("Error while waiting for response: {e:?}");
                return Err(e.into());
            }
            Ok(0) => return Err("Connection closed".into()),
            Ok(n) => n,
        };

        let txt_responses =
            from_utf8(&buffer[..]).map_err(|e| "Protocol Error expected utf8 response")?;
        let mut index = 0;
        {
            let txt_responses = txt_responses.split('\n').collect::<Vec<_>>();
            for response in txt_responses.into_iter().rev().skip(1).rev() {
                index += response.len() + 1;
                if response.starts_with("OK ") {
                    let seq = response
                        .strip_prefix("OK ")
                        .unwrap()
                        .parse::<u64>()
                        .map_err(|e| "Protocol Error expected sequence number in response")?;
                    responses.push(DataResponse::Ok(seq));
                } else if response.starts_with("NOT OK ") {
                    let seq = response
                        .strip_prefix("NOT OK ")
                        .unwrap()
                        .parse::<u64>()
                        .map_err(|e| "Protocol Error expected sequence number in response")?;
                    responses.push(DataResponse::NotOk(seq));
                } else {
                    return Err("Protocol Error expected \"OK <seq>\"/ \"NOT OK <seq>\"".into());
                }
            }
        }

        let (beginning, end) = buffer[..].split_at_mut(index);
        let length = end.len();
        beginning[0..length].copy_from_slice(end);
        buffer.resize(length, 0);
    }
    Ok(responses)
}
async fn channel_handler(
    cancellation_token: CancellationToken,
    port: u16,
    queue: async_channel::Receiver<TupleBuffer>,
) -> ChannelHandlerResult {
    let mut wait_for_ack = HashMap::new();
    let window_size = 32;

    let socket = match TcpSocket::new_v4() {
        Ok(socket) => socket,
        Err(e) => return ChannelHandlerResult::ConnectionLost("Could not create socket".into()),
    };

    let mut connection = match cancellation_token
        .run_until_cancelled(socket.connect(format!("127.0.0.1:{port}").parse().unwrap()))
        .await
    {
        None => return ChannelHandlerResult::Cancelled,
        Some(Err(e)) => return ChannelHandlerResult::ConnectionLost(e.into()),
        Some(Ok(connection)) => connection,
    };

    let mut response_buffer = BytesMut::with_capacity(256);
    let mut pending_writes: Vec<TupleBuffer> = vec![];
    loop {
        let responses = match try_read_response(&mut connection, &mut response_buffer) {
            Ok(res) => res,
            Err(e) => return ChannelHandlerResult::ConnectionLost(e.into()),
        };

        for response in responses {
            match response {
                DataResponse::Ok(seq) => {
                    info!("Ack for {seq}");
                    wait_for_ack.remove(&seq);
                }
                DataResponse::NotOk(seq) => {
                    let Some(write) = wait_for_ack.remove(&seq) else {
                        return ChannelHandlerResult::ConnectionLost(
                            "Protocol Error: Receiver acknowledged a sequence that does not exist"
                                .into(),
                        );
                    };
                    pending_writes.push(write);
                }
            }
        }
        for pending_write in pending_writes {
            match cancellation_token
                .run_until_cancelled(pending_write.serialize(&mut connection))
                .await
            {
                None => {
                    return ChannelHandlerResult::Cancelled;
                }
                Some(Ok(())) => {
                    wait_for_ack.insert(pending_write.sequence_number, pending_write);
                }
                Some(Err(e)) => {
                    return ChannelHandlerResult::ConnectionLost(e.into());
                }
            }
        }
        pending_writes = vec![];

        while wait_for_ack.len() < window_size {
            match queue.try_recv() {
                Err(async_channel::TryRecvError::Empty) => break,
                Ok(data) => {
                    pending_writes.push(data);
                }
                Err(async_channel::TryRecvError::Closed) => {
                    panic!("Data Channel Queue should not be closed");
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

enum EstablishChannelResult {
    Ok(CancellationToken),
    ChannelReject(ChannelIdentifier, async_channel::Receiver<TupleBuffer>),
    BadConnection(
        ChannelIdentifier,
        async_channel::Receiver<TupleBuffer>,
        Error,
    ),
    BadProtocol,
}

async fn establish_channel(
    connection: &mut TcpStream,
    channel: ChannelIdentifier,
    channel_cancellation_token: CancellationToken,
    queue: async_channel::Receiver<TupleBuffer>,
    controller: NetworkingConnectionController,
) -> EstablishChannelResult {
    if let Err(e) = connection
        .write_all(format!("{channel}\n").as_bytes())
        .await
    {
        return BadConnection(
            channel,
            queue,
            format!("Could not send channel creation request {}", e).into(),
        );
    }

    let mut buffer = BytesMut::with_capacity(64);
    match connection.read_buf(&mut buffer).await {
        Ok(bytes_read) if bytes_read == 0 => {
            return BadConnection(channel, queue, "Connection closed".into());
        }
        Err(e) => {
            return BadConnection(
                channel,
                queue,
                format!("Could not read channel creation response {}", e).into(),
            );
        }
        _ => {}
    };

    let Ok(response) = from_utf8(&buffer) else {
        return BadProtocol;
    };

    assert!(response.ends_with('\n'));

    if response == "NOT OK\n" {
        return ChannelReject(channel, queue);
    }

    if response.starts_with("OK ") {
        let response = response
            .strip_prefix("OK ")
            .unwrap()
            .strip_suffix('\n')
            .unwrap();
        let Ok(port) = response.parse::<u16>() else {
            return BadProtocol;
        };
        tokio::spawn(
            {
                let channel = channel.clone();
                let channel_cancellation_token = channel_cancellation_token.clone();
                async move {
                    match channel_handler(channel_cancellation_token.clone(), port, queue.clone())
                        .await
                    {
                        ChannelHandlerResult::Cancelled => {
                            return;
                        }
                        ChannelHandlerResult::ConnectionLost(_) => {
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
                                None => {
                                    return;
                                }
                                Some(Ok(())) => {
                                    info!("Channel connection lost.Reopening channel");
                                }
                                Some(Err(_)) => {}
                            }
                        }
                        ChannelHandlerResult::Closed => {
                            info!("Channel closed");
                        }
                    }
                }
            }
            .instrument(info_span!("channel handler", channel = %channel, port = %port)),
        );
        return EstablishChannelResult::Ok(channel_cancellation_token);
    };
    EstablishChannelResult::BadProtocol
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
        async_channel::Receiver<TupleBuffer>,
    )>,
> {
    let mut pending = vec![];
    let mut requests = vec![];

    loop {
        match cancellation_token
            .run_until_cancelled(tokio::time::timeout(timeout, listener.recv()))
            .await
        {
            None => return None,           // cancelled
            Some(Err(_)) => break,         // timeout
            Some(Ok(None)) => return None, // queue closed
            Some(Ok(Some(request))) => {
                requests.push(request);
            }
        }
    }

    for request in requests {
        match request {
            NetworkingConnectionControlMessage::RegisterChannel(channel, response) => {
                let (sender, queue) = async_channel::bounded(100);
                let channel_cancellation = CancellationToken::new();
                if let Err(_) = response.send((channel_cancellation.clone(), sender)) {
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

async fn connection_handler(
    connection_cancellation_token: CancellationToken,
    connection: ConnectionIdentifier,
    controller: NetworkingConnectionController,
    mut listener: NetworkingConnectionControlListener,
) -> Result<()> {
    let mut pending_channels: Vec<(
        ChannelIdentifier,
        CancellationToken,
        async_channel::Receiver<TupleBuffer>,
    )> = vec![];
    let mut active_channel: HashMap<ChannelIdentifier, CancellationToken> = HashMap::default();
    let on_cancel = |active_channel: HashMap<ChannelIdentifier, CancellationToken>| {
        active_channel
            .into_iter()
            .for_each(|(channel, token)| token.cancel());
        return Ok(());
    };

    let mut retry = 0;
    'connection: loop {
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
        let mut connection = match connection_cancellation_token
            .run_until_cancelled(socket.connect(format!("127.0.0.1:{connection}").parse()?))
            .await
        {
            None => return on_cancel(active_channel),
            Some(Ok(connection)) => connection,
            Some(Err(e)) => {
                retry += 1;
                warn!("Could not establish connection {}.Retry in {} s", e, retry);
                tokio::time::sleep(std::time::Duration::from_secs(retry)).await;
                continue;
            }
        };

        retry = 0;

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
                        &mut connection,
                        channel.clone(),
                        channel_cancellation_token.clone(),
                        queue,
                        controller.clone(),
                    ))
                    .await
                {
                    None => return on_cancel(active_channel),
                    Some(EstablishChannelResult::Ok(token)) => {
                        active_channel.insert(channel, token);
                    }
                    Some(ChannelReject(channel, queue)) => {
                        warn!("Channel {} was rejected", channel);
                        updated_pending_channels.push((
                            channel,
                            channel_cancellation_token.clone(),
                            queue,
                        ));
                    }
                    Some(BadProtocol) => {
                        return Err("Bad Protocol".into());
                    }
                    Some(BadConnection(channel, queue, e)) => {
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
async fn create_connection(
    connection: ConnectionIdentifier,
) -> Result<(CancellationToken, NetworkingConnectionController)> {
    let (tx, rx) = tokio::sync::mpsc::channel::<NetworkingConnectionControlMessage>(1024);
    let control = tx.clone();
    let token = tokio_util::sync::CancellationToken::new();
    tokio::spawn({
        let token = token.clone();
        async move {
            info!(
                "Connection is terminated: {:?}",
                connection_handler(token, connection, control, rx).await
            );
        }
        .instrument(tracing::info_span!(
            "connection_handler",
            connection = connection
        ))
    });
    Ok((token, tx))
}

async fn network_sender_dispatcher(
    cancellation_token: CancellationToken,
    mut control: NetworkingServiceControlListener,
) -> Result<()> {
    let mut connections: HashMap<u16, (CancellationToken, NetworkingConnectionController)> =
        HashMap::default();
    let on_cancel =
        |active_channel: HashMap<u16, (CancellationToken, NetworkingConnectionController)>| {
            active_channel
                .into_iter()
                .for_each(|(_, (token, _))| token.cancel());
            return Ok(());
        };

    loop {
        match cancellation_token.run_until_cancelled(control.recv()).await {
            None => return on_cancel(connections),
            Some(None) => return Err("Queue was closed".into()),
            Some(Some(NetworkingServiceControlMessage::RegisterChannel(
                connection,
                channel,
                tx,
            ))) => {
                if !connections.contains_key(&connection) {
                    match cancellation_token
                        .run_until_cancelled(create_connection(connection))
                        .await
                    {
                        None => return on_cancel(connections),
                        Some(Ok((token, controller))) => {
                            connections.insert(connection, (token, controller));
                        }
                        Some(Err(e)) => {
                            return Err(e);
                        }
                    }
                }

                match cancellation_token
                    .run_until_cancelled(connections.get(&connection).unwrap().1.send(
                        NetworkingConnectionControlMessage::RegisterChannel(channel, tx),
                    ))
                    .await
                {
                    None => return on_cancel(connections),
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
    pub fn start(runtime: Runtime) -> Arc<NetworkService> {
        let (controller, listener) = tokio::sync::mpsc::channel(5);
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
    ) -> Result<(CancellationToken, DataQueue)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.runtime
            .lock()
            .unwrap()
            .as_ref()
            .ok_or("Networking Service was stopped")?
            .block_on(async move {
                let Ok(()) = self
                    .controller
                    .send(NetworkingServiceControlMessage::RegisterChannel(
                        connection, channel, tx,
                    ))
                    .await
                else {
                    return Err("Could not send request".into());
                };

                rx.await
                    .map_err(|e| format!("Could not receive: {e}").into())
            })
    }

    pub fn shutdown(self: Arc<NetworkService>) -> Result<()> {
        let runtime = self
            .runtime
            .lock()
            .unwrap()
            .take()
            .ok_or("Networking Service was stopped")?;
        self.cancellation_token.cancel();
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}

impl NetworkService {}
