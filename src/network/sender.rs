use crate::engine::Data;
use crate::network::protocol::*;
use crate::network::sender::EstablishChannelResult::{BadConnection, BadProtocol, ChannelReject};
use bytes::BytesMut;
use log::{info, warn};
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::error::{SendError, TryRecvError};
use tokio::time::error::Elapsed;
use tokio_util::sync::CancellationToken;
use tracing::{error, info_span, instrument, Instrument};
use tracing_subscriber::fmt::format;

pub(crate) type DataQueue = async_channel::Sender<Data>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error>;
pub(crate) struct NetworkService {
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
        async_channel::Receiver<Data>,
    ),
}
type NetworkingServiceController = tokio::sync::mpsc::Sender<NetworkingServiceControlMessage>;
type NetworkingServiceControlListener =
    tokio::sync::mpsc::Receiver<NetworkingServiceControlMessage>;
type NetworkingConnectionController = tokio::sync::mpsc::Sender<NetworkingConnectionControlMessage>;
type NetworkingConnectionControlListener =
    tokio::sync::mpsc::Receiver<NetworkingConnectionControlMessage>;
async fn channel_handler(
    cancellation_token: CancellationToken,
    channel: ChannelIdentifier,
    port: u16,
    queue: async_channel::Receiver<Data>,
    controller: NetworkingConnectionController,
) -> Result<()> {
    let mut retry = 0;
    let mut pending_writes = None;
    'connection: loop {
        let socket = TcpSocket::new_v4()?;
        let mut connection = match cancellation_token
            .run_until_cancelled(socket.connect(format!("127.0.0.1:{port}").parse()?))
            .await
        {
            None => {
                return Ok(());
            }
            Some(Ok(mut connection)) => connection,
            Some(Err(e)) => {
                if retry > 2 {
                    warn!("closing channel. Attempt to recreate channel");
                    if let Some(Err(e)) = cancellation_token
                        .run_until_cancelled(controller.send(
                            NetworkingConnectionControlMessage::RetryChannel(
                                channel,
                                cancellation_token.clone(),
                                queue.clone(),
                            ),
                        ))
                        .await
                    {
                        warn!("could not send recreate request to controller: {}.", e);
                    }

                    return Ok(());
                }
                warn!("Could not establish channel connection: {e}. Retry in {retry}");
                retry += 1;
                if let None = cancellation_token
                    .run_until_cancelled(tokio::time::sleep(Duration::from_secs(retry)))
                    .await
                {
                    return Ok(());
                }
                continue;
            }
        };

        retry = 0;

        loop {
            let mut data = match pending_writes.take() {
                Some(data) => data,
                None => match cancellation_token.run_until_cancelled(queue.recv()).await {
                    Some(result) => result?,
                    None => return Ok(()),
                },
            };

            match cancellation_token
                .run_until_cancelled(connection.write_buf(&mut data.bytes))
                .await
            {
                None => {
                    return Ok(());
                }
                Some(Ok(0)) => {
                    warn!("Could not send data to channel: wrote 0 bytes",);
                    pending_writes = Some(data);
                    retry += 1;
                    continue 'connection;
                }
                Some(Err(e)) => {
                    warn!("Could not send data to channel: {e}");
                    pending_writes = Some(data);
                    retry += 1;
                    continue 'connection;
                }
                _ => {}
            };
        }
    }
    Ok(())
}

enum EstablishChannelResult {
    Ok(CancellationToken),
    ChannelReject(ChannelIdentifier, async_channel::Receiver<Data>),
    BadConnection(ChannelIdentifier, async_channel::Receiver<Data>, Error),
    BadProtocol,
}

async fn establish_channel(
    connection: &mut TcpStream,
    channel: ChannelIdentifier,
    channel_cancellation_token: CancellationToken,
    queue: async_channel::Receiver<Data>,
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
                    info!("Starting channel handler for port {}", port);
                    info!(
                        "Channel handler for port {} stopped: {:?}",
                        port,
                        channel_handler(
                            channel_cancellation_token,
                            channel,
                            port,
                            queue,
                            controller
                        )
                        .await
                    );
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
        async_channel::Receiver<Data>,
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
        async_channel::Receiver<Data>,
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
                warn!("Could not establish connection {}. Retry in {} s", e, retry);
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
    pub(crate) fn start(runtime: Runtime) -> Arc<NetworkService> {
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
