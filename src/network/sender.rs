use crate::engine::Data;
use crate::network::protocol::*;
use crate::network::receiver::EmitFn;
use crate::network::sender::EstablishChannelResult::{BadConnection, BadProtocol, ChannelReject};
use bytes::{Buf, BytesMut};
use log::{info, warn};
use std::collections::HashMap;
use std::future::Future;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Runtime;
use tokio::time::error::Elapsed;
use tracing::{info_span, instrument, Instrument};
use tracing_subscriber::fmt::format;

type DataQueue = async_channel::Sender<Data>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error>;
pub(crate) struct NetworkService {
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    controller: NetworkingServiceController,
}

enum NetworkingServiceControlMessage {
    Stop,
    RegisterChannel(
        ConnectionIdentifier,
        ChannelIdentifier,
        tokio::sync::oneshot::Sender<DataQueue>,
    ),
}
enum NetworkingConnectionControlMessage {
    Stop,
    RegisterChannel(ChannelIdentifier, tokio::sync::oneshot::Sender<DataQueue>),
    RetryChannel(ChannelIdentifier, async_channel::Receiver<Data>),
}
type NetworkingServiceController = tokio::sync::mpsc::Sender<NetworkingServiceControlMessage>;
type NetworkingServiceControlListener =
    tokio::sync::mpsc::Receiver<NetworkingServiceControlMessage>;
type NetworkingConnectionController = tokio::sync::mpsc::Sender<NetworkingConnectionControlMessage>;
type NetworkingConnectionControlListener =
    tokio::sync::mpsc::Receiver<NetworkingConnectionControlMessage>;
#[instrument(skip(queue, controller))]
async fn channel_handler(
    channel: ChannelIdentifier,
    port: u16,
    queue: async_channel::Receiver<Data>,
    controller: NetworkingConnectionController,
) -> Result<()> {
    let mut retry = 0;
    let mut pending_writes = None;
    'connection: loop {
        let socket = TcpSocket::new_v4()?;

        let Ok(mut connection) = socket.connect(format!("127.0.0.1:{port}").parse()?).await else {
            if retry > 2 {
                warn!("closing channel. Attempt to recreate channel");
                if let Err(e) = controller
                    .send(NetworkingConnectionControlMessage::RetryChannel(
                        channel,
                        queue.clone(),
                    ))
                    .await
                {
                    warn!("could not send recreate request to controller: {}.", e);
                }

                return Ok(());
            }
            warn!("Could not establish channel connection. Retry in {retry}");
            retry += 1;
            tokio::time::sleep(Duration::from_secs(retry)).await;
            continue;
        };

        retry = 0;

        loop {
            let mut data = match pending_writes.take() {
                Some(data) => data,
                None => queue.recv().await?,
            };
            info!(
                "sending data to channel: {}",
                from_utf8(&data.bytes).unwrap()
            );

            let Ok(bytes_written) = connection.write_buf(&mut data.bytes).await else {
                warn!(
                    "Could not send data to channel: {}",
                    from_utf8(&data.bytes).unwrap()
                );
                pending_writes = Some(data);
                retry += 1;
                continue 'connection;
            };

            if bytes_written == 0 {
                warn!(
                    "Could not send data to channel: {}",
                    from_utf8(&data.bytes).unwrap()
                );
                pending_writes = Some(data);
                retry += 1;
                continue 'connection;
            }
        }
    }
    Ok(())
}

enum EstablishChannelResult {
    Ok,
    ChannelReject(ChannelIdentifier, async_channel::Receiver<Data>),
    BadConnection(ChannelIdentifier, async_channel::Receiver<Data>, Error),
    BadProtocol,
}

#[instrument(skip(connection, queue, controller))]
async fn establish_channel(
    connection: &mut TcpStream,
    channel: ChannelIdentifier,
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
                async move {
                    info!("Starting channel handler for port {}", port);
                    info!(
                        "Channel handler for port {} stopped: {:?}",
                        port,
                        channel_handler(channel, port, queue, controller).await
                    );
                }
            }
            .instrument(info_span!("channel handler", channel = %channel, port = %port)),
        );
    }

    EstablishChannelResult::Ok
}
#[instrument(skip(listener, controller))]
async fn connection_handler(
    connection: ConnectionIdentifier,
    controller: NetworkingConnectionController,
    mut listener: NetworkingConnectionControlListener,
) -> Result<()> {
    let mut pending_channels = vec![];
    let mut retry = 0;
    'connection: loop {
        let socket = TcpSocket::new_v4()?;
        let mut connection = match socket
            .connect(format!("127.0.0.1:{connection}").parse()?)
            .await
        {
            Ok(connection) => connection,
            Err(e) => {
                retry += 1;
                warn!("Could not establish connection {}. Retry in {} s", e, retry);
                tokio::time::sleep(std::time::Duration::from_secs(retry)).await;
                continue;
            }
        };

        retry = 0;

        loop {
            let mut updated_pending_channels = vec![];
            let mut connection_failure = false;
            for (channel, queue) in pending_channels {
                if connection_failure {
                    updated_pending_channels.push((channel, queue));
                    continue;
                }

                match establish_channel(&mut connection, channel, queue, controller.clone()).await {
                    EstablishChannelResult::Ok => {}
                    ChannelReject(channel, queue) => {
                        updated_pending_channels.push((channel, queue));
                    }
                    BadProtocol => {
                        return Err("Bad Protocol".into());
                    }
                    BadConnection(channel, queue, e) => {
                        connection_failure = true;
                        updated_pending_channels.push((channel, queue));
                    }
                }
            }
            pending_channels = updated_pending_channels;
            if connection_failure {
                continue 'connection;
            }

            match tokio::time::timeout(Duration::from_millis(50), listener.recv()).await {
                Ok(Some(NetworkingConnectionControlMessage::Stop)) => {
                    return Ok(());
                }
                Ok(Some(NetworkingConnectionControlMessage::RetryChannel(channel, queue))) => {
                    match establish_channel(&mut connection, channel, queue, controller.clone())
                        .await
                    {
                        EstablishChannelResult::Ok => {}
                        ChannelReject(channel, queue) => {
                            pending_channels.push((channel, queue));
                        }
                        BadProtocol => {
                            return Err("Bad Protocol".into());
                        }
                        BadConnection(channel, queue, e) => {
                            pending_channels.push((channel, queue));
                            continue 'connection;
                        }
                    }
                }
                Ok(Some(NetworkingConnectionControlMessage::RegisterChannel(channel, tx))) => {
                    let (sender, queue) = async_channel::bounded(100);
                    tx.send(sender).unwrap();
                    match establish_channel(&mut connection, channel, queue, controller.clone())
                        .await
                    {
                        EstablishChannelResult::Ok => {}
                        ChannelReject(channel, queue) => {
                            pending_channels.push((channel, queue));
                        }
                        BadProtocol => {
                            return Err("Bad Protocol".into());
                        }
                        BadConnection(channel, queue, e) => {
                            pending_channels.push((channel, queue));
                            continue 'connection;
                        }
                    }
                }
                Ok(None) => {
                    return Ok(());
                }
                Err(_) => continue,
            }
        }
    }
}
#[instrument]
async fn create_connection(
    connection: ConnectionIdentifier,
) -> Result<NetworkingConnectionController> {
    let (tx, rx) = tokio::sync::mpsc::channel::<NetworkingConnectionControlMessage>(1024);
    let control = tx.clone();
    tokio::spawn(async move {
        info!(
            "Connection is terminated: {:?}",
            connection_handler(connection, control, rx).await
        );
    });
    Ok(tx)
}

async fn network_sender_dispatcher(mut control: NetworkingServiceControlListener) -> Result<()> {
    let mut connections: HashMap<u16, NetworkingConnectionController> = HashMap::default();

    loop {
        match control.recv().await.ok_or("control queue closed")? {
            NetworkingServiceControlMessage::Stop => {
                return Ok(());
            }
            NetworkingServiceControlMessage::RegisterChannel(connection, channel, tx) => {
                if !connections.contains_key(&connection) {
                    connections.insert(connection, create_connection(connection).await?);
                }
                connections
                    .get(&connection)
                    .unwrap()
                    .send(NetworkingConnectionControlMessage::RegisterChannel(
                        channel, tx,
                    ))
                    .await?;
            }
        }
    }
}
impl NetworkService {
    pub(crate) fn start(runtime: Runtime) -> Arc<NetworkService> {
        let (controller, listener) = tokio::sync::mpsc::channel(5);

        runtime.spawn(async move {
            info!("Starting sender network service");
            info!(
                "sender network service stopped: {:?}",
                network_sender_dispatcher(listener).await
            );
        });

        Arc::new(NetworkService {
            runtime: Mutex::new(Some(runtime)),
            controller,
        })
    }

    pub fn register_channel(
        self: &Arc<NetworkService>,
        connection: ConnectionIdentifier,
        channel: ChannelIdentifier,
    ) -> Result<DataQueue> {
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
        runtime.block_on(self.controller.send(NetworkingServiceControlMessage::Stop))?;
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}

impl NetworkService {}
