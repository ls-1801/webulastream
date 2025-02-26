use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::protocol::*;
use crate::{protocol, sender};
use bytes::BytesMut;
use futures::SinkExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::RwLock;
use tokio::{select, stream};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, trace, warn, Instrument, Span};

pub struct NetworkService {
    sender: NetworkingServiceController,
    runtime: Mutex<Option<Runtime>>,
}

enum NetworkingServiceControl {
    Stop,
    RetryChannel(ChannelIdentifier, DataQueue, CancellationToken),
    RegisterChannel(
        ChannelIdentifier,
        DataQueue,
        oneshot::Sender<CancellationToken>,
    ),
}
type DataQueue = async_channel::Sender<TupleBuffer>;
type NetworkingServiceController = tokio::sync::mpsc::Sender<NetworkingServiceControl>;
type NetworkingServiceControlListener = tokio::sync::mpsc::Receiver<NetworkingServiceControl>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type EmitFn = Box<dyn FnMut(TupleBuffer) -> bool + Send + Sync>;

enum ChannelHandlerError {
    ClosedByOtherSide,
    Cancelled,
    Network(Error),
    Timeout,
}

type RegisteredChannels = Arc<RwLock<HashMap<ChannelIdentifier, (DataQueue, CancellationToken)>>>;
async fn channel_handler(
    cancellation_token: CancellationToken,
    queue: &mut DataQueue,
    listener: &mut TcpListener,
) -> core::result::Result<(), ChannelHandlerError> {
    let (mut stream, address) = match cancellation_token
        .run_until_cancelled(tokio::time::timeout(
            Duration::from_secs(3),
            listener.accept(),
        ))
        .await
    {
        None => return Err(ChannelHandlerError::Cancelled),
        Some(Err(_)) => return Err(ChannelHandlerError::Timeout),
        Some(Ok(Err(e))) => return Err(ChannelHandlerError::Network(e.into())),
        Some(Ok(Ok((stream, address)))) => (stream, address),
    };

    let (mut reader, mut writer) = protocol::data_channel_receiver(stream);

    let mut pending_buffer: Option<TupleBuffer> = None;
    info!("Accepted TupleBuffer channel connection from {address}");
    loop {
        if let Some(pending_buffer) = pending_buffer.take() {
            let sequence = pending_buffer.sequence_number;
            select! {
                _ = cancellation_token.cancelled() => return Err(ChannelHandlerError::Cancelled),
                write_queue_result = queue.send(pending_buffer) => {
                    match(write_queue_result) {
                        Ok(_) => {
                            let Some(result) = cancellation_token.run_until_cancelled(writer.send(DataChannelResponse::AckData(sequence))).await else {
                                return Err(ChannelHandlerError::Cancelled);
                            };
                            result.map_err(|e| ChannelHandlerError::Network(e.into()))?
                        },
                        Err(_) => {
                            let Some(result) = cancellation_token.run_until_cancelled(writer.send(DataChannelResponse::Close)).await else {
                                return Err(ChannelHandlerError::Cancelled);
                            };
                            return result.map_err(|e| ChannelHandlerError::Network(e.into()));
                        }
                    }
                },
            }
        }

        select! {
            _ = cancellation_token.cancelled() => return Err(ChannelHandlerError::Cancelled),
            request = reader.next() => pending_buffer = {
                match request.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::Network(e.into()))? {
                    DataChannelRequest::Data(buffer) => Some(buffer),
                    DataChannelRequest::Close => {
                        queue.close();
                        return Err(ChannelHandlerError::ClosedByOtherSide)
                    },
                }
            }
        }
    }
}

async fn create_channel_handler(
    channel_id: ChannelIdentifier,
    mut queue: DataQueue,
    channel_cancellation_token: CancellationToken,
    control: NetworkingServiceController,
) -> Result<u16> {
    let (tx, rx) = oneshot::channel::<std::result::Result<u16, Error>>();
    tokio::spawn({
        let channel = channel_id.clone();
        async move {
            let listener = channel_cancellation_token
                .run_until_cancelled(TcpListener::bind("0.0.0.0:0"))
                .await;

            let mut listener = match listener {
                None => return,
                Some(Ok(listener)) => listener,
                Some(Err(e)) => {
                    tx.send(Err(e.into()))
                        .expect("BUG: Channel should not be closed.");
                    return;
                }
            };

            let port = match listener.local_addr() {
                Ok(addr) => addr.port(),
                Err(e) => {
                    tx.send(Err(e.into()))
                        .expect("BUG: Channel should not be closed.");
                    return;
                }
            };
            tx.send(Ok(port))
                .expect("BUG: Channel should not be closed.");

            Span::current().record("port", format!("{}", port));

            let Err(channel_handler_error) = channel_handler(
                channel_cancellation_token.clone(),
                &mut queue,
                &mut listener,
            )
            .await
            else {
                return;
            };

            match channel_handler_error {
                ChannelHandlerError::Cancelled => {
                    return;
                }
                ChannelHandlerError::Network(e) => {
                    warn!("Data Channel Stopped due to network error: {e}");
                }
                ChannelHandlerError::Timeout => {
                    warn!("Data Channel Stopped due to connection timeout");
                }
                ChannelHandlerError::ClosedByOtherSide => {}
            }
            // Reopen the channel
            control
                .send(NetworkingServiceControl::RetryChannel(
                    channel,
                    queue,
                    channel_cancellation_token,
                ))
                .await
                .expect("ReceiverServer should not have closed, while a channel is active");
        }
        .instrument(info_span!("channel", channel_id = %channel_id))
    });

    rx.await?
        .map_err(|_| "Could not create TupleBuffer channel listener".into())
}
async fn control_socket_handler(
    stream: TcpStream,
    channels: RegisteredChannels,
    control: NetworkingServiceController,
) -> Result<ControlChannelRequest> {
    let (mut reader, mut writer) = protocol::control_channel_receiver(stream);
    loop {
        let message = reader.next().await.ok_or("Connection Closed")?;
        match message? {
            ControlChannelRequest::ChannelRequest(channel) => {
                let Some((emit, token)) = channels.write().await.remove(&channel) else {
                    writer
                        .send(ControlChannelResponse::DenyChannelResponse)
                        .await?;
                    continue;
                };

                let port = create_channel_handler(channel, emit, token, control.clone()).await?;
                writer
                    .send(ControlChannelResponse::OkChannelResponse(port))
                    .await?;
            }
        }
    }
}

async fn control_socket(
    mut listener: NetworkingServiceControlListener,
    controller: NetworkingServiceController,
    connection_identifier: ConnectionIdentifier,
) -> Result<()> {
    use tokio::net::*;
    info!("Starting control socket: {}", connection_identifier);
    let listener_port = TcpListener::bind(connection_identifier.parse::<SocketAddr>()?).await?;
    let registered_channels = Arc::new(tokio::sync::RwLock::new(HashMap::default()));

    info!(
        "Control bound to {}",
        listener_port
            .local_addr()
            .expect("Local address is not accessible")
    );

    loop {
        tokio::select! {
            connect = listener_port.accept() => {
                let (stream, addr) = connect?;
                tokio::spawn(
                    {
                        let channels = registered_channels.clone();
                        let controller = controller.clone();
                        async move {
                            info!("Starting Connection Handler");
                            info!("Connection Handler terminated: {:?}", control_socket_handler(stream, channels, controller).await);
                        }.instrument(info_span!("connection", addr = %addr))
                    });
            },
            control_message = listener.recv() => {
               let message  = control_message.ok_or("Socket Closed")?;
               match message{
                    NetworkingServiceControl::Stop => {
                        registered_channels.write().await.iter().for_each(|(_, (_, token))|{
                           token.cancel();
                        });
                        return Ok(())
                    },
                    NetworkingServiceControl::RegisterChannel(ident,emit_fn, response) => {
                        let token = CancellationToken::new();
                        {
                            let mut locked = registered_channels.write().await;
                            locked.retain(|_, (_, token)| !token.is_cancelled());
                            locked.insert(ident, (emit_fn, token.clone()));
                        }
                        match response.send(token.clone()) {
                            Ok(_) => {},
                            Err(_) => {token.cancel();}
                        }
                    }
                    NetworkingServiceControl::RetryChannel(ident, emit_fn, token) => {registered_channels.write().await.insert(ident, (emit_fn, token));}
                };
            }
        }
    }
}
impl NetworkService {
    pub fn start(
        runtime: Runtime,
        connection_identifier: ConnectionIdentifier,
    ) -> Arc<NetworkService> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let service = Arc::new(NetworkService {
            sender: tx.clone(),
            runtime: Mutex::new(Some(runtime)),
        });

        service
            .runtime
            .lock()
            .expect("BUG: No one should panic while holding this lock")
            .as_ref()
            .expect("BUG: The service was just started")
            .spawn({
                let listener = rx;
                let controller = tx;
                async move {
                    info!("Starting Control");
                    let control_socket_result =
                        control_socket(listener, controller, connection_identifier).await;
                    match control_socket_result {
                        Ok(_) => {
                            info!("Control stopped")
                        }
                        Err(e) => {
                            error!("Control stopped: {:?}", e);
                        }
                    }
                }
            });

        service
    }

    pub fn register_channel(
        self: &Arc<NetworkService>,
        channel: ChannelIdentifier,
    ) -> Result<(async_channel::Receiver<TupleBuffer>, CancellationToken)> {
        let (data_queue_sender, data_queue_receiver) = async_channel::bounded(10);
        let (tx, rx) = oneshot::channel();
        self.runtime
            .lock()
            .expect("BUG: No one should panic while holding this lock")
            .as_ref()
            .ok_or("Networking Service was stopped")?
            .block_on(async move {
                if let Err(e) = self
                    .sender
                    .send(NetworkingServiceControl::RegisterChannel(
                        channel,
                        data_queue_sender,
                        tx,
                    ))
                    .await
                {
                    return Err("Networking Service was stopped".into());
                }

                match rx.await {
                    Ok(cancellation_token) => Ok((data_queue_receiver, cancellation_token)),
                    Err(_) => Err("Networking Service was stopped".into()),
                }
            })
    }

    pub fn shutdown(self: Arc<NetworkService>) -> Result<()> {
        let runtime = self
            .runtime
            .lock()
            .expect("BUG: No one should panic while holding this lock")
            .take()
            .ok_or("Networking Service was stopped")?;
        runtime.block_on(self.sender.send(NetworkingServiceControl::Stop))?;
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}
