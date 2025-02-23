use std::collections::HashMap;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::protocol::*;
use crate::{protocol, sender};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::stream;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{info, info_span, trace, warn, Instrument, Span};

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

    info!("Accepted TupleBuffer channel connection from {address}");
    loop {
        let buf: TupleBuffer = match cancellation_token
            .run_until_cancelled(read_message(&mut stream))
            .await
        {
            None => return Err(ChannelHandlerError::Cancelled),
            Some(Ok(buf)) => buf,
            Some(Err(e)) => {
                return Err(ChannelHandlerError::Network(e.into()));
            }
        };

        let sequence = buf.sequence_number;
        let response = match cancellation_token
            .run_until_cancelled(queue.send(buf))
            .await
        {
            None => return Err(ChannelHandlerError::Cancelled),
            Some(Ok(())) => DataResponse::AckData(sequence),
            // closing the other side of the data queue terminates the connection
            Some(Err(_)) => DataResponse::Closed,
        };

        match cancellation_token
            .run_until_cancelled(send_message(&mut stream, &response))
            .await
        {
            None => return Err(ChannelHandlerError::Cancelled),
            Some(Err(e)) => return Err(ChannelHandlerError::Network(e.into())),
            _ => {}
        }

        if matches!(response, DataResponse::Closed) {
            info!("Closed Data Channel");
            return Ok(());
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
    mut stream: TcpStream,
    channels: RegisteredChannels,
    control: NetworkingServiceController,
) -> Result<ControlChannelRequests> {
    loop {
        let message: ControlChannelRequests = read_message(&mut stream).await?;
        match message {
            ControlChannelRequests::ChannelRequest(channel) => {
                let Some((emit, token)) = channels.write().await.remove(&channel) else {
                    send_message(&mut stream, &ChannelResponse::DenyChannelResponse).await?;
                    continue;
                };

                let port = create_channel_handler(channel, emit, token, control.clone()).await?;
                send_message(&mut stream, &ChannelResponse::OkChannelResponse(port)).await?;
            }
        }
    }
}

async fn control_socket(
    mut listener: NetworkingServiceControlListener,
    controller: NetworkingServiceController,
    port: u16,
) -> Result<()> {
    use tokio::net::*;
    let listener_port = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    let registered_channels = Arc::new(tokio::sync::RwLock::new(HashMap::default()));

    info!("Control bound to {}", listener_port.local_addr().expect("Local address is not accessible"));

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
                        response.send(token).unwrap();
                    }
                    NetworkingServiceControl::RetryChannel(ident, emit_fn, token) => {registered_channels.write().await.insert(ident, (emit_fn, token));}
                };
            }
        }
    }
}
impl NetworkService {
    pub fn start(runtime: Runtime, port: u16) -> Arc<NetworkService> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let service = Arc::new(NetworkService {
            sender: tx.clone(),
            runtime: Mutex::new(Some(runtime)),
        });

        service.runtime.lock().unwrap().as_ref().unwrap().spawn({
            let listener = rx;
            let controller = tx;
            async move {
                info!("Starting Control");
                let control_socket_result = control_socket(listener, controller, port).await;
                info!("Control stopped: {:?}", control_socket_result)
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
            .unwrap()
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
                    return Err(e);
                }
                Ok((data_queue_receiver, rx.await.unwrap()))
            })
            .map_err(|e| format!("Could not register channel: {e}").as_str().into())
    }

    pub fn shutdown(self: Arc<NetworkService>) -> Result<()> {
        let runtime = self
            .runtime
            .lock()
            .unwrap()
            .take()
            .ok_or("Networking Service was stopped")?;
        runtime.block_on(self.sender.send(NetworkingServiceControl::Stop))?;
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}
