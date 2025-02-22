use crate::protocol::*;
use crate::sender::DataQueue;
use async_channel::SendError;
use bytes::BytesMut;
use std::collections::HashMap;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{info, info_span, span, warn, Instrument, Span};
use tracing_subscriber::fmt::format;

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
type NetworkingServiceController = tokio::sync::mpsc::Sender<NetworkingServiceControl>;
type NetworkingServiceControlListener = tokio::sync::mpsc::Receiver<NetworkingServiceControl>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type EmitFn = Box<dyn FnMut(TupleBuffer) -> bool + Send + Sync>;

type RegisteredChannels = Arc<RwLock<HashMap<ChannelIdentifier, (DataQueue, CancellationToken)>>>;
async fn channel_handler(
    cancellation_token: CancellationToken,
    queue: &mut DataQueue,
    listener: &mut TcpListener,
) -> Result<()> {
    let Some(Ok(Ok((mut stream, address)))) = cancellation_token
        .run_until_cancelled(tokio::time::timeout(
            Duration::from_secs(3),
            listener.accept(),
        ))
        .await
    else {
        return Err("could not accept".into());
    };

    info!("Accepted TupleBuffer channel connection from {address}");
    loop {
        let buf = match cancellation_token
            .run_until_cancelled(TupleBuffer::deserialize(&mut stream))
            .await
        {
            Some(Ok(buf)) => buf,
            None => {
                return Err("Cancelled".into());
            }
            Some(Err(e)) => {
                return Err(e.into());
            }
        };
        let sequence = buf.sequence_number;
        let response = match cancellation_token
            .run_until_cancelled(queue.send(buf))
            .await
        {
            None => {
                return Err("Cancelled".into());
            }
            Some(Ok(())) => {
                format!("OK {}\n", sequence)
            }
            Some(Err(e)) => {
                format!("NOT OK {}\n", sequence)
            }
        };

        match cancellation_token
            .run_until_cancelled(stream.write_all(response.as_bytes()))
            .await
        {
            None => {
                return Err("Cancelled".into());
            }
            Some(Err(e)) => {
                return Err(e.into());
            }
            _ => {}
        }
    }
}
async fn create_channel_handler(
    channel_id: ChannelIdentifier,
    mut queue: DataQueue,
    channel_cancellation_token: CancellationToken,
    control: NetworkingServiceController,
) -> Result<u16> {
    let (tx, rx) = oneshot::channel::<std::result::Result<u16, ()>>();
    tokio::spawn({
        let channel = channel_id.clone();
        async move {
            let listener = channel_cancellation_token
                .run_until_cancelled(TcpListener::bind("0.0.0.0:0"))
                .await;
            let Some(Ok(mut listener)) = listener else {
                tx.send(Err(())).unwrap();
                return;
            };
            let port = listener.local_addr().unwrap().port();
            tx.send(Ok(port)).unwrap();
            Span::current().record("port", format!("{}", port));

            warn!(
                "Channel Handler terminated: {:?}",
                channel_handler(
                    channel_cancellation_token.clone(),
                    &mut queue,
                    &mut listener
                )
                .await
            );
            if channel_cancellation_token.is_cancelled() {
                return;
            }

            info!("Reopening channel");
            control
                .send(NetworkingServiceControl::RetryChannel(
                    channel,
                    queue,
                    channel_cancellation_token,
                ))
                .await
                .unwrap();
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
) -> Result<()> {
    let mut buf = BytesMut::with_capacity(1024);
    loop {
        buf.clear();
        let bytes_read = stream.read_buf(&mut buf).await?;
        if bytes_read == 0 {
            info!("Control Socket Handler is terminated");
            return Ok(());
        }
        let channel_id = from_utf8(&buf[0..bytes_read])?;
        assert!(channel_id.ends_with('\n'));
        let channel_id = channel_id.strip_suffix('\n').unwrap();

        let Some((emit, token)) = channels.write().await.remove(channel_id) else {
            stream.write_all("NOT OK\n".to_string().as_bytes()).await?;
            continue;
        };

        if token.is_cancelled() {
            stream.write_all("NOT OK\n".to_string().as_bytes()).await?;
            continue;
        }

        let port = create_channel_handler(channel_id.into(), emit, token, control.clone()).await?;
        stream.write_all(format!("OK {port}\n").as_bytes()).await?;
    }
    Ok(())
}
async fn control_socket(
    mut listener: NetworkingServiceControlListener,
    controller: NetworkingServiceController,
    port: u16,
) -> Result<()> {
    use tokio::net::*;
    let listener_port = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    let registered_channels = Arc::new(tokio::sync::RwLock::new(HashMap::default()));

    info!("Control bound to {}", listener_port.local_addr().unwrap());

    loop {
        tokio::select! {
            connect = listener_port.accept() => {
                let (stream, addr) = connect?;
                let handle = tokio::spawn(
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
                    NetworkingServiceControl::Stop => return Ok(()),
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
    Ok(())
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
