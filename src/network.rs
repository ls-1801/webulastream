use crate::engine::Data;
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
use tracing::{info, warn};
use tracing_subscriber::fmt::format;

pub(crate) struct NetworkService {
    sender: NetworkingServiceController,
    runtime: Mutex<Option<Runtime>>,
}

type ChannelIdentifier = String;
enum ProtocolControl {
    NewDataChannel(ChannelIdentifier),
}
enum NetworkingServiceControl {
    Stop,
    RegisterChannel(ChannelIdentifier, EmitFn),
}
type NetworkingServiceController = tokio::sync::mpsc::Sender<NetworkingServiceControl>;
type NetworkingServiceControlListener = tokio::sync::mpsc::Receiver<NetworkingServiceControl>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error>;
pub type EmitFn = Box<dyn Fn(Data) -> bool + Send + Sync>;

type RegisteredChannels = Arc<RwLock<HashMap<ChannelIdentifier, EmitFn>>>;
async fn channel_handler(emit: EmitFn, listener: TcpListener) -> Result<()> {
    'accept: loop {
        let (mut stream, address) = listener.accept().await?;
        info!("Accepted data channel connection from {address}");
        loop {
            let mut buf = BytesMut::with_capacity(1024);
            let bytes_read = stream.read_buf(&mut buf).await?;

            if bytes_read == 0 {
                warn!("Data channel connection lost");
                continue 'accept;
            }

            if (emit(Data { bytes: buf })) {
                stream.write_all(format!("OK\n").as_bytes()).await?;
            } else {
                stream.write_all(format!("NOT OK\n").as_bytes()).await?;
            }
        }
    }
}
async fn create_channel_handler(emit: EmitFn) -> Result<u16> {
    let (tx, rx) = oneshot::channel::<std::result::Result<u16, ()>>();
    tokio::spawn(async move {
        let listener = TcpListener::bind("0.0.0.0:0").await;
        let Ok(listener) = listener else {
            tx.send(Err(())).unwrap();
            return;
        };
        let port = listener.local_addr().unwrap().port();
        tx.send(Ok(port)).unwrap();
        info!(
            "Channel Handler terminated: {:?}",
            channel_handler(emit, listener).await
        );
    });

    rx.await?
        .map_err(|_| "Could not create data channel listener".into())
}
async fn control_socket_handler(mut stream: TcpStream, channels: RegisteredChannels) -> Result<()> {
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

        if let Some(emit) = channels.write().await.remove(channel_id) {
            let port = create_channel_handler(emit).await?;
            stream.write_all(format!("OK {port}\n").as_bytes()).await?;
        } else {
            stream.write_all(format!("NOT OK\n").as_bytes()).await?;
        }
    }
    Ok(())
}
async fn control_socket(mut control: NetworkingServiceControlListener) -> Result<()> {
    use tokio::net::*;
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    let registered_channels = Arc::new(tokio::sync::RwLock::new(HashMap::default()));

    info!("Control bound to {}", listener.local_addr().unwrap());

    loop {
        tokio::select! {
            connect = listener.accept() => {
                let (stream, addr) = connect?;
                tokio::spawn(
                    {
                        let channels = registered_channels.clone();
                        async move {
                            info!("Control Socket Handler: {:?}", control_socket_handler(stream, channels).await);
                        }
                    });
            },
            control_message = control.recv() => {
               let message  = control_message.ok_or("Socket Closed")?;
               match message{
                    NetworkingServiceControl::Stop => return Ok(()),
                    NetworkingServiceControl::RegisterChannel(ident, emitFn) => registered_channels.write().await.insert(ident, emitFn)
                };
            }
        }
    }
    Ok(())
}
impl NetworkService {
    pub(crate) fn start(runtime: Runtime) -> Arc<NetworkService> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let service = Arc::new(NetworkService {
            sender: tx,
            runtime: Mutex::new(Some(runtime)),
        });

        service
            .runtime
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .spawn(async move {
                info!("Starting Control");
                info!("Control stopped: {:?}", control_socket(rx).await)
            });

        service
    }

    pub fn register_channel(
        self: &Arc<NetworkService>,
        channel: ChannelIdentifier,
        emit: EmitFn,
    ) -> Result<()> {
        self.runtime
            .lock()
            .unwrap()
            .as_ref()
            .ok_or("Networking Service was stopped")?
            .block_on(
                self.sender
                    .send(NetworkingServiceControl::RegisterChannel(channel, emit)),
            )
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

#[test]
fn basic_usage() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()
        .unwrap();
    let service = NetworkService::start(rt);

    sleep(Duration::from_secs(100));
    service.shutdown().unwrap();
}
