use crate::protocol::{ChannelIdentifier, ConnectionIdentifier};
use crate::sender::data_channel_handler::ChannelControlQueue;
use crate::sender::dispatcher::network_sender_dispatcher;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub(crate) type Result<T> = std::result::Result<T, Error>;
pub(crate) type Error = Box<dyn std::error::Error + Send + Sync>;

// Instruct the network service to create a new network channel
// The first message will lead to establishment of a control connection to the remote host
// identified by the ConnectionIdentifier
pub(crate) enum NetworkServiceControlMessage {
    RegisterChannel(
        ConnectionIdentifier,
        ChannelIdentifier,
        oneshot::Sender<ChannelControlQueue>,
    ),
}

// Controller will send RegisterChannel requests, ControlListener will rcv and react on them
type Controller = async_channel::Sender<NetworkServiceControlMessage>;
pub(crate) type ControlListener = async_channel::Receiver<NetworkServiceControlMessage>;

// Handle networking for the given node as the "sender" to downstream nodes
pub struct SenderNetworkService {
    runtime: Mutex<Option<Runtime>>,
    // Sender end of the async_channel that can be used to inject RegisterChannel requests
    controller: Controller,
    // Can be used to request cancellation of the entire NetworkService, which will lead to 
    // cancellation of all child futures (data channels)
    cancellation_token: CancellationToken,
}

impl SenderNetworkService {
    pub fn start(runtime: Runtime) -> Arc<SenderNetworkService> {
        // create an async channel, returns the sender and receiver side for further use
        let (controller, listener) = async_channel::bounded(5);
        let service_cancellation_token = CancellationToken::new();
        runtime.spawn({
            let token = service_cancellation_token.clone();
            // this block evaluates to the root future of the sender network service
            // it awaits the result of the sender dispatcher
            async move {
                info!("Starting SenderNetworkService");
                info!(
                    "SenderNetworkService stopped: {:?}",
                    network_sender_dispatcher(token, listener).await
                );
            }
        });

        Arc::new(SenderNetworkService {
            runtime: Mutex::new(Some(runtime)),
            cancellation_token: service_cancellation_token,
            controller,
        })
    }

    // Called when a component wants to send data via a channel to a remote host.
    // For example, an operator wants to send its intermediate results to another node
    pub fn register_channel(
        self: &Arc<SenderNetworkService>,
        connection: ConnectionIdentifier,
        channel: ChannelIdentifier,
    ) -> Result<ChannelControlQueue> {
        let (tx, rx) = oneshot::channel();
        let Ok(_) = self
            .controller
            .send_blocking(NetworkServiceControlMessage::RegisterChannel(connection, channel, tx))
        else {
            return Err("Network Service Closed".into());
        };

        rx.blocking_recv()
            .map_err(|_| "Network Service Closed".into())
    }

    pub fn shutdown(self: Arc<SenderNetworkService>) -> Result<()> {
        let runtime = self
            .runtime
            .lock()
            .expect("BUG: Nothing should panic while holding the lock")
            .take()
            .ok_or("Networking Service was stopped")?;
        self.cancellation_token.cancel();
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}

impl SenderNetworkService {}
