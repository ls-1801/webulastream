use crate::protocol::{ChannelIdentifier, ConnectionIdentifier, TupleBuffer};
use crate::receiver::listener::network_receiver_listener;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type EmitFn = Box<dyn FnMut(TupleBuffer) -> bool + Send + Sync>;

pub(crate) type DataQueue = async_channel::Sender<TupleBuffer>;

pub(crate) enum NetworkServiceControlMessage {
    RetryChannel(ChannelIdentifier, DataQueue, CancellationToken),
    RegisterChannel(ChannelIdentifier, DataQueue, oneshot::Sender<()>),
}
pub(crate) type NetworkServiceController = async_channel::Sender<NetworkServiceControlMessage>;
pub(crate) type NetworkServiceControlListener =
    async_channel::Receiver<NetworkServiceControlMessage>;

// A network service that acts as the receiver for upstream connections
// Can be used to transmit intermediate query results to this node for further processing
pub struct ReceiverNetworkService {
    // Exposed to allow the service to control futures that e.g., run the control socket
    controller: NetworkServiceController,
    runtime: Mutex<Option<Runtime>>,
}

impl ReceiverNetworkService {
    // Start the receiver network service and return a reference to it
    pub fn start(
        runtime: Runtime,
        connection_id: ConnectionIdentifier,
    ) -> Arc<ReceiverNetworkService> {
        let (tx, rx) = async_channel::bounded(10);
        let service = Arc::new(ReceiverNetworkService {
            controller: tx.clone(),
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
                    info!("Starting control channel");
                    match network_receiver_listener(listener, controller, connection_id).await {
                        Ok(_) => {
                            info!("Control channel stopped")
                        }
                        Err(e) => {
                            error!("Control channel stopped: {:?}", e);
                        }
                    }
                }
            });
        service
    }

    // Register a data channel by requesting it via the controller
    // Return the receiving end of an async_channel that can be used to read the transmitted data
    pub fn register_channel(
        self: &Arc<ReceiverNetworkService>,
        channel: ChannelIdentifier,
    ) -> Result<async_channel::Receiver<TupleBuffer>> {
        let (data_queue_sender, data_queue_receiver) = async_channel::bounded(10);
        let (tx, rx) = oneshot::channel();
        let Ok(_) = self
            .controller
            .send_blocking(NetworkServiceControlMessage::RegisterChannel(
                channel,
                data_queue_sender,
                tx,
            ))
        else {
            return Err("ReceiverNetworkService was stopped".into());
        };
        rx.blocking_recv()
            .map_err(|_| "ReceiverNetworkService was stopped")?;
        Ok(data_queue_receiver)
    }

    pub fn shutdown(self: Arc<ReceiverNetworkService>) -> Result<()> {
        self.controller.close();
        let runtime = self
            .runtime
            .lock()
            .expect("BUG: No one should panic while holding this lock")
            .take()
            .ok_or("ReceiverNetworkService was stopped")?;
        runtime.shutdown_timeout(Duration::from_secs(1));
        Ok(())
    }
}
