use crate::protocol::{ChannelIdentifier, ConnectionIdentifier};
use crate::sender::cancellation::{Cancellable, cancel_all};
use crate::sender::control_connection_handler::connection_handler;
use crate::sender::data_channel_handler::{ChannelControlQueue, ChannelControlQueueListener};
use crate::sender::network_service;
use crate::sender::network_service::Result;
use std::collections::HashMap;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span};

// set of messages used by the sender network service to establish
pub(crate) enum ConnectionControlMessage {
    RegisterChannel(ChannelIdentifier, oneshot::Sender<ChannelControlQueue>),
    RetryChannel(
        ChannelIdentifier,
        CancellationToken,
        ChannelControlQueueListener,
    ),
}

// async channel to exchange requests to create
pub(crate) type ConnectionController = async_channel::Sender<ConnectionControlMessage>;
pub(crate) type ConnectionControlListener = async_channel::Receiver<ConnectionControlMessage>;

impl Cancellable for (CancellationToken, ConnectionController) {
    fn cancellation_token(&self) -> &CancellationToken {
        &self.0
    }
}

// Maintains active control connections between hosts
// Dispatches connection requests to the control connection handler
pub(crate) async fn network_sender_dispatcher(
    cancellation_token: CancellationToken,
    control_listener: network_service::ControlListener,
) -> Result<()> {
    // Maps a connection identifier (like 127.0.0.1:8080) to the corresponding pair of cancellation token and controller
    let mut connections: HashMap<ConnectionIdentifier, (CancellationToken, ConnectionController)> =
        HashMap::default();
    // Loop endlessly until canceled, listening for incoming register channel requests
    loop {
        match cancellation_token
            .run_until_cancelled(control_listener.recv())
            .await
        {
            None => return cancel_all(connections),
            Some(Err(_)) => return Err("Queue was closed".into()),
            Some(Ok(network_service::ControlMessage::RegisterChannel(
                connection_id,
                channel_id,
                tx,
            ))) => {
                info!("Received RegisterChannelRequest {:?}", channel_id);
                // Create a new connection if it does not exist yet
                if !connections.contains_key(&connection_id) {
                    match cancellation_token
                        .run_until_cancelled(create_connection(&connection_id))
                        .await
                    {
                        // When control resumes here, we have a new connection, a failure, or cancellation occurred
                        None => return cancel_all(connections),
                        Some(Ok((token, controller))) => {
                            // Result is a connection controller with an associated cancellation token
                            // We can use the controller to forward the register channel request
                            // to the newly established connection
                            connections.insert(connection_id.clone(), (token, controller));
                        }
                        Some(Err(e)) => {
                            return Err(e);
                        }
                    }
                }

                // Dispatch the request via the async channel (NOTE: this is an internal channel!)
                // In the absence of cancellation or failure, we loop back around, waiting to
                // dispatch the next request
                match cancellation_token
                    .run_until_cancelled(
                        connections
                            .get(&connection_id)
                            .expect("BUG: Just inserted the key")
                            .1
                            .send(ConnectionControlMessage::RegisterChannel(channel_id, tx)),
                    )
                    .await
                {
                    None => return cancel_all(connections),
                    Some(Err(e)) => {
                        return Err(e)?;
                    }
                    Some(Ok(())) => {}
                }
            }
        }
    }
}

// responsible for establishing a control connection to a remote node
// receives a host:port pair and returns a connection controller with an associated cancellation token
// the connection controller is a sender handle to
async fn create_connection(
    connection_id: &ConnectionIdentifier,
) -> Result<(CancellationToken, ConnectionController)> {
    let cancellation_token = CancellationToken::new();
    // Create a new async channel to exchange to allow message exchange between async tasks
    // The controller will send RegisterChannel messages, the listener will receive and handle them
    let (controller, listener) = async_channel::bounded::<ConnectionControlMessage>(1024);
    let controller_clone = controller.clone();
    // Spawn a new task, associated with a new cancellation token to handle the connection
    // to the remote host identified by connection_id.
    // Control flow is forked, with the new connection being handled concurrently to the dispatcher
    // receiving more requests
    tokio::spawn(
        {
            // Forward token and pass it back to handle both ends of cancellation
            let connection_cancellation_token = cancellation_token.clone();
            let connection = connection_id.clone();
            async move {
                info!(
                    "Connection is terminated: {:?}",
                    connection_handler(
                        connection,
                        connection_cancellation_token,
                        controller_clone,
                        listener
                    )
                    .await
                );
            }
        }
        .instrument(info_span!(
            "sender::connection_handler",
            connection = connection_id.clone()
        )),
    );
    // The controller is passed back to the caller (network_sender_dispatcher) to be used to send
    // The dispatcher will forward the register channel request to the connection handler
    Ok((cancellation_token, controller))
}
