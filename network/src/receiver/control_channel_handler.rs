use crate::protocol;
use crate::protocol::ChannelIdentifier;
use crate::receiver::data_channel_handler::ChannelHandlerError;
use crate::receiver::data_channel_handler::data_channel_handler;
use crate::receiver::network_service::{
    DataQueue, Error, NetworkServiceControlMessage, NetworkServiceController, Result,
};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::{RwLock, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, info, info_span, warn};

type RegisteredChannels = Arc<RwLock<HashMap<ChannelIdentifier, (DataQueue, CancellationToken)>>>;
type TcpPort = u16;

async fn create_channel_handler(
    channel_id: ChannelIdentifier,
    mut queue: DataQueue,
    channel_cancellation_token: CancellationToken,
    controller: NetworkServiceController, // necessary to inject retry channel requests
) -> Result<TcpPort> {
    // This oneshot channel allows us to await the result of data channel establishment within
    // the current chain of async execution
    let (tx, rx) = oneshot::channel::<std::result::Result<TcpPort, Error>>();
    // Control forks, as we spawn a new thread of async execution with a future handling the
    // establishment of the data channel
    tokio::spawn({
        let channel = channel_id.clone();
        async move {
            // Create a TCP listener and bind to a port assigned by the os
            // Propagate failure to oneshot channel and return
            let mut listener = select! {
                _ = channel_cancellation_token.cancelled() => return,
                listener = TcpListener::bind("0.0.0.0:0") => {
                    match listener {
                        Ok(listener) => listener,
                        Err(e) => {
                            tx.send(Err(e.into())).expect("BUG: Channel should not be closed.");
                            return;
                        }
                    }
                }
            };

            // Obtain the assigned port, propagate failure to oneshot channel and return
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

            let Err(channel_handler_error) = data_channel_handler(
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
                    info!("Data channel stopped");
                    return;
                }
                ChannelHandlerError::Network(e) => {
                    warn!("Data channel stopped due to network error: {e}");
                }
                ChannelHandlerError::Timeout => {
                    warn!("Data channel stopped due to connection timeout");
                }
                ChannelHandlerError::ClosedByOtherSide => {
                    info!("Data channel stopped due to connection close by the upstream host");
                    queue.close();
                }
            }
            info!("Try reopening data channel");
            controller
                .send(NetworkServiceControlMessage::RetryChannel(
                    channel,
                    queue,
                    channel_cancellation_token,
                ))
                .await
                .expect("ControlListener should not have closed while a channel is active");
        }
        .instrument(info_span!("data_channel", channel_id = %channel_id))
    });

    // Pause here and await the creation of the channel listener inside the spawned future
    rx.await?
        .map_err(|_| "Could not create TupleBuffer channel listener".into())
}

pub(crate) async fn control_channel_handler(
    stream: TcpStream,
    registered_channels: RegisteredChannels,
    control: NetworkServiceController,
) -> Result<protocol::ControlChannelRequest> {
    let (mut reader, mut writer) = protocol::control_channel_receiver(stream);
    let mut active_channels = vec![];
    loop {
        // First await receiving a message that is a channel request from upstream via the established TCP control channel
        // We do not initiate communication since control flows upstream --> downstream
        let Some(Ok(request)) = reader.next().await else {
            // Receiving none or an error means that the connection has closed
            warn!(
                "Connection was closed. Cancelling {} channel(s)",
                active_channels.len()
            );
            active_channels
                .into_iter()
                .for_each(|token: CancellationToken| token.cancel());
            return Err("Connection closed".into());
        };

        // We have received a valid channel request from upstream
        match request {
            protocol::ControlChannelRequest::ChannelRequest(channel) => {
                // Remove channels that have been cancelled
                active_channels.retain(|token: &CancellationToken| !token.is_cancelled());
                // Move the data queue and the cancel token out of the registered_channels if one exists
                // If not, send a deny channel response since we do not know about the channel yet
                let Some((data_queue, token)) = registered_channels.write().await.remove(&channel)
                else {
                    writer
                        .send(protocol::ControlChannelResponse::DenyChannelResponse)
                        .await?;
                    continue;
                };

                let port =
                    create_channel_handler(channel, data_queue, token.clone(), control.clone())
                        .await?;
                writer
                    .send(protocol::ControlChannelResponse::OkChannelResponse(port))
                    .await?;
                // Push the newly created data channel to active_channels via its cancellation token
                // We do not require more state associated with the channel as closing propagates to us
                // from the upstream node and we only need to cancel as a reaction to a closed connection
                active_channels.push(token);
            }
        }
    }
}
