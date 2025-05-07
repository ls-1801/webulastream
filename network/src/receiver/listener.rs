use crate::protocol::ConnectionIdentifier;
use crate::receiver::control_channel_handler::control_channel_handler;
use crate::receiver::network_service::{
    NetworkServiceControlListener, NetworkServiceControlMessage, NetworkServiceController,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span};

pub(crate) async fn network_receiver_listener(
    listener: NetworkServiceControlListener,
    controller: NetworkServiceController,
    connection_identifier: ConnectionIdentifier,
) -> crate::receiver::network_service::Result<()> {
    info!("Starting control channel: {}", connection_identifier);
    // We do a passive open here, because we as a receiver act as a server to the upstream client
    // After binding to the specified port, we listen for incoming connections from upstream hosts
    let listener_port = TcpListener::bind(connection_identifier.parse::<SocketAddr>()?).await?;
    // State of the listener: mapping channel_id's to a sender side of a queue and a cancellation token
    // A registered channel does not mean that a corresponding physical data channel in form of
    // a TCP connection already exists, just that an interest in creating such a channel was registered.
    let registered_channels = Arc::new(RwLock::new(HashMap::default()));

    info!(
        "Control channel bound to {}",
        listener_port
            .local_addr()
            .expect("Local address is not accessible")
    );

    loop {
        select! {
            connect = listener_port.accept() => {
                let (stream, addr) = connect?;
                tokio::spawn(
                    {
                        let channels = registered_channels.clone();
                        let controller = controller.clone();
                        async move {
                            info!("Starting control channel handler");
                            info!("Control channel handler terminated: {:?}", control_channel_handler(stream, channels, controller).await);
                        }.instrument(info_span!("receiver::control_channel", addr = %addr))
                    });
            },
            control_message = listener.recv() => match control_message {
                Err(_) => { // Channel closed and no more messages left
                    registered_channels.write().await.iter().for_each(|(_, (_, token))|{
                       token.cancel();
                    });
                    return Ok(())
                },
                Ok(NetworkServiceControlMessage::RegisterChannel(channel_id, emit_fn, response)) => {
                    let token = CancellationToken::new();
                    {
                        let mut locked = registered_channels.write().await;
                        locked.retain(|_, (_, token)| !token.is_cancelled());
                        locked.insert(channel_id, (emit_fn, token.clone()));
                    }

                    match response.send(()) {
                        Ok(_) => {},
                        Err(_) => {token.cancel();} // Receiver does not exist anymore, mark request as cancelled
                    }
                }
                // The existing channel will be updated with the new emit_fn and cancellation token provided by the message
                Ok(NetworkServiceControlMessage::RetryChannel(channel_id, emit_fn, token)) => {registered_channels.write().await.insert(channel_id, (emit_fn, token));}
            }
        }
    }
}
