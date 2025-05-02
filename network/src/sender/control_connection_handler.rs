use crate::protocol::{
    ChannelIdentifier, ConnectionIdentifier, ControlChannelRequest, ControlChannelResponse,
    ControlChannelSenderReader, ControlChannelSenderWriter, control_channel_sender,
};
use crate::sender::cancellation::{Cancellable, cancel_all};
use crate::sender::data_channel_handler;
use crate::sender::data_channel_handler::ChannelHandlerResult;
use crate::sender::dispatcher::{
    ConnectionControlListener, ConnectionControlMessage, ConnectionController,
};
use crate::sender::network_service::{Error, Result};
use futures::SinkExt;
use futures::StreamExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpSocket, TcpStream};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span, warn};

enum EstablishChannelResult {
    Ok(CancellationToken),
    ChannelReject(
        ChannelIdentifier,
        data_channel_handler::ChannelControlQueueListener,
    ),
    BadConnection(
        ChannelIdentifier,
        data_channel_handler::ChannelControlQueueListener,
        Error,
    ),
    Cancelled,
}

impl Cancellable for CancellationToken {
    fn cancellation_token(&self) -> &CancellationToken {
        &self
    }
}

// Establishes a data channel with the receiver host over the control connection.
// Uses a request-response pattern, i.e., we do not return from this method until we have received 
// a response or an error/cancellation occurred. 
// Failed attempts will return an EstablishChannelResult that indicates the cause of the failure,
// leading to a retry or cancellation of the channel.
async fn establish_channel(
    control_channel_sender_writer: &mut ControlChannelSenderWriter,
    control_channel_sender_reader: &mut ControlChannelSenderReader,
    connection_identifier: &ConnectionIdentifier,
    channel: ChannelIdentifier,
    channel_cancellation_token: CancellationToken,
    queue: data_channel_handler::ChannelControlQueueListener,
    controller: ConnectionController,
) -> EstablishChannelResult {
    // send a data channel creation request, continue if success, return if canceled/error
    select! {
        _ = channel_cancellation_token.cancelled() => {
            return EstablishChannelResult::Cancelled;
        },
        result = control_channel_sender_writer.send(
            ControlChannelRequest::ChannelRequest(channel.clone())
        ) => {
            match result {
                Ok(()) => {}
                Err(e) => {
                    return EstablishChannelResult::BadConnection(
                        channel,
                        queue,
                        format!("Could not send channel creation request {}", e).into(),
                    );
                }
            }
        }
    }

    // Receive a data channel creation response, continue if success, return if canceled/error
    let port = select! {
        _ = channel_cancellation_token.cancelled() => return EstablishChannelResult::Cancelled,
        negotiated_port = control_channel_sender_reader.next() => match negotiated_port {
            Some(Ok(ControlChannelResponse::OkChannelResponse(port))) => port,
            Some(Ok(ControlChannelResponse::DenyChannelResponse)) => return EstablishChannelResult::ChannelReject(channel, queue),
            Some(Err(e)) => return EstablishChannelResult::BadConnection(channel, queue, e.into()),
            None => return EstablishChannelResult::BadConnection(channel, queue, "Connection closed".into()),
        }
    };
    info!("Creation of data channel accepted by remote node. Using port '{:?}'", port);

    // At this point, we know that the receiving side is ready to accept data via the channel
    let channel_address = connection_identifier.parse::<SocketAddr>()
        .expect("The Connection identifier should not be an invalid address, as the connection should have already been established");
    let channel_address = SocketAddr::new(channel_address.ip(), port);

    // spawn a task to handle the data channel
    tokio::spawn(
        {
            let channel = channel.clone();
            let channel_cancellation_token = channel_cancellation_token.clone();
            async move {
                match data_channel_handler::channel_handler(
                    channel_cancellation_token.clone(),
                    channel_address,
                    queue.clone(),
                )
                .await
                {
                    ChannelHandlerResult::Cancelled => {}
                    ChannelHandlerResult::ConnectionLost(e) => {
                        warn!("Connection Lost: {e:?}");
                        match &channel_cancellation_token
                            .run_until_cancelled(controller.send(
                                ConnectionControlMessage::RetryChannel(
                                    channel,
                                    channel_cancellation_token.clone(),
                                    queue.clone(),
                                ),
                            ))
                            .await
                        {
                            // TODO: what about none and err variants?
                            None => {}
                            Some(Ok(())) => {
                                info!("Channel connection lost. Reopening channel...");
                            }
                            Some(Err(_)) => {}
                        }
                    }
                    ChannelHandlerResult::Closed => {
                        info!("Channel closed");
                    }
                }
            }
        }
        .instrument(info_span!("channel_handler", channel = %channel, port = %port)),
    );
    EstablishChannelResult::Ok(channel_cancellation_token)
}

async fn accept_channel_requests(
    cancellation_token: CancellationToken,
    timeout: Duration,
    active_channels: &mut HashMap<ChannelIdentifier, CancellationToken>,
    listener: &mut ConnectionControlListener,
) -> Option<
    Vec<(
        ChannelIdentifier,
        CancellationToken,
        data_channel_handler::ChannelControlQueueListener,
    )>,
> {
    let mut pending = vec![];
    // Receive RegisterChannel or RetryChannel requests
    // loop until stopped, timeout occurs, or the queue is closed
    loop {
        select! {
            _ = cancellation_token.cancelled() => return None,
            _ = tokio::time::sleep(timeout) => break,
            recv = listener.recv() => match recv {
                Err(_) => break, // queue closed
                Ok(request) => match request {
                    // Recv register requests by the dispatcher
                    ConnectionControlMessage::RegisterChannel(channel_id, response) => {
                        info!("Received register channel: {channel_id:?}");
                        // Create a new async_channel
                        let (sender, queue) = async_channel::bounded(100);
                        // Create a new cancellation token to enable per-channel cancellation
                        let channel_cancellation_token = CancellationToken::new();
                        // NOTE: this just responds to the initial RegisterChannelRequest message,
                        // nothing further happens
                        if response.send(sender).is_err() {
                            warn!("Channel registration was canceled");
                            continue;
                        }
                        pending.push((channel_id, channel_cancellation_token, queue));
                    }
                    // Recv retry requests by this handler
                    ConnectionControlMessage::RetryChannel(
                        active_channel_id,
                        channel_cancellation,
                        queue,
                    ) => {
                        info!("Received retry channel: {active_channel_id:?}");
                        active_channels.remove(&active_channel_id);
                        pending.push((active_channel_id, channel_cancellation, queue));
                    }
                }
            }
        }
    }

    Some(pending)
}

async fn establish_control_connection(
    connection_id: &ConnectionIdentifier,
    cancellation_token: &CancellationToken,
) -> Option<TcpStream> {
    let mut retry = 0;
    loop {
        let socket = match TcpSocket::new_v4() {
            Ok(s) => s,
            Err(_) => return None,
        };
        let socket_addr = match connection_id.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(_) => return None,
        };

        select! {
            _ = cancellation_token.cancelled() => return None,
            tcp_stream = socket.connect(socket_addr) => {
                match tcp_stream {
                    Ok(stream) => {
                        info!("Successfully established TCP connection to {}", connection_id);
                        return Some(stream)
                    },
                    Err(e) => {
                        retry += 1;
                        warn!("Could not establish connection: {}. Retry in {}s", e, retry);
                        tokio::time::sleep(Duration::from_secs(retry)).await;
                    }
                }
            }
        }
    }
}

async fn handle_pending_channels(
    pending_channels: Vec<(
        ChannelIdentifier,
        CancellationToken,
        data_channel_handler::ChannelControlQueueListener,
    )>,
    active_channels: &mut HashMap<ChannelIdentifier, CancellationToken>,
    connection_id: &ConnectionIdentifier,
    connection_cancellation_token: CancellationToken,
    sender_writer: &mut ControlChannelSenderWriter,
    sender_reader: &mut ControlChannelSenderReader,
    connection_controller: ConnectionController,
) -> (
    Vec<(
        ChannelIdentifier,
        CancellationToken,
        data_channel_handler::ChannelControlQueueListener,
    )>,
    bool,
) {
    let mut updated_pending_channels = vec![];
    let mut connection_failed = false;

    for (channel_id, channel_cancel_token, queue) in pending_channels {
        if channel_cancel_token.is_cancelled() {
            continue;
        }
        if connection_failed {
            updated_pending_channels.push((channel_id, channel_cancel_token, queue));
            continue;
        }

        match connection_cancellation_token
            .run_until_cancelled(establish_channel(
                sender_writer,
                sender_reader,
                connection_id,
                channel_id.clone(),
                channel_cancel_token.clone(),
                queue,
                connection_controller.clone(),
            ))
            .await
        {
            None | Some(EstablishChannelResult::Cancelled) => return (vec![], true),
            Some(EstablishChannelResult::Ok(token)) => {
                info!("Successfully established channel: {channel_id:?}");
                active_channels.insert(channel_id, token);
            }
            Some(EstablishChannelResult::ChannelReject(channel, queue)) => {
                warn!("Channel {} was rejected", channel);
                updated_pending_channels.push((channel, channel_cancel_token, queue));
            }
            Some(EstablishChannelResult::BadConnection(channel, queue, e)) => {
                warn!("Could not establish channel: {e:?}");
                connection_failed = true;
                updated_pending_channels.push((channel, channel_cancel_token, queue));
            }
        }
    }
    (updated_pending_channels, connection_failed)
}

// Async handler that does the following:
// 1. Establishes a TCP connection to a host identified by `connection_id`
// 2. Accepts incoming data channel requests from the sender dispatcher (living in the current process)
// 3. Handles these requests by negotiating over the previously created control connection
// 4. Holds and maintains the state of all active and pending data channels (connections) between us and the receiver host
// In general, the sender side (we) initiate the connection, creating an upstream --> downstream flow of information
pub(crate) async fn connection_handler(
    connection_id: ConnectionIdentifier,
    connection_cancellation_token: CancellationToken,
    controller: ConnectionController,
    mut listener: ConnectionControlListener,
) -> Result<()> {
    // Pending channels are channels that were requested by a query but do not have a fully established TCP connection yet
    let mut pending_data_channels: Vec<(
        ChannelIdentifier,
        CancellationToken,
        data_channel_handler::ChannelControlQueueListener,
    )> = vec![];
    // Active channels are data channels that have an established TCP connection
    let mut active_data_channels: HashMap<ChannelIdentifier, CancellationToken> =
        HashMap::default();

    'connection: loop {
        // 1. establish control connection
        let Some(connection) =
            establish_control_connection(&connection_id, &connection_cancellation_token).await
        else {
            // Cancellation of the connection handler was requested, cancel all active channels and exit
            return cancel_all(active_data_channels);
        };

        // At this point, we have a connection to `connection_id` and can process requests to create data channels
        let (mut reader, mut writer) = control_channel_sender(connection);
        // 2. accept channel requests in a loop
        // The only reason to exit this loop is when the control connection is lost
        loop {
            // TODO: set limit to number of pending channels?
            if let Some(mut new_pending_channels) = accept_channel_requests(
                connection_cancellation_token.clone(),
                Duration::from_millis(10),
                &mut active_data_channels,
                &mut listener,
            )
            .await
            {
                pending_data_channels.append(&mut new_pending_channels);
            } else {
                return cancel_all(active_data_channels);
            }

            let (updated_pending_channels, control_connection_failure) = handle_pending_channels(
                pending_data_channels,
                &mut active_data_channels,
                &connection_id,
                connection_cancellation_token.clone(),
                &mut writer,
                &mut reader,
                controller.clone(),
            )
            .await;
            pending_data_channels = updated_pending_channels;
            // retry to establish control connection if it failed, otherwise accept new channel requests
            if control_connection_failure {
                continue 'connection;
            }
        }
    }
}
