use crate::protocol;
use crate::protocol::TupleBuffer;
use crate::receiver::network_service::{DataQueue, Error};
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::info;

const TCP_LISTENER_TIMEOUT_SECONDS: u64 = 3;

pub(crate) enum ChannelHandlerError {
    ClosedByOtherSide,
    Cancelled,
    Network(Error),
    Timeout,
}

pub(crate) async fn data_channel_handler(
    cancellation_token: CancellationToken,
    queue: &mut DataQueue,
    listener: &mut TcpListener,
) -> Result<(), ChannelHandlerError> {
    let (stream, address) = select! {
        _ =  cancellation_token.cancelled() => return Err(ChannelHandlerError::Cancelled),
        _ = tokio::time::sleep(Duration::from_secs(TCP_LISTENER_TIMEOUT_SECONDS))
            => return Err(ChannelHandlerError::Timeout),
        accept_result = listener.accept() => match accept_result {
            Err(e) => return Err(ChannelHandlerError::Network(e.into())),
            Ok((stream, addr)) => (stream, addr),
        }
    };

    // Represents a bidirectional data flow via the TcpStream object between the upstream host 
    // that we receive from (reader) and respond to (writer).
    let (mut reader, mut writer) = protocol::data_channel_receiver(stream);

    let mut arrived_buffer: Option<TupleBuffer> = None;
    info!("Accepted data channel connection from {address}");
    loop {
        // If we have received a buffer over the network, write it out via the queue for consumption
        if let Some(buf) = arrived_buffer.take() {
            // Extract the seq_number for further use
            let seq_number = buf.sequence_number;
            select! {
                _ = cancellation_token.cancelled() => return Err(ChannelHandlerError::Cancelled),
                // Try to send the buffer downstream to the receiver of the queue
                write_queue_result = queue.send(buf) => {
                    match write_queue_result {
                        Ok(_) => {
                            match cancellation_token.run_until_cancelled(
                                writer.send(protocol::DataChannelResponse::AckData(seq_number))
                            ).await {
                                None => return Err(ChannelHandlerError::Cancelled),
                                Some(Err(e)) => return Err(ChannelHandlerError::Network(e.into())),
                                Some(Ok(_)) => (),
                            }
                        },
                        // Channel was closed by the network bridge, propagate this back to the sender via the network
                        Err(_) => {
                            match cancellation_token.run_until_cancelled(
                                writer.send(protocol::DataChannelResponse::Close)
                            ).await {
                                None => return Err(ChannelHandlerError::Cancelled),
                                Some(Err(e)) => return Err(ChannelHandlerError::Network(e.into())),
                                Some(Ok(_)) => (),
                            }
                        }
                    }
                },
            }
        }

        // Read the next buffer from the network, return errors to the caller if any
        select! {
            _ = cancellation_token.cancelled() => return Err(ChannelHandlerError::Cancelled),
            next_request = reader.next() => arrived_buffer = {
                match next_request {
                    None => return Err(ChannelHandlerError::Network("Connection lost".into())),
                    Some(Err(e)) => return Err(ChannelHandlerError::Network(e.into())),
                    Some(Ok(protocol::DataChannelRequest::Data(buf))) => Some(buf),
                    Some(Ok(protocol::DataChannelRequest::Close)) => {
                        queue.close();
                        return Err(ChannelHandlerError::ClosedByOtherSide);
                    }
                }
            },
        }
    }
}
