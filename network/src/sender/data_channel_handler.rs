use crate::protocol;
use crate::protocol::{
    DataChannelRequest, DataChannelResponse, DataChannelSenderReader, DataChannelSenderWriter,
    TupleBuffer,
};
use futures::SinkExt;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, trace, warn};

type Result<T> = core::result::Result<T, ChannelHandlerError>;

pub(crate) enum ChannelHandlerResult {
    Cancelled,
    ConnectionLost(Box<dyn std::error::Error + Send + Sync>),
    Closed,
}

const MAX_PENDING_ACKS: usize = 64;

#[derive(Debug)]
pub enum ChannelControlMessage {
    Data(TupleBuffer),
    Flush(oneshot::Sender<()>),
    Terminate,
}
pub type ChannelControlQueue = async_channel::Sender<ChannelControlMessage>;
pub(crate) type ChannelControlQueueListener = async_channel::Receiver<ChannelControlMessage>;
pub(crate) enum ChannelHandlerError {
    ClosedByOtherSide,
    ConnectionLost(Box<dyn std::error::Error + Send + Sync>),
    Protocol(Box<dyn std::error::Error + Send + Sync>),
    Cancelled,
    Terminated,
}
pub(crate) struct ChannelHandler {
    // Cancellation for this data channel
    cancellation_token: CancellationToken,
    // Pending buffers that have not been sent
    pending_writes: VecDeque<TupleBuffer>,
    // Pending buffers that have been sent, but have not yet received an ACK
    wait_for_ack: HashMap<u64, TupleBuffer>,
    // Handle to write to the TcpStream
    writer: DataChannelSenderWriter,
    // Handle to read from the TcpStream
    reader: DataChannelSenderReader,
    queue: ChannelControlQueueListener,
}

impl ChannelHandler {
    pub fn new(
        cancellation_token: CancellationToken,
        stream: TcpStream,
        queue: ChannelControlQueueListener,
    ) -> Self {
        let (reader, writer) = protocol::data_channel_sender(stream);

        Self {
            cancellation_token,
            pending_writes: Default::default(),
            wait_for_ack: Default::default(),
            reader,
            writer,
            queue,
        }
    }

    async fn handle_request(
        &mut self,
        channel_control_message: ChannelControlMessage,
    ) -> Result<()> {
        info!("Received internal request: {:?}", channel_control_message);
        match channel_control_message {
            ChannelControlMessage::Data(data) => self.pending_writes.push_back(data),
            ChannelControlMessage::Flush(done) => {
                self.flush().await?;
                let _ = done.send(());
            }
            ChannelControlMessage::Terminate => {
                let _ = self.writer.send(DataChannelRequest::Close).await;
                return Err(ChannelHandlerError::Terminated);
            }
        }
        Ok(())
    }

    // Handle a response from the receiver at the downstream host
    fn handle_response(&mut self, response: DataChannelResponse) -> Result<()> {
        info!("Received response from downstream host: {:?}", response);
        match response {
            // Request to close the data channel
            DataChannelResponse::Close => {
                info!("Channel closed by other receiver");
                return Err(ChannelHandlerError::ClosedByOtherSide);
            }
            // Receiver has NACKed buffer with sequence number seq, move back into pending list
            DataChannelResponse::NAckData(seq) => {
                if let Some(write) = self.wait_for_ack.remove(&seq) {
                    warn!("NAck for {seq}");
                    self.pending_writes.push_back(write);
                } else {
                    return Err(ChannelHandlerError::Protocol(
                        format!("Protocol Error. Unknown Seq {seq}").into(),
                    ));
                }
            }
            // Receiver has ACKed buffer with sequence number seq, remove from list
            DataChannelResponse::AckData(seq) => {
                let Some(_) = self.wait_for_ack.remove(&seq) else {
                    return Err(ChannelHandlerError::Protocol(
                        format!("Protocol Error. Unknown Seq {seq}").into(),
                    ));
                };
                trace!("Ack for {seq}");
            }
        }

        Ok(())
    }

    // Sends the oldest buffer through the data channel
    // On success, move the buffer to the wait_for_ack map
    async fn send_pending(
        writer: &mut DataChannelSenderWriter,
        pending_writes: &mut VecDeque<TupleBuffer>,
        wait_for_ack: &mut HashMap<u64, TupleBuffer>,
    ) -> Result<()> {
        if pending_writes.is_empty() {
            return Ok(());
        }

        let next_buffer = pending_writes.front().expect("BUG: check value earlier");

        if writer
            .feed(DataChannelRequest::Data(next_buffer.clone()))
            .await
            .is_ok()
        {
            wait_for_ack.insert(
                next_buffer.sequence_number,
                pending_writes
                    .pop_front()
                    .expect("BUG: checked value earlier"),
            );
        }
        Ok(())
    }

    // Flush until there are no pending writes and no ACKs missing
    async fn flush(&mut self) -> Result<()> {
        while !self.pending_writes.is_empty() || !self.wait_for_ack.is_empty() {
            self.writer
                .flush()
                .await
                .map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?;

            if !self.pending_writes.is_empty() && self.wait_for_ack.len() < MAX_PENDING_ACKS {
                select! {
                    _ = self.cancellation_token.cancelled() => {return Err(ChannelHandlerError::Cancelled);},
                    response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                    send_result = Self::send_pending(&mut self.writer, &mut self.pending_writes, &mut self.wait_for_ack) => send_result?,
                }
            } else {
                select! {
                    _ = self.cancellation_token.cancelled() => {return Err(ChannelHandlerError::Cancelled);},
                    response = self.reader.next() => self.handle_response(response.ok_or(ChannelHandlerError::ClosedByOtherSide)?.map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?)?,
                }
            }
        }

        Ok(())
    }

    // Race the cancellation token, a response from downstream, a request from this process
    // Deal with whatever arrives first, cancel the others, repeat in loop
    pub(super) async fn run(&mut self) -> Result<()> {
        loop {
            // If we have pending writes AND currently wait for less than MAX_PENDING_ACKS:
            // Additional select arm: we are allowed to send one pending buffer
            // Otherwise (no pending writes or too many unacked buffers), only deal with process requests/downstream responses
            let can_send =
                !self.pending_writes.is_empty() && self.wait_for_ack.len() < MAX_PENDING_ACKS;
            select! {
                // Send data to downstream node, move data to wait_for_ack list
                send_result = async {
                    if can_send {
                        Self::send_pending(&mut self.writer, &mut self.pending_writes, &mut self.wait_for_ack).await
                    } else {
                        futures::future::pending().await
                    }
                } => send_result?,
                // Cancellation of the data channel
                _ = self.cancellation_token.cancelled() => {return Err(ChannelHandlerError::Cancelled);},
                // Receive and handle response from downstream node via TCP (acks, channel close request)
                response = self.reader.next() => self.handle_response(
                    response
                    .ok_or(ChannelHandlerError::ClosedByOtherSide)?
                    .map_err(|e| ChannelHandlerError::ConnectionLost(e.into()))?
                )?,
                // Receive and handle request from the current process via async channel (data, flush, terminate)
                request = self.queue.recv() => self.handle_request(request.map_err(|_| ChannelHandlerError::Cancelled)?).await?,
            }
        }
    }
}

pub(crate) async fn data_channel_handler(
    cancellation_token: CancellationToken,
    channel_address: SocketAddr,
    queue: ChannelControlQueueListener,
) -> ChannelHandlerResult {
    let socket = match TcpSocket::new_v4() {
        Ok(socket) => socket,
        Err(e) => {
            return ChannelHandlerResult::ConnectionLost(
                format!("Could not create socket {e:?}").into(),
            );
        }
    };

    // TCP connection establishment with the downstream host for the data channel
    let connection = match cancellation_token
        .run_until_cancelled(socket.connect(channel_address))
        .await
    {
        None => return ChannelHandlerResult::Cancelled,
        Some(Err(e)) => return ChannelHandlerResult::ConnectionLost(e.into()),
        Some(Ok(connection)) => connection,
    };
    info!(
        "Successfully established TCP connection to {}",
        channel_address
    );

    let mut handler = ChannelHandler::new(cancellation_token, connection, queue);
    match handler.run().await {
        Ok(_) => ChannelHandlerResult::Closed,
        Err(ChannelHandlerError::Terminated) => ChannelHandlerResult::Closed,
        Err(ChannelHandlerError::ClosedByOtherSide) => ChannelHandlerResult::Closed,
        Err(ChannelHandlerError::Cancelled) => ChannelHandlerResult::Cancelled,
        Err(ChannelHandlerError::ConnectionLost(e)) => ChannelHandlerResult::ConnectionLost(e),
        Err(ChannelHandlerError::Protocol(e)) => ChannelHandlerResult::ConnectionLost(e),
    }
}
