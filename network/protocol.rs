use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_serde::formats::Cbor;
use tokio_serde::Framed;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};

pub type ChannelIdentifier = String;
pub type ConnectionIdentifier = String;

#[derive(Debug, Serialize, Deserialize)]
pub enum ControlChannelRequest {
    ChannelRequest(ChannelIdentifier),
}
#[derive(Debug, Serialize, Deserialize)]
pub enum ControlChannelResponse {
    OkChannelResponse(u16),
    DenyChannelResponse,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum DataChannelRequest {
    Data(TupleBuffer),
    Close,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum DataChannelResponse {
    AckData(u64),
    NAckData(u64),
    Close,
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct TupleBuffer {
    pub sequence_number: u64,
    pub origin_id: u64,
    pub watermark: u64,
    pub chunk_number: u64,
    pub number_of_tuples: u64,
    pub last_chunk: bool,
    pub data: Vec<u8>,
    pub child_buffers: Vec<Vec<u8>>,
}

impl Debug for TupleBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("TupleBuffer{{ sequence_number: {}, origin_id: {}, chunk_number: {}, watermark: {}, number_of_tuples: {}, bufferSize: {}, children: {:?}}}", self.sequence_number, self.origin_id, self.chunk_number, self.watermark, self.number_of_tuples, self.data.len(), self.child_buffers.iter().map(|buffer| buffer.len()).collect::<Vec<_>>()))
    }
}

pub type DataChannelSenderReader = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    DataChannelResponse,
    DataChannelResponse,
    Cbor<DataChannelResponse, DataChannelResponse>,
>;
pub type DataChannelSenderWriter = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    DataChannelRequest,
    DataChannelRequest,
    Cbor<DataChannelRequest, DataChannelRequest>,
>;
pub type DataChannelReceiverReader = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    DataChannelRequest,
    DataChannelRequest,
    Cbor<DataChannelRequest, DataChannelRequest>,
>;
pub type DataChannelReceiverWriter = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    DataChannelResponse,
    DataChannelResponse,
    Cbor<DataChannelResponse, DataChannelResponse>,
>;

pub type ControlChannelSenderReader = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    ControlChannelResponse,
    ControlChannelResponse,
    Cbor<ControlChannelResponse, ControlChannelResponse>,
>;
pub type ControlChannelSenderWriter = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    ControlChannelRequest,
    ControlChannelRequest,
    Cbor<ControlChannelRequest, ControlChannelRequest>,
>;
pub type ControlChannelReceiverReader = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    ControlChannelRequest,
    ControlChannelRequest,
    Cbor<ControlChannelRequest, ControlChannelRequest>,
>;
pub type ControlChannelReceiverWriter = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    ControlChannelResponse,
    ControlChannelResponse,
    Cbor<ControlChannelResponse, ControlChannelResponse>,
>;

pub fn data_channel_sender(
    stream: TcpStream,
) -> (DataChannelSenderReader, DataChannelSenderWriter) {
    let (read, write) = stream.into_split();
    let read = FramedRead::new(read, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Cbor::<DataChannelResponse, DataChannelResponse>::default(),
    );

    let write = FramedWrite::new(write, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Cbor::<DataChannelRequest, DataChannelRequest>::default(),
    );

    (read, write)
}

pub fn data_channel_receiver(
    stream: TcpStream,
) -> (DataChannelReceiverReader, DataChannelReceiverWriter) {
    let (read, write) = stream.into_split();
    let read = FramedRead::new(read, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Cbor::<DataChannelRequest, DataChannelRequest>::default(),
    );

    let write = FramedWrite::new(write, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Cbor::<DataChannelResponse, DataChannelResponse>::default(),
    );

    (read, write)
}

pub fn control_channel_sender(
    stream: TcpStream,
) -> (ControlChannelSenderReader, ControlChannelSenderWriter) {
    let (read, write) = stream.into_split();
    let read = FramedRead::new(read, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Cbor::<ControlChannelResponse, ControlChannelResponse>::default(),
    );

    let write = FramedWrite::new(write, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Cbor::<ControlChannelRequest, ControlChannelRequest>::default(),
    );

    (read, write)
}

pub fn control_channel_receiver(
    stream: TcpStream,
) -> (ControlChannelReceiverReader, ControlChannelReceiverWriter) {
    let (read, write) = stream.into_split();
    let read = FramedRead::new(read, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Cbor::<ControlChannelRequest, ControlChannelRequest>::default(),
    );

    let write = FramedWrite::new(write, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Cbor::<ControlChannelResponse, ControlChannelResponse>::default(),
    );

    (read, write)
}
