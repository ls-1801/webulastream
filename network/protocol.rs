use std::fmt::Debug;
use bytes::{Bytes, BytesMut};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::ops::DerefMut;
use std::str::from_utf8;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::trace;

pub type ChannelIdentifier = String;
pub type ConnectionIdentifier = u16;
#[derive(Debug, Serialize, Deserialize)]
pub enum ControlChannelRequests {
    ChannelRequest(ChannelIdentifier),
}
#[derive(Debug, Serialize, Deserialize)]
pub enum ChannelResponse {
    OkChannelResponse(u16),
    DenyChannelResponse,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum DataChannelMessage {
    Data(TupleBuffer),
    Close,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum DataResponse {
    AckData(u64),
    NAckData(u64),
    Closed,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct TupleBuffer {
    pub sequence_number: u64,
    pub origin_id: u64,
    pub chunk_number: u64,
    pub number_of_tuples: u64,
    pub last_chunk: bool,
    pub data: Vec<u8>,
    pub child_buffers: Vec<Vec<u8>>,
}
pub async fn send_message<T, W>(writer: &mut W, data: &T) -> crate::sender::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize + Debug,
{
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    data.serialize(&mut serializer)?;
    writer.write_u64_le(serializer.view().len() as u64).await?;
    writer.write_all(serializer.view()).await?;
    trace!("Sent message: {data:?}");
    Ok(())
}

pub async fn try_read_message<'a, T>(reader: &mut TcpStream) -> crate::sender::Result<Option<T>>
where
    T: DeserializeOwned,
{
    let mut buf = 0u64.to_le_bytes();
    let bytes_read = match reader.try_read(&mut buf) {
        Ok(0) => return Err("Connection lost".into()),
        Ok(n) => n,
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            return Ok(None);
        }
        Err(e) => return Err(e.into()),
    };
    reader.read_exact(&mut buf[bytes_read..]).await?;
    let size = u64::from_le_bytes(buf);
    let mut buffer = BytesMut::zeroed(size as usize);
    reader.read_exact(&mut buffer).await?;

    let r = flexbuffers::Reader::get_root(&buffer[..])?;
    let data = T::deserialize(r)?;
    Ok(Some(data))
}
pub async fn read_message<T, R>(reader: &mut R) -> crate::sender::Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let size = reader.read_u64_le().await?;
    let mut buffer = BytesMut::zeroed(size as usize);
    reader.read_exact(&mut buffer).await?;

    let r = flexbuffers::Reader::get_root(&buffer[..])?;
    let data = T::deserialize(r)?;
    Ok(data)
}
