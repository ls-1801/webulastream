use bytes::{Bytes, BytesMut};
use std::ops::DerefMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

pub type ChannelIdentifier = String;
pub type ConnectionIdentifier = u16;
pub enum ProtocolControl {
    NewDataChannel(ChannelIdentifier),
}

pub struct TupleBuffer {
    pub sequence_number: u64,
    pub data: Bytes,
}
impl TupleBuffer {
    pub async fn serialize<T>(&self, writer: &mut T) -> Result<(), Box<dyn std::error::Error>>
    where
        T: AsyncWrite + Unpin,
    {
        writer.write_u64(self.sequence_number).await?;
        writer.write_u64(self.data.len() as u64).await?;
        writer.write_all(&self.data).await?;

        Ok(())
    }

    pub async fn deserialize<T>(reader: &mut T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: AsyncRead + Unpin,
    {
        let sequence_number = reader.read_u64().await?;
        let size = reader.read_u64().await?;
        let mut bytes = BytesMut::with_capacity(size as usize);
        reader.read_exact(bytes.deref_mut()).await?;

        Ok(TupleBuffer {
            sequence_number,
            data: bytes.freeze(),
        })
    }
}
