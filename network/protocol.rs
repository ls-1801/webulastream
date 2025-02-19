use bytes::{Buf, Bytes, BytesMut};
use std::io::Cursor;
use std::ops::DerefMut;
use std::str::from_utf8;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::info;

pub type ChannelIdentifier = String;
pub type ConnectionIdentifier = u16;
pub enum ProtocolControl {
    NewDataChannel(ChannelIdentifier),
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct TupleBuffer {
    pub sequence_number: u64,
    pub origin_id: u64,
    pub chunk_number: u64,
    pub number_of_tuples: u64,
    pub last_chunk: bool,
    pub data: Bytes,
    pub child_buffers: Vec<Bytes>,
}
impl TupleBuffer {
    pub async fn serialize<T>(&self, writer: &mut T) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: AsyncWrite + Unpin,
    {
        writer.write_u64(self.sequence_number).await?;
        writer.write_u64(self.origin_id).await?;
        writer.write_u64(self.chunk_number).await?;
        writer.write_u64(self.number_of_tuples).await?;
        writer.write_u64(self.child_buffers.len() as u64).await?;
        writer.write_u8(if self.last_chunk { 1 } else { 0 }).await?;

        writer.write_u64(self.data.len() as u64).await?;
        writer.write_all(&self.data).await?;

        for child_buffer in self.child_buffers.iter() {
            writer.write_u64(child_buffer.len() as u64).await?;
            writer.write_all(&child_buffer).await?;
        }

        Ok(())
    }

    pub async fn deserialize<T>(reader: &mut T) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>
    where
        T: AsyncRead + Unpin,
    {
        let sequence_number = reader.read_u64().await?;
        let origin_id = reader.read_u64().await?;
        let chunk_number = reader.read_u64().await?;
        let number_of_tuples = reader.read_u64().await?;
        let number_of_children = reader.read_u64().await?;
        let last_chunk = reader.read_u8().await? == 1;

        let data_size = reader.read_u64().await?;
        let mut bytes = BytesMut::zeroed(data_size as usize);
        reader.read_exact(bytes.deref_mut()).await?;
        let data = bytes.freeze();

        let mut child_buffers = vec![];
        child_buffers.reserve(number_of_children as usize);
        for number_of_child in 0..number_of_children {
            let data_size = reader.read_u64().await?;
            let mut bytes = BytesMut::zeroed(data_size as usize);
            reader.read_exact(bytes.deref_mut()).await?;
            child_buffers.push(bytes.freeze());
        }

        Ok(TupleBuffer {
            sequence_number,
            origin_id,
            child_buffers,
            chunk_number,
            last_chunk,
            number_of_tuples,
            data,
        })
    }
}

#[tokio::test]
async fn test_serialization() {
    let buffer = TupleBuffer {
        sequence_number: 1,
        origin_id: 2,
        child_buffers: vec![Bytes::from("some bytes")],
        data: Bytes::from("This is data"),
        last_chunk: true,
        number_of_tuples: 4,
        chunk_number: 2,
    };

    let mut cursor = Cursor::new(Vec::new());
    buffer.serialize(&mut cursor).await.unwrap();
    cursor.set_position(0);
    let new_buffer = TupleBuffer::deserialize(&mut cursor).await.unwrap();

    assert_eq!(buffer, new_buffer);
}
