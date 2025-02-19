use distributed::protocol::{ChannelIdentifier, ConnectionIdentifier};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Query {
    Source {
        downstream_channel: ChannelIdentifier,
        downstream_connection: ConnectionIdentifier,
    },
    Bridge {
        input_channel: ChannelIdentifier,
        downstream_channel: ChannelIdentifier,
        downstream_connection: ConnectionIdentifier,
        ingestion_rate_in_milliseconds: Option<u64>,
    },
    Sink {
        input_channel: ChannelIdentifier,
        ingestion_rate_in_milliseconds: Option<u64>,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Command {
    StartQuery { q: Query },
    StopQuery { id: usize },
    Wait { millis: usize },
}
#[derive(Deserialize, Serialize)]
pub struct Node {
    pub(crate) connection: ConnectionIdentifier,
    pub commands: Vec<Command>,
}
pub(crate) fn load_config(file: &std::path::Path, index: usize) -> Node {
    let file = std::fs::File::open(file).unwrap();
    let mut nodes: Vec<Node> = serde_yaml::from_reader(&file).unwrap();
    nodes.remove(index)
}
