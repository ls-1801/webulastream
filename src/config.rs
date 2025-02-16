use crate::network::protocol::{ChannelIdentifier, ConnectionIdentifier};
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
    },
    Sink {
        input_channel: ChannelIdentifier,
    },
}
#[derive(Deserialize, Serialize)]
pub struct Node {
    pub(crate) connection: ConnectionIdentifier,
    pub queries: Vec<Query>,
}

#[test]
fn test_serialization() {
    let node = Node {
        connection: 124,
        queries: vec![Query::Source {
            downstream_channel: "yes".to_string(),
            downstream_connection: 444,
        }],
    };

    println!("{}", serde_yaml::to_string(&node).unwrap())
}

pub(crate) fn load_config(file: &std::path::Path, index: usize) -> Node {
    let file = std::fs::File::open(file).unwrap();
    let mut nodes: Vec<Node> = serde_yaml::from_reader(&file).unwrap();
    nodes.remove(index)
}
