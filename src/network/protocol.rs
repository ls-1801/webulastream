pub type ChannelIdentifier = String;
pub type ConnectionIdentifier = u16;
pub enum ProtocolControl {
    NewDataChannel(ChannelIdentifier),
}