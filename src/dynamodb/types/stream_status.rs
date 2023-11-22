use aws_sdk_dynamodbstreams::types;
use serde::Serialize;

#[derive(Debug, Serialize, Clone, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum StreamStatus {
    Disabled,
    Disabling,
    Enabled,
    Enabling,
    Unknown,
}

impl From<types::StreamStatus> for StreamStatus {
    fn from(status: types::StreamStatus) -> StreamStatus {
        match status {
            types::StreamStatus::Disabled => StreamStatus::Disabled,
            types::StreamStatus::Disabling => StreamStatus::Disabling,
            types::StreamStatus::Enabled => StreamStatus::Enabled,
            types::StreamStatus::Enabling => StreamStatus::Enabling,
            _ => StreamStatus::Unknown,
        }
    }
}
