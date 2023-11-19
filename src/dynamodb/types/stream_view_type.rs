use aws_sdk_dynamodbstreams::types;
use serde::Serialize;

#[derive(Debug, Serialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamViewType {
    KeysOnly,
    NewAndOldImages,
    NewImage,
    OldImage,
    Unknown,
}

impl From<types::StreamViewType> for StreamViewType {
    fn from(value: types::StreamViewType) -> StreamViewType {
        match value {
            types::StreamViewType::KeysOnly => StreamViewType::KeysOnly,
            types::StreamViewType::NewAndOldImages => StreamViewType::NewAndOldImages,
            types::StreamViewType::NewImage => StreamViewType::NewImage,
            types::StreamViewType::OldImage => StreamViewType::OldImage,
            _ => StreamViewType::Unknown,
        }
    }
}
