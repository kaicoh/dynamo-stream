use aws_sdk_dynamodbstreams::types;
use serde::Serialize;

#[derive(Debug, Serialize, Clone, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ShardIteratorType {
    AfterSequenceNumber,
    AtSequenceNumber,
    Latest,
    TrimHorizon,
    Unknown,
}

impl From<types::ShardIteratorType> for ShardIteratorType {
    fn from(value: types::ShardIteratorType) -> ShardIteratorType {
        match value {
            types::ShardIteratorType::AfterSequenceNumber => ShardIteratorType::AfterSequenceNumber,
            types::ShardIteratorType::AtSequenceNumber => ShardIteratorType::AtSequenceNumber,
            types::ShardIteratorType::Latest => ShardIteratorType::Latest,
            types::ShardIteratorType::TrimHorizon => ShardIteratorType::TrimHorizon,
            _ => ShardIteratorType::Unknown,
        }
    }
}
