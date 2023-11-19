use aws_sdk_dynamodbstreams::types;
use serde::Serialize;

#[derive(Debug, Serialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum OperationType {
    Insert,
    Modify,
    Remove,
    Unknown,
}

impl From<types::OperationType> for OperationType {
    fn from(value: types::OperationType) -> OperationType {
        match value {
            types::OperationType::Insert => OperationType::Insert,
            types::OperationType::Modify => OperationType::Modify,
            types::OperationType::Remove => OperationType::Remove,
            _ => OperationType::Unknown,
        }
    }
}
