use super::{Identity, OperationType, StreamRecord};

use aws_sdk_dynamodbstreams::types;
use serde::Serialize;
use std::cmp::{Ord, Ordering, PartialOrd};

#[derive(Debug, Serialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    event_id: Option<String>,
    event_name: Option<OperationType>,
    event_version: Option<String>,
    event_source: Option<String>,
    aws_region: Option<String>,
    dynamodb: Option<StreamRecord>,
    user_identity: Option<Identity>,
}

#[cfg(test)]
impl Record {
    pub fn new<T: Into<String>>(event_id: T) -> Self {
        Self {
            event_id: Some(event_id.into()),
            event_name: None,
            event_version: None,
            event_source: None,
            aws_region: None,
            dynamodb: None,
            user_identity: None,
        }
    }

    pub fn event_id(&self) -> &str {
        self.event_id.as_ref().unwrap()
    }
}

impl From<types::Record> for Record {
    fn from(value: types::Record) -> Record {
        Record {
            event_id: value.event_id,
            event_name: value.event_name.map(OperationType::from),
            event_version: value.event_version,
            event_source: value.event_source,
            aws_region: value.aws_region,
            dynamodb: value.dynamodb.map(StreamRecord::from),
            user_identity: value.user_identity.map(Identity::from),
        }
    }
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.dynamodb.as_ref(), other.dynamodb.as_ref()) {
            (Some(s), Some(o)) => s.cmp(o),
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            _ => Ordering::Equal,
        }
    }
}
