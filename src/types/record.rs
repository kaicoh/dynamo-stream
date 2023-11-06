use aws_sdk_dynamodbstreams::{primitives, types};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Records {
    records: Vec<Record>,
}

impl Records {
    pub fn new(records: Vec<Record>) -> Self {
        Self { records }
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    #[cfg(test)]
    pub fn into_inner(self) -> Vec<Record> {
        self.records
    }
}

#[derive(Debug, Serialize, Clone)]
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

    pub fn event_id(&self) -> Option<String> {
        self.event_id.clone()
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

#[derive(Debug, Serialize, Clone, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
enum OperationType {
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

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "PascalCase")]
struct StreamRecord {
    approximate_creation_date_time: Option<DateTime<Utc>>,
    keys: Option<HashMap<String, AttributeValue>>,
    new_image: Option<HashMap<String, AttributeValue>>,
    old_image: Option<HashMap<String, AttributeValue>>,
    sequence_number: Option<String>,
    size_bytes: Option<i64>,
    stream_view_type: Option<StreamViewType>,
}

impl From<types::StreamRecord> for StreamRecord {
    fn from(value: types::StreamRecord) -> StreamRecord {
        StreamRecord {
            approximate_creation_date_time: value.approximate_creation_date_time.map(into_chrono),
            keys: value.keys.map(into_item),
            new_image: value.new_image.map(into_item),
            old_image: value.old_image.map(into_item),
            sequence_number: value.sequence_number,
            size_bytes: value.size_bytes,
            stream_view_type: value.stream_view_type.map(StreamViewType::from),
        }
    }
}

#[derive(Debug, Serialize, Clone, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
enum AttributeValue {
    B(String),
    Bool(bool),
    Bs(Vec<String>),
    L(Vec<AttributeValue>),
    M(HashMap<String, AttributeValue>),
    N(String),
    Ns(Vec<String>),
    Null(bool),
    S(String),
    Ss(Vec<String>),
    Unknown,
}

impl From<types::AttributeValue> for AttributeValue {
    fn from(value: types::AttributeValue) -> AttributeValue {
        match value {
            types::AttributeValue::B(v) => AttributeValue::B(into_str(v)),
            types::AttributeValue::Bool(v) => AttributeValue::Bool(v),
            types::AttributeValue::Bs(v) => {
                AttributeValue::Bs(v.into_iter().map(into_str).collect())
            }
            types::AttributeValue::L(v) => {
                AttributeValue::L(v.into_iter().map(AttributeValue::from).collect())
            }
            types::AttributeValue::M(v) => {
                let mut map: HashMap<String, AttributeValue> = HashMap::new();
                for (key, val) in v.iter() {
                    map.insert(key.to_owned(), AttributeValue::from(val.clone()));
                }
                AttributeValue::M(map)
            }
            types::AttributeValue::N(v) => AttributeValue::N(v),
            types::AttributeValue::Ns(v) => AttributeValue::Ns(v),
            types::AttributeValue::Null(v) => AttributeValue::Null(v),
            types::AttributeValue::S(v) => AttributeValue::S(v),
            types::AttributeValue::Ss(v) => AttributeValue::Ss(v),
            _ => AttributeValue::Unknown,
        }
    }
}

#[derive(Debug, Serialize, Clone, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StreamViewType {
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

#[derive(Debug, Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Identity {
    principal_id: Option<String>,
    r#type: Option<String>,
}

impl From<types::Identity> for Identity {
    fn from(value: types::Identity) -> Identity {
        Identity {
            principal_id: value.principal_id,
            r#type: value.r#type,
        }
    }
}

fn into_str(blob: primitives::Blob) -> String {
    String::from_utf8_lossy(&blob.into_inner()).into_owned()
}

fn into_chrono(datetime: primitives::DateTime) -> DateTime<Utc> {
    let secs = datetime.secs();
    let nsecs = datetime.subsec_nanos();
    DateTime::<Utc>::from_timestamp(secs, nsecs).expect("Invalid timestamp")
}

fn into_item(value: HashMap<String, types::AttributeValue>) -> HashMap<String, AttributeValue> {
    let mut map: HashMap<String, AttributeValue> = HashMap::new();
    for (key, val) in value.iter() {
        map.insert(key.to_owned(), AttributeValue::from(val.clone()));
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_transforms_blob_into_string() {
        let b = blob("Hello");
        assert_eq!(into_str(b), "Hello".to_string());
    }

    #[test]
    fn it_transforms_crate_datetime_into_chrono_datetime() {
        let org = primitives::DateTime::from_secs_and_nanos(946_713_600, 500_000_000u32);
        let dt = into_chrono(org);
        let expected = DateTime::<Utc>::from_timestamp(946_713_600, 500_000_000u32).unwrap();
        assert_eq!(dt, expected);
    }

    #[test]
    fn it_transforms_crate_attribute_value_into_original() {
        let mut crate_map: HashMap<String, types::AttributeValue> = HashMap::new();
        crate_map.insert("Test".into(), types::AttributeValue::S("Foo".into()));

        let mut expected: HashMap<String, AttributeValue> = HashMap::new();
        expected.insert("Test".into(), AttributeValue::S("Foo".into()));

        assert_eq!(into_item(crate_map), expected);
    }

    #[test]
    fn it_serializes_attribute_value_b() {
        let value = AttributeValue::B("dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk".into());
        let json = serde_json::to_value(value).unwrap();
        let expected = serde_json::json!({
            "B": "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"
        });
        assert_eq!(json, expected);
    }

    #[test]
    fn it_serializes_attribute_value_bool() {
        let value = AttributeValue::Bool(true);
        let json = serde_json::to_value(value).unwrap();
        let expected = serde_json::json!({
            "BOOL": true
        });
        assert_eq!(json, expected);
    }

    #[test]
    fn it_serializes_attribute_value_bs() {
        let value = AttributeValue::Bs(vec!["U3Vubnk=".into(), "UmFpbnk=".into()]);
        let json = serde_json::to_value(value).unwrap();
        let expected = serde_json::json!({
            "BS": ["U3Vubnk=", "UmFpbnk="]
        });
        assert_eq!(json, expected);
    }

    #[test]
    fn it_serializes_attribute_value_l() {
        let value = AttributeValue::L(vec![
            AttributeValue::S("Cookies".into()),
            AttributeValue::S("Coffee".into()),
        ]);
        let json = serde_json::to_value(value).unwrap();
        let expected = serde_json::json!({
            "L": [
                { "S": "Cookies" },
                { "S": "Coffee" }
            ]
        });
        assert_eq!(json, expected);
    }

    #[test]
    fn it_serializes_attribute_value_m() {
        let mut map: HashMap<String, AttributeValue> = HashMap::new();
        map.insert("Name".into(), AttributeValue::S("Joe".into()));
        map.insert("Age".into(), AttributeValue::N("35".into()));

        let value = AttributeValue::M(map);
        let json = serde_json::to_value(value).unwrap();
        let expected = serde_json::json!({
            "M": {
                "Name": { "S": "Joe" },
                "Age": { "N": "35" }
            }
        });
        assert_eq!(json, expected);
    }

    #[test]
    fn it_serializes_attribute_value_n() {
        let value = AttributeValue::N("123.45".into());
        let json = serde_json::to_value(value).unwrap();
        let expected = serde_json::json!({
            "N": "123.45"
        });
        assert_eq!(json, expected);
    }

    #[test]
    fn it_serializes_attribute_value_ns() {
        let value = AttributeValue::Ns(vec!["42.2".into(), "-19".into()]);
        let json = serde_json::to_value(value).unwrap();
        let expected = serde_json::json!({
            "NS": ["42.2", "-19"]
        });
        assert_eq!(json, expected);
    }

    #[test]
    fn it_serializes_attribute_value_null() {
        let value = AttributeValue::Null(true);
        let json = serde_json::to_value(value).unwrap();
        let expected = serde_json::json!({
            "NULL": true
        });
        assert_eq!(json, expected);
    }

    #[test]
    fn it_serializes_attribute_value_s() {
        let value = AttributeValue::S("Hello".into());
        let json = serde_json::to_value(value).unwrap();
        let expected = serde_json::json!({
            "S": "Hello"
        });
        assert_eq!(json, expected);
    }

    #[test]
    fn it_serializes_attribute_value_ss() {
        let value = AttributeValue::Ss(vec!["Giraffe".into(), "Hippo".into()]);
        let json = serde_json::to_value(value).unwrap();
        let expected = serde_json::json!({
            "SS": ["Giraffe", "Hippo"]
        });
        assert_eq!(json, expected);
    }

    fn blob(val: &str) -> primitives::Blob {
        primitives::Blob::new(val.as_bytes().to_vec())
    }
}
