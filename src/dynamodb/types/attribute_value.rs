use super::into_str;

use aws_sdk_dynamodbstreams::types;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum AttributeValue {
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

#[cfg(test)]
mod tests {
    use super::super::{into_chrono, into_item};
    use super::*;
    use aws_sdk_dynamodbstreams::primitives;
    use chrono::{DateTime, Utc};

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
