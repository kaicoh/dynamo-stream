use aws_sdk_dynamodbstreams::types;
use serde::Serialize;

#[derive(Debug, Serialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
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
