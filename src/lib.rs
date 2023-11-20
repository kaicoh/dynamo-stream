mod channel;
mod dynamodb;
pub mod web;

pub const ENV_DYNAMODB_ENDPOINT_URL: &str = "DYNAMODB_ENDPOINT_URL";

pub use dynamodb::client::DynamodbClient;
