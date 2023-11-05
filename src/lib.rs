pub mod routes;
mod states;
mod stream;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub use states::{Subscription, Subscriptions};
pub use stream::subscribe;
use stream::Records;

pub const ENV_DYNAMODB_ENDPOINT_URL: &str = "DYNAMODB_ENDPOINT_URL";
