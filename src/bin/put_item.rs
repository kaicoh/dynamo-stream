use aws_sdk_dynamodb::{
    config::Builder as ConfigBuilder,
    types::AttributeValue,
    Client,
};
use dynamo_stream::ENV_DYNAMODB_ENDPOINT_URL;
use std::env;
use tracing::{error, info};
use tracing_subscriber::FmtSubscriber;
use ulid::Ulid;

const TABLE: &str = "People";
const PK: &str = "Id";

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let url = env::var(ENV_DYNAMODB_ENDPOINT_URL)
        .expect("env ENV_DYNAMODB_ENDPOINT_URL is required");
    let config = ConfigBuilder::from(&aws_config::load_from_env().await)
        .endpoint_url(url)
        .build();
    let client = Client::from_conf(config);

    match client
        .put_item()
        .table_name(TABLE)
        .item(PK, AttributeValue::S(Ulid::new().to_string()))
        .send()
        .await
    {
        Ok(output) => {
            if let Some(attributes) = output.attributes {
                info!("Put Item: {:?}", attributes);
            }
        },
        Err(err) => {
            error!("{:#?}", err);
        }
    }
}
