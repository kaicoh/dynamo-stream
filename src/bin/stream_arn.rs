use aws_sdk_dynamodb::{config::Builder as ConfigBuilder, Client};
use dynamo_stream::ENV_DYNAMODB_ENDPOINT_URL;
use std::env;
use tracing::{error, info};
use tracing_subscriber::FmtSubscriber;

const TABLE: &str = "People";

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let url =
        env::var(ENV_DYNAMODB_ENDPOINT_URL).expect("env ENV_DYNAMODB_ENDPOINT_URL is required");
    let config = ConfigBuilder::from(&aws_config::load_from_env().await)
        .endpoint_url(url)
        .build();
    let client = Client::from_conf(config);

    match client.describe_table().table_name(TABLE).send().await {
        Ok(output) => {
            if let Some(table_description) = output.table {
                info!(
                    "Stream ARN: {}",
                    table_description.latest_stream_arn.unwrap_or_default()
                );
            }
        }
        Err(err) => {
            error!("{:#?}", err);
        }
    }
}
