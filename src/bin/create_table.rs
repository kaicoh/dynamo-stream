use aws_sdk_dynamodb::{
    config::Builder as ConfigBuilder,
    types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
        StreamSpecification, StreamViewType,
    },
    Client,
};
use dynamo_stream::ENV_DYNAMODB_ENDPOINT_URL;
use std::env;
use tracing::{error, info};
use tracing_subscriber::FmtSubscriber;

const TABLE: &str = "People";
const PK: &str = "Id";

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

    match client
        .create_table()
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(PK)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .table_name(TABLE)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(PK)
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewImage)
                .build()
                .unwrap(),
        )
        .send()
        .await
    {
        Ok(output) => {
            if let Some(table_description) = output.table_description {
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
