use super::Subscriptions;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

pub async fn subscribe(state: Arc<Mutex<Subscriptions>>) {
    loop {
        sleep(Duration::from_secs(3)).await;

        let state = state.lock().unwrap();

        for (arn, subscriptions) in state.iter() {
            let arn = arn.clone();
            let subscriptions = subscriptions.clone();

            tokio::spawn(async move {
                for sub in subscriptions {
                    println!("DynamoDB Stream Arn: {}, Post to {}", arn, sub.url,);
                }
            });
        }
    }
}
