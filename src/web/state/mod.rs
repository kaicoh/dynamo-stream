mod app;

use super::{
    config::Config,
    subscription::{Destination, Subscription},
    DynamodbClient,
};

use std::sync::{Arc, Mutex};

pub use app::AppState;

pub type SharedState = Arc<Mutex<AppState>>;

impl From<AppState> for SharedState {
    fn from(state: AppState) -> Self {
        Arc::new(Mutex::new(state))
    }
}
