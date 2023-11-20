pub mod root;

use super::{
    error::HttpError,
    extractor::{FromValidate, Json},
    subscription::Subscription,
    SharedState,
};

use std::sync::{MutexGuard, PoisonError};
use tracing::error;

fn from_guard<T>(error: PoisonError<MutexGuard<'_, T>>) -> anyhow::Error {
    error!("{:#?}", error);
    anyhow::anyhow!("{}", error)
}
