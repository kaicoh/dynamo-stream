use std::sync::{MutexGuard, PoisonError};
use tracing::error;

pub fn from_guard<T>(error: PoisonError<MutexGuard<'_, T>>) -> anyhow::Error {
    error!("{:#?}", error);
    anyhow::anyhow!("{}", error)
}
