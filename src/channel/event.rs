#[derive(Debug)]
pub enum Event {
    Close,
    Error(anyhow::Error),
}
