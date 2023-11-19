use super::Record;

use serde::Serialize;

#[derive(Debug, Default, Serialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Records {
    records: Vec<Record>,
}

impl Records {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn append(&mut self, records: &mut Records) {
        self.records.append(&mut records.records)
    }

    pub fn sort(&mut self) {
        self.records.sort()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    #[cfg(test)]
    pub fn includes<T: Into<String>>(&self, event_id: T) -> bool {
        let event_id: String = event_id.into();
        self.records
            .iter()
            .find(|r| r.event_id() == event_id.as_str())
            .is_some()
    }
}

impl<I, T> From<I> for Records
where
    I: IntoIterator<Item = T>,
    T: Into<Record>,
{
    fn from(values: I) -> Records {
        let records = values.into_iter().map(|v| v.into()).collect();
        Records { records }
    }
}
