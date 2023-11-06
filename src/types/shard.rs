#[derive(Debug, Clone)]
pub struct Shard {
    id: String,
    status: Status,
    iterator: String,
}

#[derive(Debug, Copy, Clone)]
pub enum Status {
    Open,
    Closed,
}

impl Shard {
    pub fn new<S, T>(id: S, iterator: T) -> Self
    where
        S: Into<String>,
        T: Into<String>,
    {
        Self {
            id: id.into(),
            status: Status::Open,
            iterator: iterator.into(),
        }
    }

    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    pub fn iterator(&self) -> &str {
        self.iterator.as_str()
    }

    pub fn is_open(&self) -> bool {
        matches!(self.status, Status::Open)
    }

    pub fn is_closed(&self) -> bool {
        matches!(self.status, Status::Closed)
    }

    pub fn set_iterator(&mut self, iterator: Option<String>) {
        match iterator {
            Some(iterator) => {
                self.status = Status::Open;
                self.iterator = iterator;
            }
            None => {
                self.status = Status::Closed;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_sets_status_with_iterator() {
        let mut shard = Shard::new("foo", "bar");
        assert!(shard.is_open());

        shard.set_iterator(None);
        assert!(shard.is_closed());

        shard.set_iterator(Some("foobar".into()));
        assert!(shard.is_open());
    }
}
