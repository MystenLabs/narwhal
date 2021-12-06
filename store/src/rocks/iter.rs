use std::marker::PhantomData;

use bincode::Options;
use eyre::Result;
use serde::{de::DeserializeOwned, Serialize};

/// An iterator over all key-value pairs in a data map.
pub struct Iter<'a, K, V> {
    db_iter: rocksdb::DBRawIterator<'a>,
    _phantom: PhantomData<(K, V)>,
}

impl<'a, K: Serialize, V> Iter<'a, K, V> {
    /// Positions the iterator at the key passed as an argument, or the next one if not present.
    ///
    pub fn skip_to(&mut self, key: &K) -> Result<()> {
        let config = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();
        self.db_iter.seek(config.serialize(key)?);
        Ok(())
    }

    /// Positions the iterator at the key passed as an argument, or the previous one if not present.
    ///
    pub fn skip_to_previous(&mut self, key: &K) -> Result<()> {
        let config = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();
        self.db_iter.seek_for_prev(config.serialize(key)?);
        Ok(())
    }
}

impl<'a, K: DeserializeOwned, V: DeserializeOwned> Iter<'a, K, V> {
    pub(super) fn new(db_iter: rocksdb::DBRawIterator<'a>) -> Self {
        Self {
            db_iter,
            _phantom: PhantomData,
        }
    }
}

impl<'a, K: DeserializeOwned, V: DeserializeOwned> Iterator for Iter<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.db_iter.valid() {
            let config = bincode::DefaultOptions::new()
                .with_big_endian()
                .with_fixint_encoding();
            let key = self.db_iter.key().and_then(|k| config.deserialize(k).ok());
            let value = self
                .db_iter
                .value()
                .and_then(|v| bincode::deserialize(v).ok());

            self.db_iter.next();
            key.and_then(|k| value.map(|v| (k, v)))
        } else {
            None
        }
    }
}

/// Note: while the traditional implementation of a [DoubleEndedIterator][std::iter::DoubleEndedIterator]
/// is meant to be "an iterator that takes elements from the back", what this rather implements
/// is an iterator that iterates in the reverse direction.
/// As a consequence, what elements will be iterated on is dependent on which element the iterator is
/// positioned at when calling an adapter that relies on this traits' implementation, such as
/// [rev][std::iter::Iterator::rev].
///
/// [std::iter::DoubleEndedIterator][https://doc.rust-lang.org/std/iter/trait.DoubleEndedIterator.html]
/// [std::iter::Iterator::rev][https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.rev]
///
impl<'a, K: DeserializeOwned, V: DeserializeOwned> DoubleEndedIterator for Iter<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.db_iter.valid() {
            let config = bincode::DefaultOptions::new()
                .with_big_endian()
                .with_fixint_encoding();
            let key = self.db_iter.key().and_then(|k| config.deserialize(k).ok());
            let value = self
                .db_iter
                .value()
                .and_then(|v| bincode::deserialize(v).ok());

            self.db_iter.prev();
            key.and_then(|k| value.map(|v| (k, v)))
        } else {
            None
        }
    }
}
