use bincode::Options;
use eyre::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;

/// An iterator over the keys of a prefix.
pub struct Keys<'a, K> {
    db_iter: rocksdb::DBRawIterator<'a>,
    _phantom: PhantomData<K>,
}

impl<'a, K: Serialize> Keys<'a, K> {
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

impl<'a, K: DeserializeOwned> Keys<'a, K> {
    pub(crate) fn new(db_iter: rocksdb::DBRawIterator<'a>) -> Self {
        Self {
            db_iter,
            _phantom: PhantomData,
        }
    }
}

impl<'a, K: DeserializeOwned> Iterator for Keys<'a, K> {
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        if self.db_iter.valid() {
            let config = bincode::DefaultOptions::new()
                .with_big_endian()
                .with_fixint_encoding();
            let key = self.db_iter.key().and_then(|k| config.deserialize(k).ok());

            self.db_iter.next();
            key
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
impl<'a, K: DeserializeOwned> DoubleEndedIterator for Keys<'a, K> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.db_iter.valid() {
            let config = bincode::DefaultOptions::new()
                .with_big_endian()
                .with_fixint_encoding();
            let key = self.db_iter.key().and_then(|k| config.deserialize(k).ok());

            self.db_iter.prev();
            key
        } else {
            None
        }
    }
}
