use std::marker::PhantomData;

use serde::de::DeserializeOwned;

/// An iterator over the values of a prefix.
pub struct Values<'a, V> {
    db_iter: rocksdb::DBRawIterator<'a>,
    _phantom: PhantomData<V>,
}

impl<'a, V: DeserializeOwned> Values<'a, V> {
    pub(crate) fn new(db_iter: rocksdb::DBRawIterator<'a>) -> Self {
        Self {
            db_iter,
            _phantom: PhantomData,
        }
    }
}

impl<'a, V: DeserializeOwned> Iterator for Values<'a, V> {
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        if self.db_iter.valid() {
            let value = self.db_iter.key().and_then(|_| {
                self.db_iter
                    .value()
                    .and_then(|v| bincode::deserialize(v).ok())
            });

            self.db_iter.next();
            value
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
impl<'a, V: DeserializeOwned> DoubleEndedIterator for Values<'a, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.db_iter.valid() {
            let value = self.db_iter.key().and_then(|_| {
                self.db_iter
                    .value()
                    .and_then(|v| bincode::deserialize(v).ok())
            });

            self.db_iter.prev();
            value
        } else {
            None
        }
    }
}
