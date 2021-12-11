// Copyright(C) 2021, Mysten Labs
// SPDX-License-Identifier: Apache-2.0
use bincode::Options;

use serde::de::DeserializeOwned;
use std::marker::PhantomData;

/// An iterator over the keys of a prefix.
pub struct Keys<'a, K> {
    db_iter: rocksdb::DBRawIterator<'a>,
    _phantom: PhantomData<K>,
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
