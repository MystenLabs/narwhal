// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crypto::Hash;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use store::rocks::DBMap;
use store::rocks::TypedStoreError::RocksDBError;
use store::Map;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tracing::warn;
use types::{Certificate, CertificateDigest, Round, StoreResult};

/// The main storage when we have to deal with certificates. It maintains
/// two storages, one main which saves the certificates by their ids, and a
/// secondary one which acts as an index to allow us fast retrieval based
/// for queries based in certificate rounds.
/// It also offers pub/sub capabilities in write events. By using the
/// `notify_read` someone can wait to hear until a certificate by a specific
/// id has been written in storage.
#[derive(Clone)]
pub struct CertificateStore {
    /// Holds the certificates by their digest id
    certificates_by_id: DBMap<CertificateDigest, Certificate>,
    /// A secondary index that keeps the certificate digest ids
    /// by the certificate rounds. That helps us to perform
    /// range requests based on rounds. We avoid storing again the
    /// certificate here to not waste space. To dereference we use
    /// the certificates_by_id storage.
    certificate_ids_by_round: DBMap<(Round, CertificateDigest), u8>,
    /// Senders to notify for a write that happened for
    /// the specified certificate digest id
    notify_on_write_subscribers: Arc<DashMap<CertificateDigest, VecDeque<Sender<Certificate>>>>,
}

impl CertificateStore {
    pub fn new(
        certificates_by_id: DBMap<CertificateDigest, Certificate>,
        certificate_ids_by_round: DBMap<(Round, CertificateDigest), u8>,
    ) -> CertificateStore {
        Self {
            certificates_by_id,
            certificate_ids_by_round,
            notify_on_write_subscribers: Arc::new(DashMap::new()),
        }
    }

    /// Inserts a certificate to the store
    pub fn write(&self, certificate: Certificate) -> StoreResult<()> {
        let mut batch = self.certificates_by_id.batch();

        let id = certificate.digest();
        let round = certificate.round();

        // write the certificate by its id
        batch = batch.insert_batch(&self.certificates_by_id, vec![(id, certificate.clone())])?;

        // write the certificate id by its round
        let key = (round, id);
        let value = 0;

        batch = batch.insert_batch(&self.certificate_ids_by_round, vec![(key, value)])?;

        // execute the batch (atomically) and return the result
        let result = batch.write();

        if result.is_ok() {
            self.notify_obligations(id, certificate);
        }

        result
    }

    /// Inserts multiple certificates in the storage. This is an atomic operation.
    /// In the end it notifies any subscribers that are waiting to hear for the
    /// value.
    pub fn write_all(
        &self,
        certificates: impl IntoIterator<Item = Certificate>,
    ) -> StoreResult<()> {
        let mut batch = self.certificates_by_id.batch();

        let certificates: Vec<(CertificateDigest, Certificate)> = certificates
            .into_iter()
            .map(|certificate| (certificate.digest(), certificate))
            .collect();

        // write the certificates by their ids
        batch = batch.insert_batch(&self.certificates_by_id, certificates.clone())?;

        // write the certificates id by their rounds
        let values: Vec<((Round, CertificateDigest), u8)> = certificates
            .iter()
            .map(|(_, c)| {
                let key = (c.round(), c.digest());
                let value = 0;

                (key, value)
            })
            .collect();

        batch = batch.insert_batch(&self.certificate_ids_by_round, values)?;

        // execute the batch (atomically) and return the result
        let result = batch.write();

        if result.is_ok() {
            for (_id, certificate) in certificates {
                self.notify_obligations(certificate.digest(), certificate);
            }
        }

        result
    }

    /// Retrieves a certificate from the store. If not found
    /// then None is returned as result.
    pub fn read(&self, id: CertificateDigest) -> StoreResult<Option<Certificate>> {
        self.certificates_by_id.get(&id)
    }

    /// Retrieves multiple certificates by their provided ids. The results
    /// are returned in the same sequence as the provided keys.
    pub fn read_all(
        &self,
        ids: impl IntoIterator<Item = CertificateDigest>,
    ) -> StoreResult<Vec<Option<Certificate>>> {
        self.certificates_by_id.multi_get(ids)
    }

    /// Waits to get notified until the requested certificate becomes available
    pub async fn notify_read(&self, id: CertificateDigest) -> StoreResult<Certificate> {
        // we register our interest to be notified with the value
        let (sender, receiver) = oneshot::channel();
        self.notify_on_write_subscribers
            .entry(id)
            .or_insert_with(VecDeque::new)
            .push_back(sender);

        // let's read the value because we might have missed the opportunity
        // to get notified about it
        if let Ok(Some(cert)) = self.read(id) {
            // notify any obligations - and remove the entries
            self.notify_obligations(id, cert.clone());

            // reply directly
            return Ok(cert);
        }

        // now wait to hear back the result
        let result = receiver
            .await
            .expect("Irrecoverable error while waiting to receive the notify_read result");

        Ok(result)
    }

    /// Deletes a single certificate by its digest.
    pub fn delete(&self, id: CertificateDigest) -> StoreResult<()> {
        // first read the certificate to get the round - we'll need in order
        // to delete the secondary index
        let cert = self.read(id)?;
        let round = if let Some(c) = cert {
            c.round()
        } else {
            return Ok(());
        };

        let mut batch = self.certificates_by_id.batch();

        // write the certificate by its id
        batch = batch.delete_batch(&self.certificates_by_id, vec![id])?;

        // write the certificate id by its round
        let key = (round, id);

        batch = batch.delete_batch(&self.certificate_ids_by_round, vec![key])?;

        // execute the batch (atomically) and return the result
        batch.write()
    }

    /// Deletes multiple certificates in an atomic way.
    pub fn delete_all(&self, ids: impl IntoIterator<Item = CertificateDigest>) -> StoreResult<()> {
        // first read the certificates to get their rounds - we'll need in order
        // to delete the secondary index
        let certs = self.read_all(ids)?;
        let ids = certs
            .clone()
            .into_iter()
            .filter_map(|c| c.map(|cert| cert.digest()))
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(());
        }

        let mut batch = self.certificates_by_id.batch();

        // delete the certificates by its ids
        batch = batch.delete_batch(&self.certificates_by_id, ids)?;

        let keys_by_round = certs
            .into_iter()
            .filter_map(|c| c.map(|cert| (cert.round(), cert.digest())))
            .collect::<Vec<(_, _)>>();

        batch = batch.delete_batch(&self.certificate_ids_by_round, keys_by_round)?;

        // execute the batch (atomically) and return the result
        batch.write()
    }

    /// Retrieves all the certificates with round >= the provided round.
    /// The result is returned with certificates sorted in round asc order
    pub fn after_round(&self, round: Round) -> StoreResult<Vec<Certificate>> {
        // The key is basically a composite of the dictated round and
        // the possible smallest value of the certificate digest (all byte values
        // should be zero).
        let key = (round, CertificateDigest::default());

        let mut certificates = Vec::new();
        for (key, _) in self.certificate_ids_by_round.iter().skip_to(&key)? {
            let certificate = self.certificates_by_id.get(&key.1)?.ok_or_else(|| {
                RocksDBError(format!(
                    "Certificate with id {} not found in main storage although it should",
                    key.1
                ))
            })?;

            certificates.push(certificate);
        }

        Ok(certificates)
    }

    /// Retrieves the certificates of the last round
    pub fn last_round(&self) -> StoreResult<Vec<Certificate>> {
        // starting from the last element - hence the last round - move backwards until
        // we find certificates of different round.
        let mut last = self.certificate_ids_by_round.iter().skip_to_last();

        let mut round = 0;
        let mut certificates = Vec::new();

        while let Some((key, _value)) = last.prev() {
            let (certificate_round, certificate_id) = key;

            // We treat zero as special value (round unset) in order to
            // capture the last certificate's round.
            if round == 0 {
                round = certificate_round;
            }

            // We are now in a different round so we want to
            // stop consuming anymore
            if round != certificate_round {
                break;
            }

            let certificate = self
                .certificates_by_id
                .get(&certificate_id)?
                .ok_or_else(|| {
                    RocksDBError(format!(
                        "Certificate with id {} not found in main storage although it should",
                        certificate_id
                    ))
                })?;

            certificates.push(certificate);
        }

        Ok(certificates)
    }

    /// Clears both the main storage of the certificates and the secondary index
    pub fn clear(&self) -> StoreResult<()> {
        self.certificates_by_id.clear()?;
        self.certificate_ids_by_round.clear()
    }

    /// Checks whether the storage is empty. The main storage is
    /// being used to determine this.
    pub fn is_empty(&self) -> bool {
        self.certificates_by_id.is_empty()
    }

    /// Notifies the subscribed ones to listen on updates for the
    /// certificate with the provided id. The obligations are notified
    /// with the provided value. The obligation entries under the certificate id
    /// are removed completely. If we fail to notify an obligation we don't
    /// fail and we rather print a warn message.
    fn notify_obligations(&self, id: CertificateDigest, value: Certificate) {
        if let Some((_, mut senders)) = self.notify_on_write_subscribers.remove(&id) {
            while let Some(s) = senders.pop_front() {
                if s.send(value.clone()).is_err() {
                    warn!("Couldn't notify obligation for certificate with id {id}");
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::certificate_store::CertificateStore;
    use crypto::Hash;
    use futures::future::join_all;
    use std::collections::{BTreeSet, HashSet};
    use std::time::Instant;
    use store::reopen;
    use store::rocks::open_cf;
    use store::rocks::DBMap;
    use test_utils::temp_dir;
    use test_utils::{certificate, committee, fixture_headers_round};
    use types::{Certificate, CertificateDigest, Round};

    fn new_store(path: std::path::PathBuf) -> CertificateStore {
        const CERTIFICATES_CF: &str = "certificates";
        const CERTIFICATE_IDS_BY_ROUND_CF: &str = "certificate_ids_by_round";

        let rocksdb = open_cf(path, None, &[CERTIFICATES_CF, CERTIFICATE_IDS_BY_ROUND_CF])
            .expect("Cannot open database");

        let (certificate_map, certificate_ids_by_round_map) = reopen!(&rocksdb,
            CERTIFICATES_CF;<CertificateDigest, Certificate>,
            CERTIFICATE_IDS_BY_ROUND_CF;<(Round,CertificateDigest), u8>
        );

        CertificateStore::new(certificate_map, certificate_ids_by_round_map)
    }

    // helper method that creates certificates for the provided
    // number of rounds.
    fn certificates(rounds: u64) -> Vec<Certificate> {
        let mut current_round: Vec<_> = Certificate::genesis(&committee(None))
            .into_iter()
            .map(|cert| cert.header)
            .collect();

        let mut result: Vec<Certificate> = Vec::new();
        for i in 0..rounds {
            let parents: BTreeSet<_> = current_round
                .iter()
                .map(|header| certificate(header).digest())
                .collect();
            (_, current_round) = fixture_headers_round(i, &parents);

            result.extend(
                current_round
                    .iter()
                    .map(certificate)
                    .collect::<Vec<Certificate>>(),
            );
        }

        result
    }

    #[tokio::test]
    async fn test_write_all_and_read_all() {
        // GIVEN
        let store = new_store(temp_dir());

        // create certificates for 10 rounds
        let certs = certificates(10);
        let ids = certs
            .iter()
            .map(|c| c.digest())
            .collect::<Vec<CertificateDigest>>();

        // store them in both main and secondary index
        store.write_all(certs.clone()).unwrap();

        // WHEN
        let result = store.read_all(ids).unwrap();

        // THEN
        assert_eq!(certs.len(), result.len());

        for (i, cert) in result.into_iter().enumerate() {
            let c = cert.expect("Certificate should have been found");

            assert_eq!(&c, certs.get(i).unwrap());
        }
    }

    #[tokio::test]
    async fn test_last_round() {
        // GIVEN
        let store = new_store(temp_dir());

        // create certificates for 50 rounds
        let certs = certificates(50);
        let ids = certs
            .iter()
            .map(|c| c.digest())
            .collect::<Vec<CertificateDigest>>();

        // store them in both main and secondary index
        store.write_all(certs.clone()).unwrap();

        // WHEN
        let result = store.last_round().unwrap();

        // THEN
        assert_eq!(result.len(), 4);

        for certificate in result {
            assert_eq!(certificate.round(), 50);
        }
    }

    #[tokio::test]
    async fn test_after_round() {
        // GIVEN
        let store = new_store(temp_dir());
        let total_rounds = 100;

        // create certificates for 50 rounds
        let now = Instant::now();

        println!("Generating certificates");

        let certs = certificates(total_rounds);
        let ids = certs
            .iter()
            .map(|c| c.digest())
            .collect::<Vec<CertificateDigest>>();

        println!(
            "Created certificates: {} seconds",
            now.elapsed().as_secs_f32()
        );

        let now = Instant::now();
        println!("Storing certificates");

        // store them in both main and secondary index
        store.write_all(certs.clone()).unwrap();

        println!(
            "Stored certificates: {} seconds",
            now.elapsed().as_secs_f32()
        );

        let round_cutoff = 21;

        // now filter the certificates over round 21
        let mut certs_ids_over_cutoff_round = certs
            .clone()
            .into_iter()
            .filter_map(|c| {
                if c.round() >= round_cutoff {
                    Some(c.digest())
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();

        // WHEN
        println!("Access after round");
        let now = Instant::now();
        let result = store
            .after_round(round_cutoff)
            .expect("Error returned while reading after_round");

        println!("Total time: {} seconds", now.elapsed().as_secs_f32());

        // THEN
        let certs_per_round = 4;
        assert_eq!(
            result.len() as u64,
            (total_rounds - round_cutoff + 1) * certs_per_round
        );

        // AND result certificates should be returned in increasing order
        let mut last_round = 0;
        for certificate in result {
            assert!(certificate.round() >= last_round);
            last_round = certificate.round();

            // should be amongst the certificates of the cut-off round
            assert!(certs_ids_over_cutoff_round.remove(&certificate.digest()));
        }

        // AND none should be left in the original set
        assert!(certs_ids_over_cutoff_round.is_empty());
    }

    #[tokio::test]
    async fn test_notify_read() {
        let store = new_store(temp_dir());

        // run the tests a few times
        for _ in 0..10 {
            let mut certs = certificates(3);
            let mut ids = certs
                .iter()
                .map(|c| c.digest())
                .collect::<Vec<CertificateDigest>>();

            let cloned_store = store.clone();

            // now populate a certificate
            let c1 = certs.remove(0);
            store.write(c1.clone()).unwrap();

            // spawn a task to notify_read on the certificate's id - we testing
            // the scenario where the value is already populated before
            // calling the notify read.
            let id = ids.remove(0);
            let handle_1 = tokio::spawn(async move {
                let result = cloned_store.notify_read(id).await;

                result
            });

            // now spawn a series of tasks before writing anything in store
            let mut handles = vec![];
            for id in ids {
                let cloned_store = store.clone();
                let handle = tokio::spawn(async move {
                    // wait until the certificate gets populated
                    let result = cloned_store.notify_read(id).await;

                    result
                });

                handles.push(handle)
            }

            // and populate the rest with a write_all
            store.write_all(certs).unwrap();

            // now wait on handle an assert result for a signle certificate
            let received_certificate = handle_1
                .await
                .expect("error")
                .expect("shouldn't receive store error");

            assert_eq!(received_certificate, c1);

            let result = join_all(handles).await;
            for r in result {
                let certificate_result = r.unwrap();
                assert!(certificate_result.is_ok());
            }

            // clear the store before next run
            store.clear().unwrap();
        }
    }

    #[tokio::test]
    async fn test_write_all_and_clear() {
        let store = new_store(temp_dir());

        // create certificates for 10 rounds
        let certs = certificates(10);

        // store them in both main and secondary index
        store.write_all(certs).unwrap();

        // confirm store is not empty
        assert!(!store.is_empty());

        // now clear the store
        store.clear().unwrap();

        // now confirm that store is empty
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn test_delete() {
        // GIVEN
        let store = new_store(temp_dir());

        // create certificates for 10 rounds
        let certs = certificates(10);

        // store them in both main and secondary index
        store.write_all(certs.clone()).unwrap();

        // WHEN now delete a couple of certificates
        let to_delete = certs.iter().take(2).map(|c| c.digest()).collect::<Vec<_>>();

        store.delete(to_delete[0]).unwrap();
        store.delete(to_delete[1]).unwrap();

        // THEN
        assert!(store.read(to_delete[0]).unwrap().is_none());
        assert!(store.read(to_delete[1]).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_all() {
        // GIVEN
        let store = new_store(temp_dir());

        // create certificates for 10 rounds
        let certs = certificates(10);

        // store them in both main and secondary index
        store.write_all(certs.clone()).unwrap();

        // WHEN now delete a couple of certificates
        let to_delete = certs.iter().take(2).map(|c| c.digest()).collect::<Vec<_>>();

        store.delete_all(to_delete.clone()).unwrap();

        // THEN
        assert!(store.read(to_delete[0]).unwrap().is_none());
        assert!(store.read(to_delete[1]).unwrap().is_none());
    }
}
