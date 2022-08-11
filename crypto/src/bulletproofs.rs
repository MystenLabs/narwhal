use std::ops;

use bulletproofs::{PedersenGens, BulletproofGens, RangeProof};
use curve25519_dalek_ng::{scalar::Scalar, ristretto::{RistrettoPoint, CompressedRistretto}};
use merlin::Transcript;
use once_cell::sync::OnceCell;
use serde::{Serialize, Deserialize, de};

use crate::traits::ToFromBytes;

// 
// Pedersen commitments
// 

#[derive(Debug, Clone)]
pub struct PedersenCommitment {
    point: RistrettoPoint,
    bytes: OnceCell<[u8; 32]>,
}

impl PedersenCommitment {
    ///
    /// Creates a new Pedersen commitment from a value, and a blinding factor
    ///
    pub fn new(value: [u8; 32], blinding_factor: [u8; 32]) -> Self {
        let generators = PedersenGens::default();
        let value = Scalar::from_bits(value);
        let blinding = Scalar::from_bits(blinding_factor);
        let point = generators.commit(value, blinding);

        PedersenCommitment {
            point,
            bytes: OnceCell::new(),
        }
    }
}

impl ops::Add<PedersenCommitment> for PedersenCommitment {
    type Output = PedersenCommitment;

    fn add(self, rhs: PedersenCommitment) -> PedersenCommitment {
        PedersenCommitment {
            point: self.point + rhs.point,
            bytes: OnceCell::new(),
        }
    }
}

impl ops::Sub<PedersenCommitment> for PedersenCommitment {
    type Output = PedersenCommitment;

    fn sub(self, rhs: PedersenCommitment) -> PedersenCommitment {
        PedersenCommitment {
            point: self.point - rhs.point,
            bytes: OnceCell::new(),
        }
    }
}

impl AsRef<[u8]> for PedersenCommitment {
    fn as_ref(&self) -> &[u8] {
        self.bytes.get_or_init(|| {
            self.point.compress().to_bytes()
        })
    }
}

impl ToFromBytes for PedersenCommitment {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        if bytes.len() != 32 {
            return Err(signature::Error::new());
        }
        let point = CompressedRistretto::from_slice(bytes);
        let decompressed_point = point.decompress()
            .ok_or(signature::Error::new())?;

        Ok(PedersenCommitment {
            point: decompressed_point,
            bytes: OnceCell::new(),
        })
    }
}

impl Serialize for PedersenCommitment {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    { 
        let bytes = self.as_ref();
        serializer.serialize_bytes(bytes)
    }
}

impl<'de> Deserialize<'de> for PedersenCommitment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let bytes = Vec::deserialize(deserializer)?;
        PedersenCommitment::from_bytes(&bytes[..])
            .map_err(|e| de::Error::custom(e.to_string()))
    }
}

impl PartialEq for PedersenCommitment {
    fn eq(&self, other: &Self) -> bool {
        self.point == other.point
    }
}

impl Eq for PedersenCommitment {}

impl PartialOrd for PedersenCommitment {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }
}

impl Ord for PedersenCommitment {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

///
/// Bulletproof Range Proofs
/// 

#[derive(Debug)]
pub struct BulletproofsRangeProof {
    proof: RangeProof,
    bytes: OnceCell<Vec<u8>>
}

impl BulletproofsRangeProof {
    /// Prove that value <= upper_bound
    pub fn prove_single_upper(
        value: u64,
        blinding: [u8; 32],
        upper_bound: u64,
    ) -> Result<(PedersenCommitment, Self), signature::Error> {
        if value > upper_bound {
            return Err(signature::Error::new());
        }

        let pc_gens = PedersenGens::default();
        let bp_gens = BulletproofGens::new(64, 1);
        let mut prover_transcript = Transcript::new(b"doctest example");

        let blinding = Scalar::from_bits(blinding);
        let bounding: u64 = u64::max_value();
        let new_value = value + (bounding - upper_bound);

        // Create a 64-bit rangeproof.
        let (proof, _) = RangeProof::prove_single(
            &bp_gens,
            &pc_gens,
            &mut prover_transcript,
            new_value,
            &blinding,
            64,
        ).expect("A real program could handle errors");

        let commitment = PedersenCommitment::new(Scalar::from(value).to_bytes(), blinding.to_bytes());

        Ok((
            commitment,
            BulletproofsRangeProof {
                proof,
                bytes: OnceCell::new(),
            }
        ))
    }

    /// Prove that value >= lower_bound 
    pub fn prove_single_lower(
        value: u64,
        blinding: [u8; 32],
        lower_bound: u64,
    ) -> Result<(PedersenCommitment, Self), signature::Error> {
        if value < lower_bound {
            return Err(signature::Error::new());
        }
        let pc_gens = PedersenGens::default();
        let bp_gens = BulletproofGens::new(64, 1);

        let mut prover_transcript = Transcript::new(b"doctest example");

        let blinding = Scalar::from_bits(blinding);

        let new_value: u64 = value - lower_bound;

        // Create a 64-bit rangeproof.
        let (proof, _) = RangeProof::prove_single(
            &bp_gens,
            &pc_gens,
            &mut prover_transcript,
            new_value,
            &blinding,
            64,
        ).expect("A real program could handle errors");

        let commitment = PedersenCommitment::new(Scalar::from(value).to_bytes(), blinding.to_bytes());

        Ok((
            commitment,
            BulletproofsRangeProof {
                proof,
                bytes: OnceCell::new(),
            }
        ))
    }

    pub fn verify_single_upper(
        &self,
        commitment: &PedersenCommitment,
        upper_bound: u64,
    ) -> Result<(), signature::Error> {
            let bounding = u64::max_value();
            let diff = bounding - upper_bound;

            let diff_scalar = Scalar::from(diff);

            let diff_commitment = PedersenCommitment::new(diff_scalar.to_bytes(), [0; 32]);
            let ped: PedersenCommitment = commitment.clone();
            let new_commit = ped + diff_commitment;
    
            let pc_gens = PedersenGens::default();
            let bp_gens = BulletproofGens::new(64, 1);
            
            let mut verifier_transcript = Transcript::new(b"doctest example");

            self
                .proof.verify_single(&bp_gens, &pc_gens, &mut verifier_transcript, &CompressedRistretto::from_slice(new_commit.as_bytes()), 64)
                .map_err(|_| signature::Error::new())
    }

    pub fn verify_single_lower(
        &self,
        commitment: &PedersenCommitment,
        upper_bound: u64,
    ) -> Result<(), signature::Error> {
            let diff_scalar = Scalar::from(upper_bound);

            let diff_commitment = PedersenCommitment::new(diff_scalar.to_bytes(), [0; 32]);
            let ped: PedersenCommitment = commitment.clone();

            let new_commit = ped - diff_commitment;
    
            let pc_gens = PedersenGens::default();
            let bp_gens = BulletproofGens::new(64, 1);
            
            let mut verifier_transcript = Transcript::new(b"doctest example");

            self
                .proof.verify_single(&bp_gens, &pc_gens, &mut verifier_transcript, &CompressedRistretto::from_slice(new_commit.as_bytes()), 64)
                .map_err(|_| signature::Error::new())
    }
}

impl AsRef<[u8]> for BulletproofsRangeProof {
    fn as_ref(&self) -> &[u8] {
        self.bytes.get_or_init(|| {
            self.proof.to_bytes()
        })
    }
}

impl ToFromBytes for BulletproofsRangeProof {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let proof = RangeProof::from_bytes(bytes)
            .map_err(|_| signature::Error::new())?;
        Ok(BulletproofsRangeProof {
            proof,
            bytes: OnceCell::new(),
        }) 
    }
}