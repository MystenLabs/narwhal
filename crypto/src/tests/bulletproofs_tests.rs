use bulletproofs::{PedersenGens, BulletproofGens, RangeProof, range_proof_mpc};
use curve25519_dalek_ng::scalar::Scalar;
use merlin::Transcript;
use crate::{bulletproofs::{PedersenCommitment, BulletproofsRangeProof}, traits::ToFromBytes};

///
/// Test Pedersen Commitments
/// 

#[test]
fn pedersen_commitment() {
    // Should we create a wrapper scalar type or is using this fine?
    let value = [0; 32];
    let blinding = [0; 32];

    // Commit
    let commitment = PedersenCommitment::new(value, blinding);

    // Open
    let commitment_2 = PedersenCommitment::new(value, blinding);
    assert_eq!(commitment, commitment_2);
}

#[test]
fn pedersen_commitment_binding() {
    let value = [0; 32];
    let other_value = [1; 32];
    let blinding = [2; 32];

    let commitment = PedersenCommitment::new(value, blinding);
    let other_commitment = PedersenCommitment::new(other_value, blinding);

    assert_ne!(commitment, other_commitment);
}

#[test]
fn pedersen_commitment_to_from_bytes() {
    let value = [0; 32];
    let blinding = [1; 32];

    let commitment = PedersenCommitment::new(value, blinding);
    let commitment_dup = PedersenCommitment::from_bytes(commitment.as_bytes()).unwrap();

    assert_eq!(commitment, commitment_dup);
}

#[test]
fn pedersen_commitment_serde() {
    let value = [0; 32];
    let blinding = [1; 32];

    let commitment = PedersenCommitment::new(value, blinding);
    let ser = bincode::serialize(&commitment).unwrap();
    let commitment_dup: PedersenCommitment = bincode::deserialize(&ser).unwrap();

    assert_eq!(commitment, commitment_dup);
}

#[test]
fn bulletproof_range_proof_lower_valid() {
    let secret = 1000u64;

    let blinding = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        0, 1];

    let (commitment, range_proof) = BulletproofsRangeProof::prove_single_lower(
        secret,
        blinding,
        1000 
    ).unwrap();

    assert!(range_proof.verify_single_lower(&commitment, 1000).is_ok());
}

#[test]
fn bulletproof_range_proof_lower_invalid() {
    let secret = 999u64;

    let blinding = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        0, 1];

    assert!(BulletproofsRangeProof::prove_single_lower(
        secret,
        blinding,
        1000 
    ).is_err());
}

#[test]
fn bulletproof_range_proof_upper_valid() {
    let secret = 1000u64;

    let blinding = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        0, 1];

    let (commitment, range_proof) = BulletproofsRangeProof::prove_single_upper(
        secret,
        blinding,
        1000 
    ).unwrap();

    assert!(range_proof.verify_single_upper(&commitment, 1000).is_ok());
}

#[test]
fn bulletproof_range_proof_upper_invalid() {
    let secret = 1001u64;

    let blinding = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        0, 1];

    assert!(BulletproofsRangeProof::prove_single_upper(
        secret,
        blinding,
        1000 
    ).is_err());
}

// fn bulletproof_range_proof_large_committed_value() {
//     let secret = 1999u64;

//     let blinding = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
//         0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
//         0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
//         0, 1];

//     let (_, range_proof) = BulletproofsRangeProof::prove_single(
//         secret,
//         blinding,
//         None,
//         Some(2000)
//     ).unwrap();

//     // Ensure that there is no panic when trying to verify the range of a committed value greater than u64.
//     let large_commitment = PedersenCommitment::new(Scalar::from(u64::max_value()).to_bytes(), blinding);

//     // Ensure that the code does not panic
//     assert!(range_proof.verify_single(&large_commitment, None, Some(2000)).is_err());
// }