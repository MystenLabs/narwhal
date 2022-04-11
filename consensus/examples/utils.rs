use async_trait::async_trait;
use config::WorkerId;
use consensus::SequenceNumber;
use serde::de::DeserializeOwned;
use std::time::Duration;
use store::StoreError;
use thiserror::Error;
use tokio::time::sleep;

#[async_trait]
pub trait AuthorityState {
    /// The type of the transaction to process.
    type Transaction: DeserializeOwned + Send;

    /// The error type to return in case something went wrong during execution.
    type Error: AuthorityStateError;

    /// Execute the transaction and atomically persist the consensus index.
    async fn handle_consensus_transaction(
        &self,
        next_certificate_index: SequenceNumber,
        next_batch_index: SequenceNumber,
        next_transaction_index: SequenceNumber,
        transaction: Self::Transaction,
    ) -> Result<(), Self::Error>;

    /// Simple guardrail ensuring there is a single instance using the state
    /// to call `handle_consensus_transaction`. Many instances may read the state,
    /// or use it for other purposes.
    fn ask_consensus_write_lock(&self) -> bool;

    /// Tell the state that the caller instance is no longer using calling
    //// `handle_consensus_transaction`.
    fn release_consensus_write_lock(&self);

    /// Load the last consensus index from storage.
    async fn load_last_consensus_indices(
        &self,
    ) -> Result<(SequenceNumber, SequenceNumber, SequenceNumber), Self::Error>;
}

pub type SubscriberResult<T> = Result<T, SubscriberError>;

#[derive(Debug, Error)]
pub enum SubscriberError {
    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    #[error("Consensus referenced unexpected worker id {0}")]
    UnexpectedWorkerId(WorkerId),

    #[error("Unexpected consensus index number {0}")]
    UnexpectedConsensusIndex(SequenceNumber),

    #[error("Deserialization of consensus message failed: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Received unexpected protocol message from consensus")]
    UnexpectedProtocolMessage,

    #[error("There can only be a single consensus client at the time")]
    OnlyOneConsensusClientPermitted,

    #[error("Execution engine failed: {0}")]
    NodeExecutionError(String),

    #[error("Client transaction invalid: {0}")]
    ClientExecutionError(String),
}

/// Trait do separate execution errors in two categories: (i) errors caused by a bad client, (ii)
/// errors caused by a fault in the authority.
pub trait AuthorityStateError {
    /// Whether the error is due to a fault in the authority (eg. internal storage error).
    fn node_error(&self) -> bool;

    /// Convert the error message in to a string.
    fn to_string(&self) -> String;
}

impl<T: AuthorityStateError> From<T> for SubscriberError {
    fn from(e: T) -> Self {
        match e.node_error() {
            true => SubscriberError::NodeExecutionError(e.to_string()),
            false => SubscriberError::ClientExecutionError(e.to_string()),
        }
    }
}

/// Make the network client wait a bit before re-attempting network connections.
pub struct ConnectionWaiter {
    /// The minimum delay to wait before re-attempting a connection.
    min_delay: u64,
    /// The maximum delay to wait before re-attempting a connection.
    max_delay: u64,
    /// The actual delay we wait before re-attempting a connection.
    delay: u64,
    /// The number of times we attempted to make a connection.
    retry: usize,
}

impl Default for ConnectionWaiter {
    fn default() -> Self {
        Self::new(/* min_delay */ 200, /* max_delay */ 60_000)
    }
}

impl ConnectionWaiter {
    /// Create a new connection waiter.
    pub fn new(min_delay: u64, max_delay: u64) -> Self {
        Self {
            min_delay,
            max_delay,
            delay: 0,
            retry: 0,
        }
    }

    /// Return the number of failed attempts.
    pub fn status(&self) -> &usize {
        &self.retry
    }

    /// Wait for a bit (depending on the number of failed connections).
    pub async fn wait(&mut self) {
        if self.delay != 0 {
            sleep(Duration::from_millis(self.delay)).await;
        }

        self.delay = match self.delay {
            0 => self.min_delay,
            _ => std::cmp::min(2 * self.delay, self.max_delay),
        };
        self.retry += 1;
    }

    /// Reset the waiter to its initial parameters.
    pub fn reset(&mut self) {
        self.delay = 0;
        self.retry = 0;
    }
}
