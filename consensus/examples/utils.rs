use async_trait::async_trait;
use consensus::SequenceNumber;
use serde::de::DeserializeOwned;
use std::time::Duration;
use tokio::time::sleep;

#[async_trait]
pub trait AuthorityState {
    /// The type of the transaction to process.
    type Transaction: DeserializeOwned;

    /// The error type to return in case something went wrong during execution.
    type Error;

    /// Execute the transaction and atomically persist the consensus index.
    async fn handle_consensus_transaction(
        &self,
        consensus_index: SequenceNumber,
        transaction: Self::Transaction,
    ) -> Result<(), Self::Error>;

    /// Simple guardrail ensuring there is a single instance using the state
    /// to call `handle_consensus_transaction`. Many instances may read the state,
    /// or use it for other purposes.
    fn ask_consensus_write_lock(&self) -> bool;

    /// Tell the state that the caller instance is no longer using calling
    //// `handle_consensus_transaction`.
    fn release_consensus_write_lock(&self);
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
