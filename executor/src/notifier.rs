use crate::{ExecutionIndices, ExecutionState};
use consensus::ConsensusOutput;
use tokio::task::JoinHandle;

use types::{metered_channel, Batch};

#[derive(Clone, Debug)]
pub struct BatchIndex {
    pub consensus_output: ConsensusOutput,
    pub next_certificate_index: u64,
    pub batch_index: u64,
}

pub struct Notifier<State: ExecutionState> {
    rx_notifier: metered_channel::Receiver<(BatchIndex, Batch)>,
    callback: State,
}

impl<State: ExecutionState + Send + Sync + 'static> Notifier<State> {
    pub fn spawn(
        rx_notifier: metered_channel::Receiver<(BatchIndex, Batch)>,
        callback: State,
    ) -> JoinHandle<()> {
        let notifier = Notifier {
            rx_notifier,
            callback,
        };
        tokio::spawn(notifier.run())
    }

    async fn run(mut self) {
        while let Some((index, batch)) = self.rx_notifier.recv().await {
            for (transaction_index, transaction) in batch.0.into_iter().enumerate() {
                let execution_indices = ExecutionIndices {
                    next_certificate_index: index.next_certificate_index,
                    next_batch_index: index.batch_index + 1,
                    next_transaction_index: transaction_index as u64 + 1,
                };
                self.callback
                    .handle_consensus_transaction(
                        &index.consensus_output,
                        execution_indices,
                        transaction,
                    )
                    .await;
            }
        }
    }
}
