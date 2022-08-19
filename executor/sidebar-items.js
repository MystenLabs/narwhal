window.SIDEBAR_ITEMS = {"constant":[["DEFAULT_CHANNEL_SIZE","Default inter-task channel size."]],"enum":[["SubscriberError",""]],"macro":[["bail",""],["ensure",""],["try_fut_and_permit",""]],"struct":[["ExecutionIndices","The state of the subscriber keeping track of the transactions that have already been executed. It ensures we do not process twice the same transaction despite crash-recovery."],["Executor","A client subscribing to the consensus output and executing every transaction."],["SingleExecutor","Executor that feeds transactions one by one to the execution state."]],"trait":[["BatchExecutionState","Execution state that gets whole certificates and the corresponding batches for execution. It is responsible for deduplication in case the same certificate is re-delivered after a crash."],["ExecutionState",""],["ExecutionStateError","Trait to separate execution errors in two categories: (i) errors caused by a bad client, (ii) errors caused by a fault in the authority."],["SingleExecutionState","Execution state that executes a single transaction at a time."]],"type":[["ExecutorOutput","The output of the executor."],["SerializedTransaction","Convenience type representing a serialized transaction."],["SerializedTransactionDigest","Convenience type representing a serialized transaction digest."],["SubscriberResult",""]]};