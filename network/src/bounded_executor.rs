// Copyright (c) The Diem Core Contributors
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

//! A bounded tokio [`Handle`]. Only a bounded number of tasks can run
//! concurrently when spawned through this executor, defined by the initial
//! `capacity`.
use crate::CancelOnDropHandler;
use exponential_backoff::Backoff;
use futures::{
    future::{BoxFuture, Future},
    FutureExt, StreamExt,
};
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{self, Sender},
        OwnedSemaphorePermit, Semaphore,
    },
    task::JoinHandle,
};
use tokio_util::time::DelayQueue;
use tracing::{debug, log::error, warn};

#[derive(Error)]
pub enum BoundedExecutionError<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    #[error("Concurrent execution limit reached")]
    Full(F),
}

impl<F> std::fmt::Debug for BoundedExecutionError<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Elide the future to let this be unwrapped
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full(_f) => f.debug_tuple("Full").finish(),
        }
    }
}

/// A bounded tokio [`Handle`]. Only a bounded number of tasks can run
/// concurrently when spawned through this executor, defined by the initial
/// `capacity`.
pub struct BoundedExecutor {
    inner: Arc<InternalBoundedExecutor>,
    _scheduler_handle: CancelOnDropHandler<()>,
}

impl BoundedExecutor {
    pub fn new(capacity: usize, executor: Handle) -> Self {
        let semaphore = Arc::new(Semaphore::new(capacity));
        let retry_manager = RetryManager::new(capacity, semaphore.clone());
        let tx_retry_manager = retry_manager.inbound_channel();

        let bounded_executor =
            InternalBoundedExecutor::new(executor.clone(), semaphore, tx_retry_manager);

        let bounded_executor = Arc::new(bounded_executor);

        // Consumes the `RetryManager` into a scheduling task
        let scheduler_handle =
            executor.spawn(retry_manager.run_scheduler(bounded_executor.clone()));

        Self {
            inner: bounded_executor,
            _scheduler_handle: CancelOnDropHandler(scheduler_handle),
        }
    }
}

impl AsRef<InternalBoundedExecutor> for BoundedExecutor {
    fn as_ref(&self) -> &InternalBoundedExecutor {
        self.inner.as_ref()
    }
}

///   BoundedExecutor
/// ┌───────────────────────────────────────────────────────────────────────────────┬────────────┐
/// │                                                                               │            │
/// │                                      tx_new_jobs                              │ DelayQueue │
/// │                   ┌────────────────────────────────────────────────────┐      │(exec_queue)│
/// │                   │                                                    │      │            │
/// │                   │                                                    │      │            │
/// │                   │                                                    │  ┌──►│  insert    │
/// │    ┌──────────────┴────────────┐    ┌─────────────────────┐            │  │   │            │
/// │    │                           │    │                     │            │  │   │            │
/// │    │                           │    │                     │            │  │   │            │
/// │    │ .InternalBoundedExecutor  │    │     RetryManager    │            ▼  │   │            │
/// │    │                           │    │                     │               │   │            │
/// │    │ ┌──────────┬────────────┐ │    │                     │     ┌─────────┴─┐ │            │
/// │    │ │          │            │ │    │                     │     │ scheduler │ │            │
/// │    │ │  Handle  │  Semaphore │ │    │                     │ run │           │ │            │
/// │    │ │ (runtime)│            │ │    │                     ├────►│           │ │            │
/// │    │ │          │            │ │    │                     │     │           │ │            │
/// │    │ │          │            │ │    │                     │     │           │ │            │
/// │    │ │          │            │ │    │                     │     └──────┬────┘ │            │
/// │    │ │          │            │ │    │                     │            │  ▲   │            │
/// │    │ │          │            │ │    │                     │            │  │   │            │
/// │    │ │          │            │ │    │                     │            │  │   │            │
/// │    │ └──────────┴────────────┘ │    │                     │            │  │   │            │
/// │    │                           │    │                     │            │  │   │            │
/// │    └───────────────────────────┘    └─────────────────────┘            │  └───┤ job_ready  │
/// │                  ▲                                                     │      │            │
/// │                  └─────────────────────────────────────────────────────┘      │            │
/// │                                    spawn                                      │            │
/// │                                                                               │            │
/// └───────────────────────────────────────────────────────────────────────────────┴────────────
pub struct InternalBoundedExecutor {
    /// Execution bound, shared by [`InternalBoundedExecutor`] and the [`RetryManager`].
    semaphore: Arc<Semaphore>,
    /// Handle to the tokio runtime.
    executor: Handle,
    /// Outbound channel to the retry scheduler task.
    /// Tasks equipped with a retry strategy hold a shared reference to this [`mpsc::Sender`].
    tx_retry_manager: Arc<mpsc::Sender<RetryJob>>,
}

impl InternalBoundedExecutor {
    /// Creates a new `InternalBoundedExecutor` from an existing tokio [`Handle`]
    /// with a maximum concurrent task capacity of `capacity`, a shared reference
    /// to a semaphore, and a outbound stream to the [`RetryManager::run_scheduler`]
    /// task.
    pub fn new(
        executor: Handle,
        semaphore: Arc<Semaphore>,
        tx_retry_manager: Arc<mpsc::Sender<RetryJob>>,
    ) -> Self {
        Self {
            semaphore,
            executor,
            tx_retry_manager,
        }
    }

    /// Acquires a [`OwnedSemaphorePermit`] with the semaphore, first gracefully,
    /// then queuing after logging that we're out of capacity.
    pub(crate) async fn acquire_permit(semaphore: Arc<Semaphore>) -> OwnedSemaphorePermit {
        match semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                debug!("concurrent task limit reached, waiting...");
                semaphore.acquire_owned().await.unwrap()
            }
        }
    }

    /// Returns the executor available capacity for running tasks.
    pub fn available_capacity(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Spawn a [`Future`] on the `BoundedExecutor`. This function is async and
    /// will block if the executor is at capacity until one of the other spawned
    /// futures completes. This function returns a [`JoinHandle`] that the caller
    /// can `.await` on for the results of the [`Future`].
    pub async fn spawn<F>(&self, f: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let permit = Self::acquire_permit(self.semaphore.clone()).await;

        self.spawn_with_permit(f, permit)
    }

    /// Try to spawn a [`Future`] on the `BoundedExecutor`. If the `BoundedExecutor`
    /// is at capacity, this will return an `Err(F)`, passing back the future the
    /// caller attempted to spawn. Otherwise, this will spawn the future on the
    /// executor and send back a [`JoinHandle`] that the caller can `.await` on
    /// for the results of the [`Future`].
    pub fn try_spawn<F>(&self, f: F) -> Result<JoinHandle<F::Output>, BoundedExecutionError<F>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => Ok(self.spawn_with_permit(f, permit)),
            Err(_) => Err(BoundedExecutionError::Full(f)),
        }
    }

    #[must_use]
    fn spawn_with_permit<F>(
        &self,
        f: F,
        spawn_permit: OwnedSemaphorePermit,
    ) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        // Release the permit back to the semaphore when this task completes.
        let f = Self::with_permit(f, spawn_permit);
        self.executor.spawn(f)
    }

    /// Returns a future equipped with retry logic.
    fn with_retries<F, Fut>(
        &self,
        mut factory: F,
        backoff: Backoff,
    ) -> impl Future<Output = Result<(), RetryError>>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), RetryError>> + Send + 'static,
    {
        let mut boxed_factory: Box<FutureFactory> = Box::new(move || Box::pin(factory()));

        let task = boxed_factory();
        let job = RetryJob::new(boxed_factory, backoff);
        let tx_scheduler = self.tx_retry_manager.clone();
        task.then(
            |result| async move { RetryManager::retry_logic(result, job, tx_scheduler).await },
        )
    }

    /// Asynchronously spawns a [`Future`] equipped with a retry strategy, and
    /// returns a handle to the first attempt at executing the task. This function
    /// is async and will block if the executor is at capacity until one of the
    /// other spawned futures completes. If retries are needed, the future is
    /// handed to a [`RetryManager`] which will schedule future execution within
    /// the specified [`Backoff`] policy.
    pub async fn spawn_with_retries<F, Fut>(
        &self,
        backoff: Backoff,
        f: F,
    ) -> JoinHandle<Result<(), RetryError>>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), RetryError>> + Send + 'static,
    {
        let retry_task = self.with_retries(f, backoff);
        self.spawn(retry_task).await
    }

    /// Asynchronously spawns a task equipped with a retry strategy, and use an
    /// adapater to map the retry result (`Result<(), RetryError>`) to `Result<T, E>`
    /// TODO(erwan): conversion trait bounds could be cleaner once API matures.
    pub async fn spawn_with_retries_and_adapter<F, Fut, A, T, E>(
        &self,
        backoff: Backoff,
        f: F,
        adapter: A,
    ) -> JoinHandle<Result<T, E>>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), RetryError>> + Send + 'static,
        A: FnOnce(Result<(), RetryError>) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let retry_task = self
            .with_retries(f, backoff)
            .then(|result| async move { adapter(result) });
        self.spawn(retry_task).await
    }

    /// Tries to spawn a [`Future`] equipped with a retry strategy, and return
    /// early if the executor is full.
    pub fn try_spawn_with_retries<F, Fut>(
        &self,
        factory: F,
        backoff: Backoff,
    ) -> Result<
        JoinHandle<Result<(), RetryError>>,
        BoundedExecutionError<impl Future<Output = Result<(), RetryError>>>,
    >
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), RetryError>> + Send + 'static,
    {
        let retry_task = self.with_retries(factory, backoff);

        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => Ok(self.spawn_with_permit(retry_task, permit)),
            Err(_) => Err(BoundedExecutionError::Full(retry_task)),
        }
    }

    // Equips a future with a final step that drops the held semaphore permit
    async fn with_permit<F>(f: F, spawn_permit: OwnedSemaphorePermit) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        f.map(|ret| {
            drop(spawn_permit);
            ret
        })
        .await
    }
}

/// Context consumed by the [`run_scheduler`] task.
struct RetryManager {
    /// Bounded execution semaphore.
    semaphore: Arc<Semaphore>,
    /// A queue of jobs eligible for execution.
    execution_queue: DelayQueue<RetryJob>,
    /// A sink of failed jobs going to the scheduler.
    tx_new_jobs: Arc<mpsc::Sender<RetryJob>>,
    /// A stream of failed jobs that need scheduling.
    rx_new_jobs: mpsc::Receiver<RetryJob>,
}

impl RetryManager {
    pub fn new(capacity: usize, semaphore: Arc<Semaphore>) -> Self {
        let (tx, rx) = mpsc::channel(crate::RETRY_CHANNEL_BUFFER);
        Self {
            execution_queue: DelayQueue::with_capacity(capacity * crate::RETRY_QUEUE_SCALE_FACTOR),
            tx_new_jobs: Arc::new(tx),
            rx_new_jobs: rx,
            semaphore,
        }
    }

    /// Returns an [`Arc`] reference to the channel consumed by `run_scheduler`.
    pub fn inbound_channel(&self) -> Arc<mpsc::Sender<RetryJob>> {
        self.tx_new_jobs.clone()
    }

    /// Long-running task scheduling retry attempts on the `BoundedExecutor`.
    async fn run_scheduler(mut self, executor: Arc<InternalBoundedExecutor>) {
        loop {
            tokio::select! {
                // Pull jobs that are ready for execution, and forward them to the executor if
                // resources can be acquired.
                Some((job_ready, permit)) = async {
                    // Unlike most [`Stream`] implementations, `DelayQueue::poll_expired` returning
                    // `Poll::Ready(None)` does not signal a "closed" stream. Instead, that value
                    // is used to indicate an empty queue. If an item is inserted later, polling
                    // the queue will return `Poll::Ready(Some<T>)`.
                    if let Some(job_ready) = self.execution_queue.next().await {
                        let permit = InternalBoundedExecutor::acquire_permit(self.semaphore.clone()).await;
                        Some((job_ready, permit))
                    } else {
                        // The execution queue is empty, we avoid polling for a semaphore permit
                        // and return early.
                        None
                    }
                } => {
                    let mut job = job_ready.into_inner();
                    let tx_scheduler = self.tx_new_jobs.clone();
                    let task = (job.task_factory)();
                    let retry_task = task.then(|result| async move { RetryManager::retry_logic(result, job, tx_scheduler).await });
                    if let Err(e) = executor.spawn_with_permit(retry_task, permit).await {
                        error!("run_scheduler: inbound channel is closed; tasks won't be retried ({e})")
                    }
                },
                // Pull new jobs that need scheduling, and insert them in the execution queue
                // if their backoff policy allows it.
                new_job = self.rx_new_jobs.recv() => {
                    let job = new_job.unwrap();
                    let cooldown_time = job.cooldown_time();
                    if let Some(duration) = cooldown_time {
                        self.execution_queue.insert(job, duration);
                    } else {
                        warn!("run_scheduler: expired jobs are being queued, this should not happen");
                    }
                }
            }
        }
    }

    /// or forward that result.
    async fn retry_logic(
        result: Result<(), RetryError>,
        mut job: RetryJob,
        tx_scheduler: Arc<Sender<RetryJob>>,
    ) -> Result<(), RetryError> {
        match result {
            Ok(_) => Ok(()),
            Err(e) => match e {
                RetryError::Transient => {
                    job.increase_retry_counter();
                    let cooldown_time = job.cooldown_time();
                    if cooldown_time.is_some() {
                        match tx_scheduler.send(job).await {
                            Ok(_) => Err(RetryError::Transient),
                            Err(e) => {
                                error!("retry_job: failed to submit task to scheduler! ({e})");
                                Err(RetryError::Permanent)
                            }
                        }
                    } else {
                        Err(RetryError::Expired)
                    }
                }
                permanent @ RetryError::Permanent => Err(permanent),
                RetryError::Expired => unreachable!("expired jobs are not executed"),
            },
        }
    }
}

#[derive(Debug, Clone, Error)]
pub enum RetryError {
    /// The task has encountered a recoverable error, retrying continues if allowed by the policy.
    #[error("Transient error encountered.")]
    Transient,
    /// The task has encountered an unrecoverable error, retrying stops.
    #[error("Permanent error encountered, stopping retries.")]
    Permanent,
    /// The task has failed too many times, retrying stops.
    #[error("Maximum number of retries reached, stopping retries.")]
    Expired,
}

/// A `Future` factory that produces boxed futures ready to be ran, and implementing Unpin.
pub type FutureFactory = dyn FnMut() -> BoxFuture<'static, Result<(), RetryError>> + Send;

/// A computation to be retried.
pub struct RetryJob {
    /// A factory producing fresh futures of the task to be retried.
    pub task_factory: Box<FutureFactory>,
    /// Retry policy specifying the backoff strategy as well as the max number of attempts.
    backoff: Backoff,
    /// Monotonic counter tracking the number of execution attempts.
    retry_counter: u32,
}

impl RetryJob {
    /// Creates a new `RetryJob` for submission to the scheduler task.
    ///
    /// The struct wraps a task factory that is used to create fresh futures to poll, a backoff
    /// policy specifying the duration between each call, and the maximum number of retries.
    pub fn new(task_factory: Box<FutureFactory>, backoff: Backoff) -> Self {
        Self {
            task_factory,
            backoff,
            retry_counter: 0,
        }
    }

    pub fn increase_retry_counter(&mut self) {
        self.retry_counter += 1;
    }

    /// The minimum duration this job should wait before being retried.
    /// Returns `None` if the job has exceeded its maximum retry policy.
    pub fn cooldown_time(&self) -> Option<Duration> {
        self.backoff.next(self.retry_counter)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::{channel::oneshot, executor::block_on, future::Future, FutureExt};
    use std::{
        sync::{
            atomic::{AtomicU32, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };
    use tokio::{runtime::Runtime, time::sleep};

    #[test]
    fn try_spawn_panicking() {
        let rt = Runtime::new().unwrap();
        let executor = rt.handle().clone();
        let executor = BoundedExecutor::new(1, executor);

        // executor has a free slot, spawn should succeed
        let fpanic = executor.try_spawn(panicking()).unwrap();
        // this would return a JoinError::panic
        block_on(fpanic).unwrap_err();

        let (tx1, rx1) = oneshot::channel();
        // the executor should not be full, because the permit for the panicking task should drop at unwinding
        let f1 = executor.try_spawn(rx1).unwrap();

        // cleanup
        tx1.send(()).unwrap();
        block_on(f1).unwrap().unwrap();
    }

    async fn panicking() {
        panic!();
    }

    #[test]
    fn try_spawn() {
        let rt = Runtime::new().unwrap();
        let executor = rt.handle().clone();
        let executor = BoundedExecutor::new(1, executor);

        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        // executor has a free slot, spawn should succeed
        assert_eq!(executor.available_capacity(), 1);

        let f1 = executor.try_spawn(rx1).unwrap();

        // executor is full, try_spawn should return err and give back the task
        // we attempted to spawn
        let BoundedExecutionError::Full(rx2) = executor.try_spawn(rx2).unwrap_err();

        // currently running tasks is updated
        assert_eq!(executor.available_capacity(), 0);

        // complete f1 future, should open a free slot in executor

        tx1.send(()).unwrap();
        block_on(f1).unwrap().unwrap();

        // should successfully spawn a new task now that the first is complete
        let f2 = executor.try_spawn(rx2).unwrap();

        // cleanup

        tx2.send(()).unwrap();
        block_on(f2).unwrap().unwrap();

        //ensure current running goes back to one
        assert_eq!(executor.available_capacity(), 1);
    }

    // ensure tasks spawned with retries do not hog the semaphore
    #[test]
    fn spawn_with_semaphore() {
        // beware: the timeout is here to witness a failure rather than a hung test in case the
        // executor does not work correctly.
        panic_after(Duration::from_secs(10), || {
            let rt = Runtime::new().unwrap();
            let executor = rt.handle().clone();
            let executor = BoundedExecutor::new(1, executor);

            let infinite_backoff = Backoff::new(u32::MAX, Duration::ZERO, None);
            let handle = executor.spawn_with_retries(infinite_backoff, always_failing);
            // we can queue this future with infinite retries
            let handle_infinite_fails = block_on(handle);

            // check we can still enqueue another successful task
            let (tx1, rx1) = oneshot::channel();
            let f1 = block_on(executor.spawn(rx1));

            // complete f1 future, should open a free slot in executor
            tx1.send(()).unwrap();
            block_on(f1).unwrap().unwrap();

            // cleanup
            handle_infinite_fails.abort();
        })
    }

    #[test]
    fn spawn_with_retries() {
        enum WitnessMode {
            AlwaysFailing,
            CrashOnAttempt(u32),
            ResolveOnAttempt(u32),
        }

        enum WitnessError {
            Transient,
            Permanent,
        }

        struct RetryWitness {
            mode: WitnessMode,
            counter: u32,
        }

        impl RetryWitness {
            fn new(mode: WitnessMode) -> Self {
                Self { mode, counter: 0 }
            }

            async fn operation(&mut self) -> Result<(), WitnessError> {
                self.counter += 1;
                match self.mode {
                    WitnessMode::CrashOnAttempt(n) => {
                        if self.counter > n {
                            Err(WitnessError::Permanent)
                        } else {
                            Err(WitnessError::Transient)
                        }
                    }
                    WitnessMode::ResolveOnAttempt(n) => {
                        if self.counter > n {
                            Ok(())
                        } else {
                            Err(WitnessError::Transient)
                        }
                    }
                    WitnessMode::AlwaysFailing => Err(WitnessError::Transient),
                }
            }

            fn state(&self) -> u32 {
                self.counter
            }
        }

        /*
         * We test four scenarios:
         *
         * #1: backoff policy specifies no max retries, and the witness succeeds on first attempt.
         * #2: backoff policy specifies 5 max retries, and the witness always fails (transient)
         * #3: backoff policy specifies 5 max retries, and the witness hard fails on attempt 2.
         * #4: backoff policy with no max retries, and the witness succeeds after 10 attempts.
         */

        use std::cell::RefCell;
        use tokio::sync::Mutex;

        fn test_executor_with_witness(
            executor: BoundedExecutor,
            witness: Arc<Mutex<RefCell<RetryWitness>>>,
            backoff: Backoff,
        ) -> Result<(), RetryError> {
            let task = move || {
                let witness = witness.clone();
                async move {
                    let mut w = witness.lock().await;
                    match w.get_mut().operation().await {
                        Ok(_) => Ok(()),
                        Err(WitnessError::Permanent) => Err(RetryError::Permanent),
                        Err(WitnessError::Transient) => Err(RetryError::Transient),
                    }
                }
            };

            let handle = block_on(executor.spawn_with_retries(backoff, task));

            let first_try = block_on(handle).unwrap();

            thread::sleep(Duration::from_millis(500));
            first_try
        }

        // Scenario #1: no max retries, task succeeds on first attempt.

        panic_after(Duration::from_secs(5), || {
            let rt = Runtime::new().unwrap();
            let executor = rt.handle().clone();
            let executor = BoundedExecutor::new(1, executor);

            let max_retry_attempts = u32::MAX;

            let mut infinite_retries = Backoff::new(max_retry_attempts, Duration::ZERO, None);
            infinite_retries.set_factor(0);

            let witness = RetryWitness::new(WitnessMode::ResolveOnAttempt(0));
            let witness = Arc::new(Mutex::new(RefCell::new(witness)));

            let result = test_executor_with_witness(executor, witness.clone(), infinite_retries);

            let number_attempts = block_on(witness.lock()).borrow().state();
            assert_eq!(number_attempts, 1);
            assert!(matches!(result.unwrap(), ()));
        });

        // Scenario #2: 5 max retries, with an ever failing witness (transient).

        panic_after(Duration::from_secs(5), || {
            let rt = Runtime::new().unwrap();
            let executor = rt.handle().clone();
            let executor = BoundedExecutor::new(1, executor);

            let max_retry_attempts = 5u32;

            let mut max_five_retries = Backoff::new(max_retry_attempts, Duration::ZERO, None);
            max_five_retries.set_factor(0);

            let witness = RetryWitness::new(WitnessMode::AlwaysFailing);
            let witness = Arc::new(Mutex::new(RefCell::new(witness)));

            let result = test_executor_with_witness(executor, witness.clone(), max_five_retries);

            let number_attempts = block_on(witness.lock()).borrow().state();
            assert_eq!(number_attempts, max_retry_attempts + 1);
            assert!(matches!(result.unwrap_err(), RetryError::Transient));
        });

        // Scenario #3: 5 max retries, witness crashes on the secon attempt.

        panic_after(Duration::from_secs(5), || {
            let rt = Runtime::new().unwrap();
            let executor = rt.handle().clone();
            let executor = BoundedExecutor::new(1, executor);

            let max_retry_attempts = 5u32;

            let mut max_five_retries = Backoff::new(max_retry_attempts, Duration::ZERO, None);
            max_five_retries.set_factor(0);

            let witness = RetryWitness::new(WitnessMode::CrashOnAttempt(2));
            let witness = Arc::new(Mutex::new(RefCell::new(witness)));

            let _ = test_executor_with_witness(executor, witness.clone(), max_five_retries);

            let number_attempts = block_on(witness.lock()).borrow().state();
            assert_eq!(number_attempts, 3);
        });

        // Scenario #4: no max retries, witness succeeds after 10 attempts.

        panic_after(Duration::from_secs(5), || {
            let rt = Runtime::new().unwrap();
            let executor = rt.handle().clone();
            let executor = BoundedExecutor::new(1, executor);

            let max_retry_attempts = u32::MAX;

            let mut infinite_retries = Backoff::new(max_retry_attempts, Duration::ZERO, None);
            infinite_retries.set_factor(0);

            let witness = RetryWitness::new(WitnessMode::ResolveOnAttempt(10));
            let witness = Arc::new(Mutex::new(RefCell::new(witness)));

            let _ = test_executor_with_witness(executor, witness.clone(), infinite_retries);

            let number_attempts = block_on(witness.lock()).borrow().state();
            assert_eq!(number_attempts, 10 + 1);
        });
    }

    // spawn NUM_TASKS futures on a BoundedExecutor, ensuring that no more than
    // MAX_WORKERS ever enter the critical section.
    #[test]
    fn concurrent_bounded_executor() {
        const MAX_WORKERS: u32 = 20;
        const NUM_TASKS: u32 = 1000;
        static WORKERS: AtomicU32 = AtomicU32::new(0);
        static COMPLETED_TASKS: AtomicU32 = AtomicU32::new(0);

        let rt = Runtime::new().unwrap();
        let executor = rt.handle().clone();
        let executor = BoundedExecutor::new(MAX_WORKERS as usize, executor);

        for _ in 0..NUM_TASKS {
            block_on(executor.spawn(async move {
                // acquired permit, there should only ever be MAX_WORKERS in this
                // critical section

                let prev_workers = WORKERS.fetch_add(1, Ordering::SeqCst);
                assert!(prev_workers < MAX_WORKERS);

                // yield back to the tokio scheduler
                yield_task().await;

                let prev_workers = WORKERS.fetch_sub(1, Ordering::SeqCst);
                assert!(prev_workers > 0 && prev_workers <= MAX_WORKERS);

                COMPLETED_TASKS.fetch_add(1, Ordering::Relaxed);
            }));
        }

        // spin until completed
        loop {
            let completed = COMPLETED_TASKS.load(Ordering::Relaxed);
            if completed == NUM_TASKS {
                break;
            } else {
                std::hint::spin_loop()
            }
        }
    }

    fn yield_task() -> impl Future<Output = ()> {
        sleep(Duration::from_millis(1)).map(|_| ())
    }

    async fn always_failing() -> Result<(), RetryError> {
        Err(RetryError::Transient)
    }

    fn panic_after<T, F>(d: Duration, f: F) -> T
    where
        T: Send + 'static,
        F: FnOnce() -> T,
        F: Send + 'static,
    {
        let (done_tx, done_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let val = f();
            done_tx.send(()).expect("Unable to send completion signal");
            val
        });

        match done_rx.recv_timeout(d) {
            Ok(_) => handle.join().expect("Thread panicked"),
            Err(_) => panic!("Thread took too long"),
        }
    }
}
