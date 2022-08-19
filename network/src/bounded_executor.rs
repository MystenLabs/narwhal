// Copyright (c) The Diem Core Contributors Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

//! A bounded tokio [`Handle`]. Only a bounded number of tasks can run
//! concurrently when spawned through this executor, defined by the initial
//! `capacity`.
use futures::{
    future::{join, BoxFuture, Future},
    FutureExt, StreamExt,
};
use std::{ops::Deref, sync::Arc};
use tokio::{
    runtime::Handle,
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};
use tokio_util::time::DelayQueue;
use tracing::debug;

use thiserror::Error;

use crate::{try_fut_and_acquire, CancelOnDropHandler};

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

pub struct BoundedExecutor {
    inner: Arc<InternalBoundedExecutor>,
    scheduler_handle: CancelOnDropHandler<()>,
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
            scheduler_handle: CancelOnDropHandler(scheduler_handle),
        }
    }
}

impl Deref for BoundedExecutor {
    type Target = InternalBoundedExecutor;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct InternalBoundedExecutor {
    semaphore: Arc<Semaphore>,
    executor: Handle,
    // Outbound channel to the retry manager task
    tx_retry_manager: Arc<mpsc::Sender<Job>>,
}

impl InternalBoundedExecutor {
    /// Create a new `BoundedExecutor` from an existing tokio [`Handle`]
    /// with a maximum concurrent task capacity of `capacity`.
    pub fn new(
        executor: Handle,
        semaphore: Arc<Semaphore>,
        tx_retry_manager: Arc<mpsc::Sender<Job>>,
    ) -> Self {
        Self {
            semaphore,
            executor,
            tx_retry_manager,
        }
    }

    pub fn semaphore(&self) -> Arc<Semaphore> {
        self.semaphore.clone()
    }

    // Acquires a permit with the semaphore, first gracefully,
    // then queuing after logging that we're out of capacity.
    pub async fn acquire_permit(semaphore: Arc<Semaphore>) -> OwnedSemaphorePermit {
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

    // Returns a [`Future`] that complies with the `BoundedExecutor`. Once launched,
    // will block if the executor is at capacity, until one of the other spawned
    // futures completes.
    async fn run_on_semaphore<F>(semaphore: Arc<Semaphore>, f: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let permit = Self::acquire_permit(semaphore.clone()).await;
        Self::with_permit(f, permit).await
    }

    /// Unconditionally spawns a task driving retries of a [`Future`] on the `BoundedExecutor`.
    /// This [`Future`] will be executed in the form of attempts, one after the other, run on
    /// our bounded executor, each according to the provided [`crate::RetryConfig`].
    ///
    /// Each attempt is async and will block if the executor is at capacity until
    /// one of the other attempts completes. In case the attempt completes with an error,
    /// the driver completes a backoff (according to the retry configuration) without holding
    /// a permit, before, queueing an attempt on the executor again.
    ///
    /// This function returns a [`JoinHandle`] that the caller can `.await` on for
    /// the results of the overall retry driver.
    ///
    /// TODO: this still spawns one task, unconditionally, per call.
    /// We would instead like to have one central task that drives all retries
    /// for the whole executor.
    #[must_use]
    pub(crate) fn spawn_with_retries<F, Fut, T, E>(
        &self,
        _retry_config: crate::RetryConfig,
        mut f: F,
    ) -> JoinHandle<Result<T, E>>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, backoff::Error<E>>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let task = f();
        let tx_retry_manager = self.tx_retry_manager.clone();
        let backoff = Backoff {};

        // TODO(erwan): Closure helpers. Focusing on the flow of execution before simplifying it.
        // 1. retry on failure
        // 2. once engaged in the retry pipeline, use thin `OpaqueResult` enum
        // 3. transmit job to scheduler for retry.
        // 4. need to figure out how to surface results:
        //      -> a task polling a oneshot channel?
        //      -> a custom Future that polls a hashtable: JobId -> ComputationResult (Failed |
        //      Retrying | Done(Result))
        // fix rust 2018 ambiguities
        let mut task = f();
        todo!()
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

struct RetryManager {
    /// Bounded execution semaphore.
    semaphore: Arc<Semaphore>,
    /// A queue of jobs eligible for execution.
    execution_queue: DelayQueue<Job>,
    /// A sink of failed jobs going to the scheduler.
    tx_new_jobs: Arc<mpsc::Sender<Job>>,
    /// A stream of failed jobs that need scheduling.
    rx_new_jobs: mpsc::Receiver<Job>,
}

impl RetryManager {
    pub fn new(capacity: usize, semaphore: Arc<Semaphore>) -> Self {
        let (tx, rx) = mpsc::channel(1);
        Self {
            // TODO(erwan): scaling factor for capacity?
            execution_queue: DelayQueue::with_capacity(capacity),
            tx_new_jobs: Arc::new(tx),
            rx_new_jobs: rx,
            semaphore,
        }
    }

    pub fn inbound_channel(&self) -> Arc<mpsc::Sender<Job>> {
        self.tx_new_jobs.clone()
    }

    /// A scheduler which manages spawning retry tasks on the `BoundedExecutor`.
    async fn run_scheduler(mut self, executor: Arc<InternalBoundedExecutor>) {
        loop {
            tokio::select! {
                 // Pull jobs that are ready for execution, and forward them to the executor if
                 // resources can be acquired (semaphore permit).
                 // TODO(erwan): gating branch.
                job_ready = self.execution_queue.next() => {
                         let (mut task_factory, backoff) = job_ready.unwrap().into_inner();
                         let tx_scheduler = self.tx_new_jobs.clone();

                         let task = task_factory();
                         let retry_task = task.map(move |result| async move {
                             let _ = (&task_factory, &backoff, &tx_scheduler); // ugly
                               match result {
                                   OpaqueResult::Ok => (),
                                   OpaqueResult::Err => {
                                       let backoff = todo!(); // tick
                                       let job = (task_factory, backoff);
                                       tx_scheduler.send(job).await;
                                  }
                               }
                                  result
                         }).flatten();  // We can do a simple closure too


                         // TODO(erwan): see discussion in issue #567 -  a blocking spawn on the
                         // semaphore might be acceptable provided that we obtain a permit to run
                         // prior to branching.
                         // TODO(erwan): with semaphore logic.
                         let permit = InternalBoundedExecutor::acquire_permit(self.semaphore.clone()).await;
                         executor.spawn_with_permit(retry_task, permit).await;
                 },
                 // Pull jobs that need scheduling, and schedule them for execution.
                 new_job = self.rx_new_jobs.recv() => {
                           let (f, backoff) = new_job.unwrap();
                           let cooldown_time = todo!();
                           self.execution_queue.insert((f, backoff), cooldown_time);
                        },
            }
        }
    }
}

// TODO(erwan): compare backoff crates
pub struct Backoff {}

/// The output of a faillible computation, similar to a `Result<T, E>` but using unit variants.
// Subsuming a `OpaqueResult` for a `Result` enables us to save a heap allocation
// when submitting computations to the scheduler; might not work for surfacing results
pub enum OpaqueResult {
    Ok,
    Err,
}

/// A `Future` task that can fail and return an opaque `Err`.
pub type FaillibleFuture = BoxFuture<'static, OpaqueResult>;

/// A job to schedule or execute.
/// It wraps a `FaillibleFuture` factory and a `Backoff` context specific to that computation.
// TODO(erwan): investigate #![feature(type_alias_impl_trait)]
pub type Job = (
    Box<dyn FnMut() -> FaillibleFuture + Send + 'static>,
    Backoff,
);

#[cfg(test)]
mod test {
    use crate::RetryConfig;

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
    fn test_spawn_with_semaphore() {
        // beware: the timeout is here to witness a failure rather than a hung test in case the
        // executor does not work correctly.
        panic_after(Duration::from_secs(10), || {
            let rt = Runtime::new().unwrap();
            let executor = rt.handle().clone();
            let executor = BoundedExecutor::new(1, executor);

            let infinite_retry_config = RetryConfig {
                // Retry for forever
                retrying_max_elapsed_time: None,
                ..Default::default()
            };

            // we can queue this future with infinite retries
            let handle_infinite_fails =
                executor.spawn_with_retries(infinite_retry_config, always_failing);

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

    async fn always_failing() -> Result<(), backoff::Error<eyre::Report>> {
        Err(Into::into(eyre::eyre!("oops")))
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
}
