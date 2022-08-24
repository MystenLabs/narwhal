// Copyright (c) The Diem Core Contributors Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

//! A bounded tokio [`Handle`]. Only a bounded number of tasks can run
//! concurrently when spawned through this executor, defined by the initial
//! `capacity`.
use futures::{
    future::{join, BoxFuture, Future},
    FutureExt, StreamExt, TryFuture, TryFutureExt,
};
use std::{ops::Deref, pin::Pin, process::Output, sync::Arc, time::Duration};
use tokio::{
    runtime::Handle,
    sync::{
        mpsc,
        oneshot::{self, error::RecvError},
        OwnedSemaphorePermit, Semaphore,
    },
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
        // TODO(erwan): find workaround boxing inner result... sadface
    ) -> impl Future<Output = Result<BoxedResult, RecvError>> + Send
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = BoxedResult> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let tx_retry_manager = self.tx_retry_manager.clone();
        let backoff = Backoff {};

        let (tx_callback, rx_callback) = oneshot::channel::<BoxedResult>();

        let mut boxed_factory: Box<FutureFactory> = Box::new(move || Box::pin(f()));

        let task = boxed_factory();
        let job = (boxed_factory, backoff, tx_callback);
        let task = task.then(|result| async move {
            let (boxed_factory, backoff, tx_callback) = job;
            match result {
                Ok(v) => {
                    tx_callback.send(Ok(v.into()));
                }
                Err(e) => {
                    let backoff = backoff.tick();
                    if backoff.keep_trying() {
                        tx_retry_manager
                            .send((boxed_factory, backoff, tx_callback))
                            .await;
                    } else {
                        tx_callback.send(Err(e.into()));
                    }
                }
            }
        });

        async move { 
            // TODO(erwan): polling a receiver on a closed/dropped oneshot channel returns `RecvError`
            //              should we wrap this error into a BoundedExecutorError::InternalError(E)
            rx_callback.await
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
                 // TODO(erwan): replace with macro wrapping a try_join
                (job_ready, permit) = join(self.execution_queue.next(), InternalBoundedExecutor::acquire_permit(self.semaphore.clone()))=> {
                         let tx_scheduler = self.tx_new_jobs.clone();
                         let (mut task_factory, backoff, tx_callback) = job_ready.unwrap().into_inner();

                         // build task and spawn into
                         let retry_task = task_factory().then(|result| async move {
                             match result {
                                 Ok(v) => { tx_callback.send(Ok(v)); },
                                 Err(e) => {
                                     let backoff = backoff.tick();
                                     if backoff.keep_trying() {
                                        let job = (task_factory, backoff, tx_callback);
                                        tx_scheduler.send(job).await;
                                     } else {
                                         tx_callback.send(Err(e.into()));
                                     }
                                 }
                             }
                         });

                         executor.spawn_with_permit(retry_task, permit).await;
                 },
                 // Pull jobs that need scheduling, and schedule them for execution.
                 new_job = self.rx_new_jobs.recv() => {
                           let (task_factory, backoff, tx_callback) = new_job.unwrap();
                           let cooldown_time = backoff.cooldown_time();
                           self.execution_queue.insert((task_factory, backoff, tx_callback), cooldown_time);
                        },
            }
        }
    }
}

// TODO(erwan): compare backoff crates
#[derive(Debug)]
pub struct Backoff {
    // tick -> monotonic clock of retries
    // cooldown_time -> how long we want to wait before next tick (= retry)
    // keep_trying -> does the policy embed a max number of retries? if so, should we surface
    // results even if they are errors?
}

impl Backoff {
    pub fn tick(self) -> Self {
        self
    }

    pub fn cooldown_time(&self) -> Duration {
        Duration::from_secs(1)
    }

    pub fn keep_trying(&self) -> bool {
        return true;
    }
}

/// Boxed Result
pub type BoxedResult = Result<Box<dyn Send>, Box<dyn Send>>;

/// A `Future` factory that produces boxed futures ready to be ran, and implementing Unpin.
pub type FutureFactory =
    dyn FnMut() -> BoxFuture<'static, Result<Box<dyn Send>, Box<dyn Send>>> + Send;

pub type Job = (
    Box<FutureFactory>,
    Backoff,
    oneshot::Sender<Result<Box<dyn Send>, Box<dyn Send>>>,
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
