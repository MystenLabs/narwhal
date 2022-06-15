use async_shutdown::Shutdown;
use futures::Future;
use parking_lot::Mutex;
use pin_project::{pin_project, pinned_drop};
use std::collections::BTreeMap;
use std::{pin::Pin, sync::Arc};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

#[derive(Clone, PartialEq)]
enum TaskTrackerState {
    Running,
    ShutdownStarted,
    ShutdownComplete,
}

#[derive(Clone)]
struct Task {
    task_id: u64,
    tag: Option<String>,
    shutdown: Shutdown,
}

/// The type of error that is returned from tasks in this module
#[allow(dead_code)]
pub type Error = tokio::sync::oneshot::error::TryRecvError;

#[pin_project(PinnedDrop)]
pub struct TaskHandle<T> {
    id: u64,
    shutdown: Shutdown,
    join_handle: JoinHandle<Result<T, Error>>,
    detached: bool,
}

impl<T> TaskHandle<T> {
    pub async fn stop(&self) {
        self.shutdown.shutdown();
        self.shutdown.wait_shutdown_complete().await;
    }
    pub fn stop_no_wait(&self) {
        self.shutdown.shutdown();
    }
    pub fn detach(&mut self) {
        self.detached = true;
    }
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T, Error>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use futures::future::FutureExt;
        // If the task panics just propagate it up
        self.join_handle.poll_unpin(cx).map(Result::unwrap)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for TaskHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.detached {
            self.stop_no_wait();
        }
    }
}

/// TaskTracker is a wrapper around tokio for asynchronous tasks and futures.
/// This class adds the following functionalities which are not present in tokio:
/// * Tokio does not provide a way to selectively wait for certain tasks to
///   finish executing while cancelling others during system shutdown
/// * Tokio does not provide an api to run some cleanup code when cancelling a task
/// * TaskTracker provides an easy api to cancel a bunch of spawned tasks with a user
///   provided tag which would otherwise be difficult to do by keeping track of
///   tokio returned join handles
///
/// It is recommended to create task tracker as a global singleton while it is
/// certainly possible to have multiple instances of it. It is possible to clone
/// and share it across multiple tasks. Each clone uses the same internal state.
/// WARNING: Dropping a task tracker without invoking `shutdown_and_wait()` first
/// will result in a panic
///
/// # Example
/// ```no_run
/// // Create task tracker in your main function or as a global singleton
/// let mut tracker = TaskTracker::new("task_tracker".to_string());
/// // a task which will be cancelled on shutdown        
/// let task1 = tracker.spawn_cancel(
///   Some("test".to_string()),
///   async move {
///     tokio::time::sleep(std::time::Duration::from_secs(600)).await;
///     42
///   }
/// );
/// // a task which will be waited on for completion on shutdown
/// let task2 = tracker.spawn_cancel(
///   Some("test".to_string()),
///   async move {
///     tokio::time::sleep(std::time::Duration::from_secs(600)).await;
///     42
///   }
/// );
/// // a task which will be canceled and instead run cleanup on shutdown
/// let task3 = tracker.spawn_cancel_and_wait(
///    "test3".to_string(),
///    Some("test".to_string()),
///    // This future will be cancelled on shutdown
///    async move {
///        tokio::time::sleep(std::time::Duration::from_secs(600)).await;
///        42
///    },
/// // This future will be run on shutdown after cancelling the one
/// // above
///    async move { 43 },
/// );
/// // Do not forget to call shutdown_and_wait
/// tracker.shutdown_and_wait().await;
/// drop(tracker);
/// ```
///
#[derive(Clone)]
pub struct TaskTracker {
    state: Arc<Mutex<State>>,
}

struct State {
    name: String,
    tasks: BTreeMap<u64, Task>,
    next_task_id: u64,
    task_tracker_state: TaskTrackerState,
}

impl TaskTracker {
    async fn cancel_all_and_wait(&mut self) {
        // loop through the task map and invoke shutdown on all
        let mut futures: Vec<async_shutdown::ShutdownComplete> = vec![];
        {
            let state = self.state.lock();
            for (_name, task) in state.tasks.iter() {
                futures.push({
                    task.shutdown.shutdown();
                    task.shutdown.wait_shutdown_complete()
                });
            }
        }
        futures::future::join_all(futures).await;
    }
    fn get_task_handle<T>(
        &mut self,
        tag: Option<String>,
        task_id: u64,
        shutdown_token: Shutdown,
        fut: Pin<Box<dyn Future<Output = ()> + Send>>,
        detached: bool,
        mut rx: tokio::sync::oneshot::Receiver<<T as Future>::Output>,
    ) -> TaskHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let task = Task {
            task_id,
            tag: tag.clone(),
            shutdown: shutdown_token.clone(),
        };
        {
            let mut state = self.state.lock();
            if !detached {
                state.tasks.insert(task_id, task);
            }
        }
        let state_cloned = self.state.clone();
        TaskHandle {
            id: task_id,
            shutdown: shutdown_token.clone(),
            join_handle: tokio::spawn(async move {
                fut.await;
                if !detached {
                    let mut state = state_cloned.lock();
                    state.tasks.remove(&task_id);
                }
                rx.try_recv()
            }),
            detached: false,
        }
    }
    pub fn new(name: String) -> Self {
        let tasks = BTreeMap::new();
        let state = State {
            name,
            tasks,
            next_task_id: 0,
            task_tracker_state: TaskTrackerState::Running,
        };
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }
    pub async fn cancel(&mut self, task_id: u64) {
        let task;
        {
            let mut state = self.state.lock();
            task = state.tasks.get_mut(&task_id).cloned();
        }
        if task.is_none() {
            error!("Task with task_id {task_id} doesn't exist!");
        } else {
            let shutdown = task.unwrap().shutdown;
            shutdown.clone().shutdown();
            shutdown.clone().wait_shutdown_complete().await;
        }
    }
    pub fn spawn_cancel<T>(&mut self, tag: Option<String>, run: T) -> TaskHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let task_id;
        let detached;
        {
            let mut state = self.state.lock();
            if state.tasks.contains_key(&state.next_task_id) {
                panic!(
                    "Task tracker already running a task with next id: `{{state.next_task_id}}`"
                );
            }
            task_id = state.next_task_id;
            detached = state.task_tracker_state != TaskTrackerState::Running;
            if detached {
                warn!("Task arrived after task tracking is shutdown, will start as detached");
            }
            state.next_task_id = task_id + 1;
        }
        let shutdown_token = Shutdown::new();
        let cloned_token = shutdown_token.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<T::Output>();
        let wrapped = shutdown_token.clone().wrap_cancel(run);
        let fut = Box::pin(async move {
            info!("Starting task with task_id {task_id}");
            let task_output = wrapped.await;
            if task_output.is_none() {
                info!("Task with task_id {task_id} is cancelled because of shutdown");
                drop(tx);
                return ();
            }
            if tx.send(task_output.unwrap()).is_err() {
                warn!("Task with task_id {task_id} output could not be sent: receiver dropped")
            }
            ()
        });
        self.get_task_handle::<T>(tag, task_id, cloned_token, fut, detached, rx)
    }
    pub fn spawn_wait<T>(&mut self, tag: Option<String>, run: T) -> TaskHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let task_id;
        let detached;
        {
            let mut state = self.state.lock();
            if state.tasks.contains_key(&state.next_task_id) {
                panic!(
                    "Task tracker already running a task with next id: `{{state.next_task_id}}`"
                );
            }
            task_id = state.next_task_id;
            detached = state.task_tracker_state != TaskTrackerState::Running;
            if detached {
                warn!("Task arrived after task tracking is shutdown, will start as detached");
            }
            state.next_task_id = task_id + 1;
        }
        let shutdown_token = Shutdown::new();
        let cloned_token = shutdown_token.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<T::Output>();
        let wrapped = shutdown_token.clone().wrap_wait(run);
        let fut = Box::pin(async move {
            info!("Starting task with task_id {task_id}");
            if wrapped.is_err() {
                error!("Task with task_id {task_id} is already shutdown before shutdown closure could run");
                drop(tx);
                return ();
            }
            let task_output = wrapped.unwrap().await;
            if tx.send(task_output).is_err() {
                warn!("Task with task_id {task_id} output could not be sent: receiver dropped")
            }
            ()
        });
        self.get_task_handle::<T>(tag, task_id, cloned_token, fut, detached, rx)
    }
    pub fn spawn_cancel_and_wait<T, U>(
        &mut self,
        tag: Option<String>,
        run: T,
        cleanup: U,
    ) -> TaskHandle<Result<T::Output, U::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
        U: Future + Send + 'static,
        U::Output: Send + 'static,
    {
        let task_id;
        let detached;
        {
            let mut state = self.state.lock();
            if state.tasks.contains_key(&state.next_task_id) {
                panic!("Task tracker already running a task with id: `{{state.next_task_id}}`");
            }
            task_id = state.next_task_id;
            detached = state.task_tracker_state != TaskTrackerState::Running;
            if detached {
                warn!("Task arrived after task tracking is shutdown, will start as detached");
            }
            state.next_task_id = task_id + 1;
        }
        let shutdown_token = Shutdown::new();
        let shutdown_cloned = shutdown_token.clone();
        let (tx, mut rx) = tokio::sync::oneshot::channel::<Result<T::Output, U::Output>>();
        let (barrier_tx, barrier_rx) = tokio::sync::oneshot::channel::<bool>();
        let wrapped_cleanup = shutdown_token.clone().wrap_wait(async move {
            let run_cleanup = barrier_rx.await;
            if run_cleanup.is_ok() && run_cleanup.unwrap() {
                Some(cleanup.await)
            } else {
                None
            }
        });
        let wrapped_run = async move {
            match shutdown_token
                .clone()
                .wrap_cancel(async move { run.await })
                .await
            {
                None => {
                    if barrier_tx.send(true).is_err() {
                        error!("Receiver dropped")
                    }
                    None
                }
                Some(val) => {
                    if barrier_tx.send(false).is_err() {
                        error!("Receiver dropped")
                    }
                    Some(val)
                }
            }
        };
        let fut = Box::pin(async move {
            info!("Starting task with task_id {task_id}");
            if wrapped_cleanup.is_err() {
                error!("Task with task_id {task_id} is already shutdown before shutdown closure could run");
                drop(tx);
                return ();
            }
            let success = wrapped_run.await;
            let out = if success.is_some() {
                Ok(success.unwrap())
            } else {
                let cleanup_result = wrapped_cleanup.unwrap().await;
                if cleanup_result.is_none() {
                    panic!("Cleanup failed to run for task with id: {task_id}");
                }
                Err(cleanup_result.unwrap())
            };
            if tx.send(out).is_err() {
                warn!("Task with task_id {task_id} output could not be sent: receiver dropped")
            }
            ()
        });
        let task = Task {
            task_id,
            tag: tag.clone(),
            shutdown: shutdown_cloned.clone(),
        };
        {
            let mut state = self.state.lock();
            if !detached {
                state.tasks.insert(task_id, task);
            }
        }
        let state_cloned = self.state.clone();
        TaskHandle {
            id: task_id,
            shutdown: shutdown_cloned,
            join_handle: tokio::spawn(async move {
                fut.await;
                if !detached {
                    let mut state = state_cloned.lock();
                    state.tasks.remove(&task_id);
                }
                rx.try_recv()
            }),
            detached: false,
        }
    }
    /// signals shutdown of this executor and any Clones
    pub async fn shutdown_and_wait(&mut self) {
        // hang up the channel which will cause the dedicated thread
        // to quit
        let mut shutdown_started = false;
        {
            let mut state = self.state.lock();
            if state.task_tracker_state == TaskTrackerState::Running {
                state.task_tracker_state = TaskTrackerState::ShutdownStarted;
                shutdown_started = true;
            }
        }
        if shutdown_started {
            self.cancel_all_and_wait().await;
            {
                let mut state = self.state.lock();
                state.task_tracker_state = TaskTrackerState::ShutdownComplete;
            }
        }
    }
    pub async fn stop_task(&self, task_id: u64) {
        let task;
        {
            let mut state = self.state.lock();
            task = state.tasks.get_mut(&task_id).cloned();
        }
        if task.is_none() {
            error!("Task with task_id {task_id} doesn't exist!");
        } else {
            let shutdown = task.unwrap().shutdown;
            shutdown.clone().shutdown();
            shutdown.clone().wait_shutdown_complete().await;
        }
    }
    pub async fn stop_tagged_tasks(&self, tag: String) {
        let mut task_ids: Vec<u64> = vec![];
        let mut futures: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![];
        {
            let state = self.state.lock();
            for (task_id, task) in state.tasks.iter() {
                if task.tag.is_some() && *task.tag.as_ref().unwrap() == tag {
                    task_ids.push(*task_id);
                }
            }
        }
        for task_id in task_ids {
            futures.push(Box::pin(self.stop_task(task_id)));
        }
        futures::future::join_all(futures).await;
    }
}

impl Drop for TaskTracker {
    fn drop(&mut self) {
        let state = self.state.lock();
        if state.task_tracker_state != TaskTrackerState::ShutdownComplete {
            let name = &state.name;
            panic!("Dropping task tracker {name} without calling shutdown_and_wait!");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use std::sync::{Arc, Barrier};

    #[tokio::test]
    async fn basic() {
        let mut tracker = TaskTracker::new("task_tracker".to_string());
        // a task which will be cancelled on shutdown
        let task1 = tracker.spawn_cancel(Some("test".to_string()), async move {
            tokio::time::sleep(std::time::Duration::from_secs(600)).await;
            42
        });
        // a task which we will wait for completion on shutdown
        let task2 = tracker.spawn_wait(Some("test".to_string()), async move {
            tokio::time::sleep(std::time::Duration::from_secs(20)).await;
            42
        });
        // a task which will be canceled and cleanup on shutdown
        let task3 = tracker.spawn_cancel_and_wait(
            Some("test".to_string()),
            // Future will be cancelled on shutdown
            async move {
                tokio::time::sleep(std::time::Duration::from_secs(600)).await;
                42
            },
            // Future will be run on shutdown
            async move { 43 },
        );
        // shutdown all tasks
        tracker.shutdown_and_wait().await;

        assert_eq!(task1.await.is_err(), true);
        assert_eq!(task2.await.unwrap(), 42);
        assert_eq!(task3.await.unwrap(), Err(43));
    }
}
