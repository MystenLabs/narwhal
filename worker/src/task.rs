use async_shutdown::Shutdown;
use futures::Future;
use parking_lot::Mutex;
use pin_project::{pin_project, pinned_drop};
use std::collections::BTreeMap;
use std::{pin::Pin, sync::Arc};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

#[derive(Clone)]
struct Task {
    task_id: u64,
    parent_task_id: u64,
    name: String,
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
        self.join_handle.abort();
    }
    pub fn stop_no_wait(&self) {
        self.shutdown.shutdown();
    }
    pub fn abort(&self) {
        self.join_handle.abort();
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

#[derive(Clone)]
pub struct TaskTracker {
    state: Arc<Mutex<State>>,
}

struct State {
    name: String,
    tasks: BTreeMap<u64, Task>,
    tasks_by_name: BTreeMap<String, u64>,
    next_task_id: u64,
    shutdown: bool,
}

impl TaskTracker {
    async fn cancel_all_and_wait(&mut self) {
        // loop through the task map and invoke cancel on all
        let mut futures: Vec<async_shutdown::ShutdownComplete> = vec![];
        {
            let state = self.state.lock();
            for (_name, task) in state.tasks.iter() {
                task.shutdown.shutdown();
            }
            for (_name, task) in state.tasks.iter() {
                futures.push(task.shutdown.wait_shutdown_complete());
            }
        }
        futures::future::join_all(futures).await;
    }
    fn get_task_handle<T>(
        &mut self,
        name: String,
        tag: Option<String>,
        task_id: u64,
        parent_task_id: u64,
        shutdown_token: Shutdown,
        fut: Pin<Box<dyn Future<Output = ()> + Send>>,
        detached: bool,
        mut rx: tokio::sync::oneshot::Receiver<<T as Future>::Output>,
    ) -> TaskHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        // Create a new shutdown object.
        // We will clone it into all tasks that need it.
        let task = Task {
            task_id,
            parent_task_id,
            name: name.clone(),
            tag: tag.clone(),
            shutdown: shutdown_token.clone(),
        };
        {
            let mut state = self.state.lock();
            if !detached {
                state.tasks.insert(task_id, task);
                state.tasks_by_name.insert(name.clone(), task_id);
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
                    state.tasks_by_name.remove(&name);
                }
                rx.try_recv()
            }),
            detached: false,
        }
    }
    pub fn new(name: String) -> Self {
        let tasks = BTreeMap::new();
        let tasks_by_name = BTreeMap::new();
        let state = State {
            name,
            tasks,
            tasks_by_name,
            next_task_id: 0,
            shutdown: false,
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
    pub fn spawn_cancel<T>(
        &mut self,
        name: String,
        tag: Option<String>,
        run: T,
    ) -> Option<TaskHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        tokio::task_local! {
            pub static TASK_ID: u64;
        };
        let parent_task_id = TASK_ID.get();
        let task_id;
        let detached;
        {
            let mut state = self.state.lock();
            if state.tasks_by_name.contains_key(&name) {
                error!("Task with {name} already exists");
                return None;
            }
            if state.tasks.contains_key(&state.next_task_id) {
                panic!("Task already running task with id: `{{state.next_task_id}}`");
            }
            task_id = state.next_task_id;
            detached = state.shutdown;
            if detached {
                warn!(
                    "Task {name} arrived after task tracking is shutdown, will start as detached"
                );
            }
            state.next_task_id = task_id + 1;
        }
        let name_cloned = name.clone();
        let shutdown_token = Shutdown::new();
        let cloned_token = shutdown_token.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<T::Output>();
        let fut = Box::pin(async move {
            info!("Starting task {name_cloned} with task_id {task_id}");
            let task_output = shutdown_token
                .clone()
                .wrap_cancel(TASK_ID.scope(task_id, run))
                .await;
            if task_output.is_none() {
                info!("Task {name_cloned} with task_id {task_id} is cancelled because of shutdown");
                drop(tx);
                return ();
            }
            if tx.send(task_output.unwrap()).is_err() {
                warn!("Task {name_cloned} with task_id {task_id} output could not be sent: receiver dropped")
            }
            ()
        });
        Some(self.get_task_handle::<T>(
            name,
            tag,
            task_id,
            parent_task_id,
            cloned_token,
            fut,
            detached,
            rx,
        ))
    }
    pub fn spawn_wait<T>(
        &mut self,
        name: String,
        tag: Option<String>,
        run: T,
    ) -> Option<TaskHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        tokio::task_local! {
            pub static TASK_ID: u64;
        };
        let parent_task_id = TASK_ID.get();
        let task_id;
        let detached;
        {
            let mut state = self.state.lock();
            if state.tasks_by_name.contains_key(&name) {
                error!("Task with {name} already exists");
                return None;
            }
            if state.tasks.contains_key(&state.next_task_id) {
                panic!("Task already running task with id: `{{state.next_task_id}}`");
            }
            task_id = state.next_task_id;
            detached = state.shutdown;
            if detached {
                warn!(
                    "Task {name} arrived after task tracking is shutdown, will start as detached"
                );
            }
            state.next_task_id = task_id + 1;
        }
        let name_cloned = name.clone();
        let shutdown_token = Shutdown::new();
        let cloned_token = shutdown_token.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<T::Output>();
        let fut = Box::pin(async move {
            info!("Starting task {name_cloned} with task_id {task_id}");
            let res = shutdown_token
                .clone()
                .wrap_wait(TASK_ID.scope(task_id, run));
            if res.is_err() {
                error!("Task {name_cloned} with task_id {task_id} is already shutdown before shutdown closure could run");
                drop(tx);
                return ();
            }
            let task_output = res.unwrap().await;
            if tx.send(task_output).is_err() {
                warn!("Task {name_cloned} with task_id {task_id} output could not be sent: receiver dropped")
            }
            ()
        });
        Some(self.get_task_handle::<T>(
            name,
            tag,
            task_id,
            parent_task_id,
            cloned_token,
            fut,
            detached,
            rx,
        ))
    }
    pub fn spawn_cancel_and_wait<T>(
        &mut self,
        name: String,
        tag: Option<String>,
        run: T,
        cleanup: T,
    ) -> Option<TaskHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        tokio::task_local! {
            pub static TASK_ID: u64;
        };
        let parent_task_id = TASK_ID.get();
        let task_id;
        let detached;
        {
            let mut state = self.state.lock();
            if state.tasks_by_name.contains_key(&name) {
                error!("Task with {name} already exists");
                return None;
            }
            if state.tasks.contains_key(&state.next_task_id) {
                panic!("Task already running task with id: `{{state.next_task_id}}`");
            }
            task_id = state.next_task_id;
            detached = state.shutdown;
            if detached {
                warn!(
                    "Task {name} arrived after task tracking is shutdown, will start as detached"
                );
            }
            state.next_task_id = task_id + 1;
        }
        let name_cloned = name.clone();
        let shutdown_token = Shutdown::new();
        let cloned_token = shutdown_token.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<T::Output>();
        let fut = Box::pin(async move {
            info!("Starting task {name_cloned} with task_id {task_id}");
            let mut task_output = shutdown_token
                .clone()
                .wrap_cancel(TASK_ID.scope(task_id, run))
                .await;
            if task_output.is_none() {
                let res = shutdown_token.wrap_wait(TASK_ID.scope(task_id, cleanup));
                if res.is_err() {
                    error!("Task {name_cloned} with task_id {task_id} is already shutdown before shutdown closure could run");
                    drop(tx);
                    return ();
                }
                task_output = Some(res.unwrap().await);
            }
            if tx.send(task_output.unwrap()).is_err() {
                warn!("Task {name_cloned} with task_id {task_id} output could not be sent: receiver dropped")
            }
            ()
        });
        Some(self.get_task_handle::<T>(
            name,
            tag,
            task_id,
            parent_task_id,
            cloned_token,
            fut,
            detached,
            rx,
        ))
    }
    /// signals shutdown of this executor and any Clones
    pub async fn shutdown_and_wait(&mut self) {
        // hang up the channel which will cause the dedicated thread
        // to quit
        let shutdown;
        {
            let mut state = self.state.lock();
            shutdown = state.shutdown;
            state.shutdown = true;
        }
        if !shutdown {
            self.cancel_all_and_wait().await;
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
    pub async fn stop_named_task(&self, name: String) {
        let task_id;
        {
            let state = self.state.lock();
            let task = state.tasks_by_name.get(&name);
            if task.is_none() {
                error!("Task with name {name} doesn't exist!");
            }
            let task = state.tasks.get(task.unwrap());
            if task.is_none() {
                panic!("Task missing in task map {name}");
            } else {
                task_id = task.unwrap().task_id;
            }
        }
        self.stop_task(task_id);
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
        let handle = tokio::runtime::Handle::current();
        handle.enter();
        futures::executor::block_on(self.cancel_all_and_wait())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, Barrier},
        time::Duration,
    };

    #[tokio::test]
    async fn basic() {
        let barrier = Arc::new(Barrier::new(2));
        let handle = tokio::runtime::Handle::current();
        let mut tracker = TaskTracker::new("task_tracker".to_string());
        let shutdown = Shutdown::new();
        let shutdown_cloned = shutdown.clone();
        let dedicated_task = tracker.spawn_cancel(
            "test1".to_string(),
            Some("test".to_string()),
            async move {
                tokio::time::sleep(std::time::Duration::from_secs(600)).await;
            },
        );
        drop(tracker);
        assert_eq!(dedicated_task.unwrap().await.is_err(), true);
    }
}
