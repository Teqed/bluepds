use std::future::Future;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{self, JoinHandle};
use tracing::error;

/// Background Queue for asynchronous processing tasks
///
/// A simple queue for in-process, out-of-band/backgrounded work
#[derive(Clone)]
pub struct BackgroundQueue {
    semaphore: Arc<Semaphore>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    destroyed: Arc<Mutex<bool>>,
}

impl BackgroundQueue {
    /// Create a new BackgroundQueue with the specified concurrency limit
    pub fn new(concurrency: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(concurrency)),
            tasks: Arc::new(Mutex::new(Vec::new())),
            destroyed: Arc::new(Mutex::new(false)),
        }
    }

    /// Add a task to the queue
    pub async fn add<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let destroyed = *self.destroyed.lock().await;
        if destroyed {
            return;
        }

        let permit = match self.semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                error!("Failed to acquire semaphore permit for background task");
                return;
            }
        };

        let tasks = self.tasks.clone();

        let handle = task::spawn(async move {
            future.await;

            // Catch any panics to prevent task failures from propagating
            if let Err(e) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {})) {
                error!("Background queue task panicked: {:?}", e);
            }

            // Release the semaphore permit
            drop(permit);
        });

        // Store the handle for later cleanup
        tasks.lock().await.push(handle);
    }

    /// Wait for all tasks to finish
    pub async fn process_all(&self) {
        let mut handles = self.tasks.lock().await;
        while let Some(handle) = handles.pop() {
            let _ = handle.await;
        }
    }

    /// Stop accepting new tasks, wait for all to finish
    pub async fn destroy(&self) {
        *self.destroyed.lock().await = true;
        self.process_all().await;
    }
}
