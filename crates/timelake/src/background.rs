//! Writes that finish after the call that started them returns. A commit is done once its
//! metadata is written; updating the version hint and the head after that are only shortcuts for
//! readers, so a commit doesn't wait for them (spec 27, P4).

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

/// Background writes started and not yet finished or dropped.
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static FINISHED: Notify = Notify::const_new();

/// Counts a task as in flight until it completes or is dropped with its runtime.
struct InFlight;

impl InFlight {
    fn start() -> Self {
        IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
        Self
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
        FINISHED.notify_waiters();
    }
}

/// Runs `task` on the current Tokio runtime without waiting for it.
pub(crate) fn spawn(task: impl Future<Output = ()> + Send + 'static) {
    let in_flight = InFlight::start();
    tokio::spawn(async move {
        task.await;
        drop(in_flight);
    });
}

/// Waits for the background writes this process started. Call it before the process exits, or
/// the last commit's version hint and head may not be updated: readers still find the newest
/// version, but need extra requests to do so. The CLI and the Python package call it for you.
pub async fn flush() {
    loop {
        let finished = FINISHED.notified();
        if IN_FLIGHT.load(Ordering::SeqCst) == 0 {
            return;
        }
        finished.await;
    }
}
