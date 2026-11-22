use parking_lot::{Condvar, Mutex, RwLock};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::pyclass::{PyTraverseError, PyVisit};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::errors::pool_closed_err;
use crate::metrics::AtomicMetrics;
use crate::timeout::deadline_from_secs;

/// RAII guard that decrements an `AtomicUsize` counter on drop.
/// Used to ensure `num_polling` is always decremented even if a panic
/// occurs between `fetch_add` and the corresponding `fetch_sub`.
struct PollerGuard<'a>(&'a AtomicUsize);

impl Drop for PollerGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Native Rust Worker Pool Core for zero-overhead task queueing and work stealing.
#[pyclass(frozen, module = "multiloop._multiloop_core")]
pub struct NativeWorkerPool {
    global_sender: Mutex<Option<flume::Sender<Py<PyAny>>>>,
    global_receiver: flume::Receiver<Py<PyAny>>,
    local_senders: Mutex<Vec<flume::Sender<Py<PyAny>>>>,
    local_receivers: Vec<flume::Receiver<Py<PyAny>>>,
    buffers: Vec<Mutex<VecDeque<Py<PyAny>>>>,
    is_closed: Arc<AtomicBool>,
    num_polling: AtomicUsize,
    idle_mask: Box<[AtomicU64]>,
    outstanding_tasks: AtomicUsize,
    drain_mutex: Mutex<()>,
    drain_condvar: Condvar,
    metrics: RwLock<Option<Py<AtomicMetrics>>>,
}

#[pymethods]
impl NativeWorkerPool {
    #[new]
    pub fn new(num_threads: usize) -> Self {
        let (global_sender, global_receiver) = flume::unbounded();
        let mut local_senders = Vec::with_capacity(num_threads);
        let mut local_receivers = Vec::with_capacity(num_threads);
        let mut buffers = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let (tx, rx) = flume::unbounded();
            local_senders.push(tx);
            local_receivers.push(rx);
            buffers.push(Mutex::new(VecDeque::new()));
        }

        let mask_words = num_threads.div_ceil(64);
        let idle_mask: Box<[AtomicU64]> = (0..mask_words)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        NativeWorkerPool {
            global_sender: Mutex::new(Some(global_sender)),
            global_receiver,
            local_senders: Mutex::new(local_senders),
            local_receivers,
            buffers,
            is_closed: Arc::new(AtomicBool::new(false)),
            num_polling: AtomicUsize::new(0),
            idle_mask,
            outstanding_tasks: AtomicUsize::new(0),
            drain_mutex: Mutex::new(()),
            drain_condvar: Condvar::new(),
            metrics: RwLock::new(None),
        }
    }

    pub fn mark_worker_idle(&self, index: usize) {
        if index < self.local_receivers.len() {
            let word = index >> 6;
            let bit = 1u64 << (index & 63);
            self.idle_mask[word].fetch_or(bit, Ordering::Release);
        }
    }

    pub fn clear_worker_idle(&self, index: usize) {
        if index < self.local_receivers.len() {
            let word = index >> 6;
            let bit = 1u64 << (index & 63);
            self.idle_mask[word].fetch_and(!bit, Ordering::Release);
        }
    }

    pub fn claim_one_idle_worker(&self) -> Option<usize> {
        let num_workers = self.local_receivers.len();
        for (word_idx, atom) in self.idle_mask.iter().enumerate() {
            let mut cur = atom.load(Ordering::Acquire);
            while cur != 0 {
                let tz = cur.trailing_zeros() as usize;
                let idx = (word_idx << 6) + tz;
                if idx >= num_workers {
                    break;
                }
                let cleared = cur & !(1u64 << tz);
                match atom.compare_exchange_weak(cur, cleared, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => return Some(idx),
                    Err(actual) => cur = actual,
                }
            }
        }
        None
    }

    pub fn close(&self) {
        // Drop senders first outside the lock so receivers become disconnected after draining,
        // then set the advisory flag so is_closed() returns true immediately.
        let _dropped_global = self.global_sender.lock().take();
        let _dropped_local = std::mem::take(&mut *self.local_senders.lock());
        self.is_closed.store(true, Ordering::Release);
        drop(_dropped_global);
        drop(_dropped_local);
    }

    pub fn set_metrics(&self, metrics: Py<AtomicMetrics>) {
        let _old = self.metrics.write().replace(metrics);
        drop(_old);
    }

    pub fn is_closed(&self) -> bool {
        if self.is_closed.load(Ordering::Acquire) {
            // Flag load: Acquire observes close store
            return true;
        }
        self.global_sender
            .lock()
            .as_ref()
            .is_none_or(|s| s.is_disconnected())
    }

    pub fn is_drained(&self) -> bool {
        if !self.global_receiver.is_empty() {
            return false;
        }
        for buf in &self.buffers {
            if !buf.lock().is_empty() {
                return false;
            }
        }
        for rx in &self.local_receivers {
            if !rx.is_empty() {
                return false;
            }
        }
        true
    }

    pub fn complete_task(&self) {
        let mut cur = self.outstanding_tasks.load(Ordering::Acquire);
        while cur > 0 {
            match self.outstanding_tasks.compare_exchange_weak(
                cur,
                cur - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if cur == 1 {
                        let _guard = self.drain_mutex.lock();
                        self.drain_condvar.notify_all();
                    }
                    return;
                }
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn outstanding_count(&self) -> usize {
        self.outstanding_tasks.load(Ordering::Acquire)
    }

    pub fn wait_drained(&self, py: Python<'_>, timeout_secs: f64) -> PyResult<bool> {
        let deadline = deadline_from_secs(timeout_secs)?;
        if self.outstanding_tasks.load(Ordering::Acquire) == 0 && self.is_drained() {
            return Ok(true);
        }
        if timeout_secs <= 0.0 {
            return Ok(false);
        }
        Ok(py.detach(|| {
            let mut guard = self.drain_mutex.lock();
            loop {
                if self.outstanding_tasks.load(Ordering::Acquire) == 0 && self.is_drained() {
                    return true;
                }
                match deadline {
                    Some(dl) => {
                        let now = std::time::Instant::now();
                        if now >= dl {
                            return false;
                        }
                        self.drain_condvar.wait_for(&mut guard, dl - now);
                    }
                    None => {
                        self.drain_condvar.wait(&mut guard);
                    }
                }
            }
        }))
    }

    pub fn drain_all(&self, _py: Python<'_>) -> Vec<Py<PyAny>> {
        let mut drained = Vec::new();
        while let Ok(task) = self.global_receiver.try_recv() {
            drained.push(task);
        }
        for buf in &self.buffers {
            let mut guard = buf.lock();
            drained.extend(guard.drain(..));
        }
        for rx in &self.local_receivers {
            while let Ok(task) = rx.try_recv() {
                drained.push(task);
            }
        }
        drained
    }

    pub fn push_global(&self, py: Python<'_>, task: Py<PyAny>) -> PyResult<()> {
        // WHY: pop_work() signals "no work" with Ok(None).  A None task pushed
        // here would therefore be silently swallowed by every worker — reject
        // it at the boundary instead of losing work.
        if task.is_none(py) {
            return Err(PyTypeError::new_err("pool task cannot be None"));
        }
        // Fast path: check advisory flag before acquiring lock — Acquire observes close store
        if self.is_closed.load(Ordering::Acquire) {
            drop(task);
            return Err(pool_closed_err(py, "Pool is closed"));
        }
        let send_res = {
            let guard = self.global_sender.lock();
            match guard.as_ref() {
                Some(sender) => {
                    self.outstanding_tasks.fetch_add(1, Ordering::AcqRel);
                    match sender.send(task) {
                        Ok(()) => Ok(()),
                        Err(flume::SendError(rejected)) => Err(Some(rejected)),
                    }
                }
                None => Err(None),
            }
        };
        match send_res {
            Ok(()) => Ok(()),
            Err(Some(rejected)) => {
                self.complete_task();
                drop(rejected);
                Err(pool_closed_err(py, "Pool is closed"))
            }
            Err(None) => Err(pool_closed_err(py, "Pool is closed")),
        }
    }

    pub fn push_global_batch(&self, py: Python<'_>, tasks: Vec<Py<PyAny>>) -> PyResult<usize> {
        if tasks.is_empty() {
            return Ok(0);
        }
        for task in &tasks {
            if task.is_none(py) {
                return Err(PyTypeError::new_err("pool task cannot be None"));
            }
        }
        if self.is_closed.load(Ordering::Acquire) {
            drop(tasks);
            return Err(pool_closed_err(py, "Pool is closed"));
        }
        let mut unsent = Vec::new();
        let (sent_count, closed_err) = {
            let guard = self.global_sender.lock();
            match guard.as_ref() {
                Some(sender) => {
                    let mut sent = 0usize;
                    let mut failed = false;
                    for task in tasks {
                        if failed {
                            unsent.push(task);
                            continue;
                        }
                        self.outstanding_tasks.fetch_add(1, Ordering::AcqRel);
                        match sender.send(task) {
                            Ok(()) => sent += 1,
                            Err(flume::SendError(rejected)) => {
                                self.complete_task();
                                unsent.push(rejected);
                                failed = true;
                            }
                        }
                    }
                    (sent, failed && sent == 0)
                }
                None => {
                    unsent = tasks;
                    (0, true)
                }
            }
        };
        drop(unsent);
        if closed_err {
            Err(pool_closed_err(py, "Pool is closed"))
        } else {
            Ok(sent_count)
        }
    }

    pub fn push_local(&self, index: usize, task: Py<PyAny>, py: Python<'_>) -> PyResult<()> {
        // Same None rejection as push_global: a None "task" would be read by
        // pop_work() as "no work" and dropped.
        if task.is_none(py) {
            return Err(PyTypeError::new_err("pool task cannot be None"));
        }
        // Fast path: check advisory flag before acquiring lock — Acquire observes close store
        if self.is_closed.load(Ordering::Acquire) {
            drop(task);
            return Err(pool_closed_err(py, "Pool is closed"));
        }
        enum LocalSendErr {
            Closed(Option<Py<PyAny>>),
            OutOfRange(Py<PyAny>),
        }
        let send_res = {
            let guard = self.local_senders.lock();
            // WHY (R10): close() clears the senders before setting the flag, so
            // a lock-side is_closed() check can still see False mid-close; an
            // empty sender list IS the closed state — report it as such instead
            // of the misleading "Worker index out of range".
            if guard.is_empty() {
                Err(LocalSendErr::Closed(Some(task)))
            } else if index >= guard.len() {
                Err(LocalSendErr::OutOfRange(task))
            } else {
                self.outstanding_tasks.fetch_add(1, Ordering::AcqRel);
                match guard[index].send(task) {
                    Ok(_) => Ok(()),
                    Err(flume::SendError(rejected)) => {
                        self.complete_task();
                        Err(LocalSendErr::Closed(Some(rejected)))
                    }
                }
            }
        };
        match send_res {
            Ok(()) => {
                // Track remote schedule: explicit routing to a specific worker.
                if let Some(ref metrics) = *self.metrics.read() {
                    metrics.get().inc_remote_schedule(index);
                }
                Ok(())
            }
            Err(LocalSendErr::Closed(rejected)) => {
                drop(rejected);
                Err(pool_closed_err(py, "Pool is closed"))
            }
            Err(LocalSendErr::OutOfRange(rejected)) => {
                drop(rejected);
                Err(PyRuntimeError::new_err("Worker index out of range"))
            }
        }
    }

    pub fn pop_work(&self, index: usize, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        // Helper to sample injection queue depth from the live channel state.
        let sample_depth = |metrics: &Py<AtomicMetrics>, idx: usize, depth: usize| {
            metrics.get().set_injection_queue_depth(idx, depth);
        };

        let num_workers = self.local_receivers.len();

        loop {
            let is_draining = self.is_closed.load(Ordering::Acquire);
            let mut was_poller = false;

            // 1. Check per-worker buffer first (cached from previous batch pulls).
            if index < self.buffers.len() {
                let mut buffer = self.buffers[index].lock();
                if let Some(task) = buffer.pop_front() {
                    return Ok(Some(task));
                }
                // Buffer empty – try batch pull from global (lock still held).
                let max_pollers = std::cmp::max(1, num_workers / 2);
                let is_poller =
                    is_draining || (self.num_polling.fetch_add(1, Ordering::Relaxed) < max_pollers);
                let _guard = if !is_draining {
                    Some(PollerGuard(&self.num_polling))
                } else {
                    None
                };
                was_poller = is_poller;
                if is_poller {
                    let global_len = self.global_receiver.len();
                    if global_len > 0 {
                        let max_batch = if is_draining { 64 } else { 128 };
                        let batch_size = global_len
                            .checked_div(num_workers)
                            .map_or(1, |d| std::cmp::min(d + 1, max_batch));
                        let mut pulled = 0usize;
                        for _ in 0..batch_size {
                            match self.global_receiver.try_recv() {
                                Ok(task) => {
                                    buffer.push_back(task);
                                    pulled += 1;
                                }
                                Err(_) => break,
                            }
                        }
                        if pulled > 0 {
                            if let Some(ref metrics) = *self.metrics.read() {
                                metrics.get().add_global_pull(index, pulled);
                                let new_depth = self.global_receiver.len();
                                sample_depth(metrics, index, new_depth);
                            }
                        }
                        if let Some(task) = buffer.pop_front() {
                            return Ok(Some(task));
                        }
                    }
                }
            }

            // 2. Check local dedicated queue.
            if index < self.local_receivers.len() {
                if let Ok(task) = self.local_receivers[index].try_recv() {
                    return Ok(Some(task));
                }
            }

            // 2b. Single-item fallback if skipped by soft poller gate so worker never parks
            // when global_receiver still has available tasks.
            if !was_poller {
                if let Ok(task) = self.global_receiver.try_recv() {
                    if let Some(ref metrics) = *self.metrics.read() {
                        metrics.get().add_global_pull(index, 1);
                        let new_depth = self.global_receiver.len();
                        sample_depth(metrics, index, new_depth);
                    }
                    return Ok(Some(task));
                }
            }

            // 2c. Cross-worker buffer work-stealing: if another worker cached a batch in its
            // private buffer, steal up to half (at least 1) without holding two buffer locks at once.
            if num_workers > 1 && index < self.buffers.len() {
                for offset in 1..num_workers {
                    let victim = (index + offset) % num_workers;
                    let mut stolen_extra = Vec::new();
                    let first_stolen = {
                        let mut victim_buf = self.buffers[victim].lock();
                        let len = victim_buf.len();
                        if len == 0 {
                            None
                        } else {
                            let steal_count = std::cmp::max(1, len / 2);
                            let first = victim_buf.pop_front();
                            for _ in 1..steal_count {
                                if let Some(t) = victim_buf.pop_front() {
                                    stolen_extra.push(t);
                                }
                            }
                            first
                        }
                    };
                    if let Some(task) = first_stolen {
                        if !stolen_extra.is_empty() {
                            let mut my_buf = self.buffers[index].lock();
                            for t in stolen_extra {
                                my_buf.push_back(t);
                            }
                        }
                        return Ok(Some(task));
                    }
                }
            }

            // 3. Exit condition: only when in Draining state and all sources (global queue,
            // all worker buffers, and this worker's local queue) are completely empty!
            let is_draining = self.is_closed.load(Ordering::Acquire);
            if is_draining {
                let all_buffers_empty = self.buffers.iter().all(|b| b.lock().is_empty());
                let local_empty =
                    index >= self.local_receivers.len() || self.local_receivers[index].is_empty();
                if self.global_receiver.is_empty() && all_buffers_empty && local_empty {
                    return Err(pool_closed_err(py, "Pool is closed and drained"));
                }
                std::thread::yield_now();
                continue;
            }

            // Worker idle — increment park count.
            if let Some(ref metrics) = *self.metrics.read() {
                metrics.get().inc_park(index);
                let depth = self.global_receiver.len();
                sample_depth(metrics, index, depth);
            }
            return Ok(None);
        }
    }

    pub fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        for buf in &self.buffers {
            let Some(guard) = buf.try_lock() else {
                return Ok(());
            };
            for item in &*guard {
                visit.call(item)?;
            }
        }
        let Some(metrics_guard) = self.metrics.try_read() else {
            return Ok(());
        };
        if let Some(ref metrics) = *metrics_guard {
            visit.call(metrics)?;
        }
        Ok(())
    }

    pub fn __clear__(&self) {
        let mut global_guard = self.global_sender.lock();
        let dropped_global_sender = global_guard.take();
        drop(global_guard);

        let mut local_guard = self.local_senders.lock();
        let dropped_local_senders = std::mem::take(&mut *local_guard);
        drop(local_guard);

        self.is_closed.store(true, Ordering::Release);

        let mut drained = Vec::new();
        while let Ok(task) = self.global_receiver.try_recv() {
            drained.push(task);
        }
        for buf in &self.buffers {
            let mut guard = buf.lock();
            drained.extend(guard.drain(..));
            drop(guard);
        }
        for rx in &self.local_receivers {
            while let Ok(task) = rx.try_recv() {
                drained.push(task);
            }
        }
        let mut metrics_guard = self.metrics.write();
        let dropped_metrics = metrics_guard.take();
        drop(metrics_guard);

        drop(dropped_global_sender);
        drop(dropped_local_senders);
        drop(drained);
        drop(dropped_metrics);
    }
}
