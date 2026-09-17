use parking_lot::Mutex;
use pyo3::prelude::*;
use std::collections::VecDeque;
use std::sync::Arc;

/// A registered waiter: (event loop, future) pair used to wake a suspended coroutine.
pub(crate) type Waiter = (Py<PyAny>, Py<PyAny>);

pub(crate) struct RWMutexInner {
    pub(crate) readers: usize,
    pub(crate) writer: bool,
    pub(crate) pending_writers: usize,
    pub(crate) read_waiters: VecDeque<Waiter>,
    pub(crate) write_waiters: VecDeque<Waiter>,
}

/// High-performance asynchronous read-write lock state machine with writer-preference fairness.
#[pyclass(module = "multiloop._multiloop_core")]
pub struct RawAsyncRWMutex {
    state: Arc<Mutex<RWMutexInner>>,
}

impl Default for RawAsyncRWMutex {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl RawAsyncRWMutex {
    #[new]
    pub fn new() -> Self {
        RawAsyncRWMutex {
            state: Arc::new(Mutex::new(RWMutexInner {
                readers: 0,
                writer: false,
                pending_writers: 0,
                read_waiters: VecDeque::new(),
                write_waiters: VecDeque::new(),
            })),
        }
    }

    /// Try to acquire a read lock without registering a waiter.
    pub fn try_acquire_read(&self) -> bool {
        let mut guard = self.state.lock();
        if !guard.writer && guard.pending_writers == 0 {
            guard.readers += 1;
            true
        } else {
            false
        }
    }

    /// Try to acquire a read lock, or register a waiter in the read queue.
    pub fn acquire_read_or_register(&self, loop_obj: Py<PyAny>, fut_obj: Py<PyAny>) -> bool {
        let mut guard = self.state.lock();
        if !guard.writer && guard.pending_writers == 0 {
            guard.readers += 1;
            true
        } else {
            guard.read_waiters.push_back((loop_obj, fut_obj));
            false
        }
    }

    /// Release one reader. If this was the last reader and writers are waiting,
    /// returns the next writer to wake.
    pub fn release_read(&self) -> Option<Vec<Waiter>> {
        let mut guard = self.state.lock();
        if guard.readers > 0 {
            guard.readers -= 1;
        }
        if guard.readers == 0 && !guard.write_waiters.is_empty() {
            let next_writer = guard.write_waiters.pop_front().unwrap();
            guard.writer = true;
            if guard.pending_writers > 0 {
                guard.pending_writers -= 1;
            }
            Some(vec![next_writer])
        } else {
            None
        }
    }

    /// Try to acquire an exclusive write lock without registering a waiter.
    pub fn try_acquire_write(&self) -> bool {
        let mut guard = self.state.lock();
        if !guard.writer && guard.readers == 0 && guard.pending_writers == 0 {
            guard.writer = true;
            true
        } else {
            false
        }
    }

    /// Try to acquire an exclusive write lock, or register a waiter in the write queue.
    pub fn acquire_write_or_register(&self, loop_obj: Py<PyAny>, fut_obj: Py<PyAny>) -> bool {
        let mut guard = self.state.lock();
        if !guard.writer && guard.readers == 0 && guard.pending_writers == 0 {
            guard.writer = true;
            true
        } else {
            guard.pending_writers += 1;
            guard.write_waiters.push_back((loop_obj, fut_obj));
            false
        }
    }

    /// Release the exclusive writer. Wakes the next pending writer (if any)
    /// or all pending readers.
    pub fn release_write(&self) -> Option<Vec<Waiter>> {
        let mut guard = self.state.lock();
        guard.writer = false;
        if !guard.write_waiters.is_empty() {
            let next_writer = guard.write_waiters.pop_front().unwrap();
            guard.writer = true;
            if guard.pending_writers > 0 {
                guard.pending_writers -= 1;
            }
            Some(vec![next_writer])
        } else if !guard.read_waiters.is_empty() {
            let readers = std::mem::take(&mut guard.read_waiters);
            guard.readers += readers.len();
            Some(readers.into())
        } else {
            None
        }
    }

    /// Remove a cancelled waiter and adjust pending counters.
    /// If removing a writer unblocks readers, returns those readers to wake.
    pub fn remove_waiter(
        &self,
        py: Python<'_>,
        _loop_obj: &Bound<'_, PyAny>,
        fut_obj: &Bound<'_, PyAny>,
        is_writer: bool,
    ) -> Option<Vec<Waiter>> {
        let mut guard = self.state.lock();
        if is_writer {
            let initial_len = guard.write_waiters.len();
            guard.write_waiters.retain(|(_, f)| !f.bind(py).is(fut_obj));
            let removed = initial_len - guard.write_waiters.len();
            if removed > 0 {
                if guard.pending_writers >= removed {
                    guard.pending_writers -= removed;
                }
                if guard.pending_writers == 0 && !guard.writer && !guard.read_waiters.is_empty() {
                    let readers = std::mem::take(&mut guard.read_waiters);
                    guard.readers += readers.len();
                    return Some(readers.into());
                }
            } else if guard.writer {
                // The waiter was already popped and ownership was transferred to it (guard.writer == true).
                // Since this writer was cancelled before entering the critical section,
                // it must release the write lock and wake the next waiter (writer or readers).
                guard.writer = false;
                if !guard.write_waiters.is_empty() {
                    let next_writer = guard.write_waiters.pop_front().unwrap();
                    guard.writer = true;
                    if guard.pending_writers > 0 {
                        guard.pending_writers -= 1;
                    }
                    return Some(vec![next_writer]);
                } else if !guard.read_waiters.is_empty() {
                    let readers = std::mem::take(&mut guard.read_waiters);
                    guard.readers += readers.len();
                    return Some(readers.into());
                }
            }
        } else {
            let initial_len = guard.read_waiters.len();
            guard.read_waiters.retain(|(_, f)| !f.bind(py).is(fut_obj));
            let removed = initial_len - guard.read_waiters.len();
            if removed == 0 {
                // This reader was already popped and granted a read permit (guard.readers was incremented).
                // Since this reader was cancelled before entering the critical section,
                // it must decrement guard.readers.
                if guard.readers > 0 {
                    guard.readers -= 1;
                }
                if guard.readers == 0 && !guard.write_waiters.is_empty() {
                    let next_writer = guard.write_waiters.pop_front().unwrap();
                    guard.writer = true;
                    if guard.pending_writers > 0 {
                        guard.pending_writers -= 1;
                    }
                    return Some(vec![next_writer]);
                }
            }
        }
        None
    }

    /// Get current state snapshot: (readers, has_writer, pending_writers)
    pub fn snapshot(&self) -> (usize, bool, usize) {
        let guard = self.state.lock();
        (guard.readers, guard.writer, guard.pending_writers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rwlock_basic_read_write_exclusion() {
        Python::attach(|_py| {
            let lock = RawAsyncRWMutex::new();
            assert!(lock.try_acquire_read());
            assert!(lock.try_acquire_read());
            assert_eq!(lock.snapshot().0, 2);

            // Write attempt fails while readers are active
            assert!(!lock.try_acquire_write());

            assert!(lock.release_read().is_none());
            assert!(lock.release_read().is_none());
            assert_eq!(lock.snapshot().0, 0);

            // Now write succeeds
            assert!(lock.try_acquire_write());
            assert_eq!(lock.snapshot().1, true);

            // Read fails while writer active
            assert!(!lock.try_acquire_read());

            assert!(lock.release_write().is_none());
            assert_eq!(lock.snapshot().1, false);
        });
    }

    #[test]
    fn test_rwlock_cancellation_ownership_transfer() {
        Python::attach(|py| {
            let lock = RawAsyncRWMutex::new();
            assert!(lock.try_acquire_write());

            let loop_obj = py.None();
            let fut1: Py<PyAny> = pyo3::types::PyString::new(py, "fut1").into_any().unbind();
            let fut2: Py<PyAny> = pyo3::types::PyString::new(py, "fut2").into_any().unbind();

            // Register writer 1 and writer 2
            assert!(!lock.acquire_write_or_register(loop_obj.clone_ref(py), fut1.clone_ref(py)));
            assert!(!lock.acquire_write_or_register(loop_obj.clone_ref(py), fut2.clone_ref(py)));
            assert_eq!(lock.snapshot().2, 2);

            // Release initial writer: wakes fut1 and assigns guard.writer = true
            let wakers1 = lock.release_write().unwrap();
            assert_eq!(wakers1.len(), 1);
            assert_eq!(lock.snapshot().1, true);
            assert_eq!(lock.snapshot().2, 1);

            // Cancel fut1 (simulated by remove_waiter with fut1):
            // Should detect ownership transfer and forward lock to fut2!
            let wakers2 = lock
                .remove_waiter(py, loop_obj.bind(py), fut1.bind(py), true)
                .unwrap();
            assert_eq!(wakers2.len(), 1);
            assert_eq!(lock.snapshot().1, true); // handed to fut2
            assert_eq!(lock.snapshot().2, 0);

            // Cancel fut2: should release lock and leave writer = false
            let wakers3 = lock.remove_waiter(py, loop_obj.bind(py), fut2.bind(py), true);
            assert!(wakers3.is_none());
            assert_eq!(lock.snapshot().1, false);

            // Now write lock can be acquired freshly
            assert!(lock.try_acquire_write());
        });
    }
}
