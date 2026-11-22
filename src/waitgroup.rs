use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::pyclass::{PyTraverseError, PyVisit};
use std::sync::Arc;

use crate::waiter::{wake_pair_outside_lock, WaiterEntry, WaiterId};

pub(crate) struct WaitGroupInner {
    pub(crate) next_waiter_id: WaiterId,
    pub(crate) counter: usize,
    pub(crate) waiters: Vec<WaiterEntry>,
}

impl WaitGroupInner {
    fn alloc_id(&mut self) -> WaiterId {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1);
        id
    }
}

#[pyclass(frozen, module = "multiloop._multiloop_core")]
pub struct RawAsyncWaitGroup {
    pub(crate) state: Arc<Mutex<WaitGroupInner>>,
}

impl Default for RawAsyncWaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl RawAsyncWaitGroup {
    #[new]
    pub fn new() -> Self {
        RawAsyncWaitGroup {
            state: Arc::new(Mutex::new(WaitGroupInner {
                next_waiter_id: 1,
                counter: 0,
                waiters: Vec::new(),
            })),
        }
    }

    /// Add `delta` to the counter (Go `sync.WaitGroup.Add` semantics).
    ///
    /// Negative deltas are legal, but the counter must never go below zero:
    /// an `add()` that would underflow raises `RuntimeError`.
    /// Wakes registered waiters directly outside the lock when counter reaches 0.
    pub fn add(&self, py: Python<'_>, delta: i64) -> PyResult<usize> {
        let waiters = {
            let mut guard = self.state.lock();
            if delta < 0 {
                let magnitude = delta.unsigned_abs() as usize;
                if guard.counter < magnitude {
                    return Err(PyRuntimeError::new_err(
                        "WaitGroup counter went negative: add() with negative delta",
                    ));
                }
                guard.counter -= magnitude;
                if guard.counter == 0 {
                    std::mem::take(&mut guard.waiters)
                } else {
                    Vec::new()
                }
            } else {
                match guard.counter.checked_add(delta as usize) {
                    Some(v) => guard.counter = v,
                    None => {
                        return Err(PyRuntimeError::new_err(
                            "WaitGroup counter overflowed: add() with positive delta",
                        ));
                    }
                }
                Vec::new()
            }
        };
        let count = waiters.len();
        for w in waiters {
            wake_pair_outside_lock(py, &w.event_loop, &w.future);
        }
        Ok(count)
    }

    pub fn done(&self, py: Python<'_>) -> PyResult<usize> {
        let waiters = {
            let mut guard = self.state.lock();
            if guard.counter == 0 {
                return Err(PyRuntimeError::new_err(
                    "WaitGroup counter went negative: too many done() calls",
                ));
            }
            guard.counter -= 1;
            if guard.counter == 0 {
                std::mem::take(&mut guard.waiters)
            } else {
                Vec::new()
            }
        };
        let count = waiters.len();
        for w in waiters {
            wake_pair_outside_lock(py, &w.event_loop, &w.future);
        }
        Ok(count)
    }

    pub fn is_zero(&self) -> bool {
        self.state.lock().counter == 0
    }

    pub fn waiter_count(&self) -> usize {
        self.state.lock().waiters.len()
    }

    /// Registers a waiter using the unified `WaiterId (u64)` ticket protocol.
    ///
    /// Returns `None` if the counter is already zero (caller proceeds immediately),
    /// or `Some(ticket_id)` if registered.
    pub fn register_waiter_ticket(
        &self,
        loop_obj: Py<PyAny>,
        fut_obj: Py<PyAny>,
    ) -> Option<WaiterId> {
        let mut guard = self.state.lock();
        if guard.counter == 0 {
            None
        } else {
            let id = guard.alloc_id();
            guard.waiters.push(WaiterEntry::new(id, loop_obj, fut_obj));
            Some(id)
        }
    }

    /// Removes a previously registered waiter by `WaiterId` ticket.
    pub fn cancel_waiter_ticket(&self, ticket_id: WaiterId) -> bool {
        let mut guard = self.state.lock();
        let removed = guard
            .waiters
            .iter()
            .position(|w| w.id == ticket_id)
            .map(|pos| guard.waiters.swap_remove(pos));
        drop(guard);
        let was_present = removed.is_some();
        drop(removed);
        was_present
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let Some(guard) = self.state.try_lock() else {
            return Ok(());
        };
        for w in &guard.waiters {
            visit.call(&w.event_loop)?;
            visit.call(&w.future)?;
            visit.call(w.channel_token.as_ref())?;
        }
        Ok(())
    }

    fn __clear__(&self) {
        let mut guard = self.state.lock();
        let waiters = std::mem::take(&mut guard.waiters);
        drop(guard);
        drop(waiters);
    }
}
