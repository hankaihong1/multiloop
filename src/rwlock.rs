use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::waiter::{wake_pair_outside_lock, WaiterId};

pub(crate) struct RWMutexWaiter {
    pub(crate) id: WaiterId,
    pub(crate) task_token: u64,
    pub(crate) loop_obj: Py<PyAny>,
    pub(crate) fut_obj: Py<PyAny>,
}

enum RWMutexMode {
    Unlocked,
    WriteLocked {
        task_token: u64,
        ticket_id: Option<WaiterId>,
    },
    ReadLocked {
        total_readers: usize,
        reader_depths: HashMap<u64, usize>,
        woken_tickets: HashSet<WaiterId>,
    },
}

pub(crate) struct RWMutexInner {
    pub(crate) next_waiter_id: WaiterId,
    mode: RWMutexMode,
    pub(crate) read_waiters: VecDeque<RWMutexWaiter>,
    pub(crate) write_waiters: VecDeque<RWMutexWaiter>,
}

impl RWMutexInner {
    fn alloc_id(&mut self) -> WaiterId {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1).max(1);
        id
    }
}

/// High-performance asynchronous read-write lock state machine with writer-preference fairness.
#[pyclass(frozen, module = "multiloop._multiloop_core")]
pub struct RawAsyncRWMutex {
    state: Arc<Mutex<RWMutexInner>>,
}

impl Default for RawAsyncRWMutex {
    fn default() -> Self {
        Self::new()
    }
}

impl RawAsyncRWMutex {
    /// Wake the next writer or all pending readers outside the lock, rolling back if a target loop is closed.
    fn wake_next_after_release(&self, py: Python<'_>) {
        loop {
            enum Action {
                None,
                WakeWriter(RWMutexWaiter),
                WakeReaders(Vec<RWMutexWaiter>),
            }

            let action = {
                let mut guard = self.state.lock();
                if !matches!(guard.mode, RWMutexMode::Unlocked) {
                    Action::None
                } else if let Some(w) = guard.write_waiters.pop_front() {
                    guard.mode = RWMutexMode::WriteLocked {
                        task_token: w.task_token,
                        ticket_id: Some(w.id),
                    };
                    Action::WakeWriter(w)
                } else if !guard.read_waiters.is_empty() {
                    let readers: Vec<RWMutexWaiter> = guard.read_waiters.drain(..).collect();
                    let mut reader_depths: HashMap<u64, usize> =
                        HashMap::with_capacity(readers.len());
                    let mut woken_tickets: HashSet<WaiterId> =
                        HashSet::with_capacity(readers.len());
                    for r in &readers {
                        woken_tickets.insert(r.id);
                        *reader_depths.entry(r.task_token).or_insert(0) += 1;
                    }
                    let total_readers = reader_depths.len();
                    guard.mode = RWMutexMode::ReadLocked {
                        total_readers,
                        reader_depths,
                        woken_tickets,
                    };
                    Action::WakeReaders(readers)
                } else {
                    Action::None
                }
            };

            match action {
                Action::None => break,
                Action::WakeWriter(w) => {
                    if wake_pair_outside_lock(py, &w.loop_obj, &w.fut_obj) {
                        break;
                    }
                    let mut guard = self.state.lock();
                    if matches!(
                        guard.mode,
                        RWMutexMode::WriteLocked {
                            ticket_id: Some(tid),
                            ..
                        } if tid == w.id
                    ) {
                        guard.mode = RWMutexMode::Unlocked;
                    } else {
                        break;
                    }
                }
                Action::WakeReaders(readers) => {
                    let mut failed_readers = Vec::new();
                    for r in readers {
                        if !wake_pair_outside_lock(py, &r.loop_obj, &r.fut_obj) {
                            failed_readers.push((r.id, r.task_token));
                        }
                    }
                    if failed_readers.is_empty() {
                        break;
                    }
                    let should_retry = {
                        let mut guard = self.state.lock();
                        let transitioned_to_unlocked = match &mut guard.mode {
                            RWMutexMode::ReadLocked {
                                total_readers,
                                reader_depths,
                                woken_tickets,
                            } => {
                                for (rid, tok) in failed_readers {
                                    if woken_tickets.remove(&rid) {
                                        if let Some(d) = reader_depths.get_mut(&tok) {
                                            if *d > 1 {
                                                *d -= 1;
                                            } else {
                                                reader_depths.remove(&tok);
                                            }
                                        }
                                    }
                                }
                                *total_readers = reader_depths.len();
                                *total_readers == 0
                            }
                            _ => false,
                        };
                        if transitioned_to_unlocked {
                            guard.mode = RWMutexMode::Unlocked;
                        }
                        matches!(guard.mode, RWMutexMode::Unlocked)
                            && (!guard.write_waiters.is_empty() || !guard.read_waiters.is_empty())
                    };
                    if !should_retry {
                        break;
                    }
                }
            }
        }
    }
}

#[pymethods]
impl RawAsyncRWMutex {
    #[new]
    pub fn new() -> Self {
        RawAsyncRWMutex {
            state: Arc::new(Mutex::new(RWMutexInner {
                next_waiter_id: 1,
                mode: RWMutexMode::Unlocked,
                read_waiters: VecDeque::new(),
                write_waiters: VecDeque::new(),
            })),
        }
    }

    /// Fast-path non-blocking read acquisition with task reentrancy and write-holder deadlock check.
    pub fn try_acquire_read_task(&self, task_token: u64) -> PyResult<bool> {
        let mut guard = self.state.lock();
        let no_pending_writers = guard.write_waiters.is_empty();
        match &mut guard.mode {
            RWMutexMode::WriteLocked { task_token: wt, .. } => {
                if *wt == task_token {
                    Err(PyRuntimeError::new_err(
                        "AsyncRWMutex: reader() cannot be used while the same task holds writer()",
                    ))
                } else {
                    Ok(false)
                }
            }
            RWMutexMode::ReadLocked {
                total_readers,
                reader_depths,
                ..
            } => {
                if let Some(depth) = reader_depths.get_mut(&task_token) {
                    *depth += 1;
                    return Ok(true);
                }
                if no_pending_writers {
                    reader_depths.insert(task_token, 1);
                    *total_readers = reader_depths.len();
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            RWMutexMode::Unlocked => {
                if no_pending_writers {
                    let mut reader_depths = HashMap::new();
                    reader_depths.insert(task_token, 1);
                    guard.mode = RWMutexMode::ReadLocked {
                        total_readers: 1,
                        reader_depths,
                        woken_tickets: HashSet::new(),
                    };
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    /// Unified ticket-based read acquisition with task reentrancy tracking.
    /// Returns `Ok(None)` if acquired immediately (including reentrant read),
    /// or `Ok(Some(ticket_id))` if queued in `read_waiters`.
    pub fn acquire_read_ticket(
        &self,
        task_token: u64,
        loop_obj: Py<PyAny>,
        fut_obj: Py<PyAny>,
    ) -> PyResult<Option<WaiterId>> {
        let (res, _dropped_args) = {
            let mut guard = self.state.lock();
            let no_pending_writers = guard.write_waiters.is_empty();
            let acquired_or_err = match &mut guard.mode {
                RWMutexMode::WriteLocked { task_token: wt, .. } => {
                    if *wt == task_token {
                        Some(Err(PyRuntimeError::new_err(
                            "AsyncRWMutex: reader() cannot be used while the same task holds writer()",
                        )))
                    } else {
                        None
                    }
                }
                RWMutexMode::ReadLocked {
                    total_readers,
                    reader_depths,
                    ..
                } => {
                    if let Some(depth) = reader_depths.get_mut(&task_token) {
                        *depth += 1;
                        Some(Ok(None))
                    } else if no_pending_writers {
                        reader_depths.insert(task_token, 1);
                        *total_readers = reader_depths.len();
                        Some(Ok(None))
                    } else {
                        None
                    }
                }
                RWMutexMode::Unlocked => {
                    if no_pending_writers {
                        let mut reader_depths = HashMap::new();
                        reader_depths.insert(task_token, 1);
                        guard.mode = RWMutexMode::ReadLocked {
                            total_readers: 1,
                            reader_depths,
                            woken_tickets: HashSet::new(),
                        };
                        Some(Ok(None))
                    } else {
                        None
                    }
                }
            };

            match acquired_or_err {
                Some(result) => (result, Some((loop_obj, fut_obj))),
                None => {
                    let id = guard.alloc_id();
                    guard.read_waiters.push_back(RWMutexWaiter {
                        id,
                        task_token,
                        loop_obj,
                        fut_obj,
                    });
                    (Ok(Some(id)), None)
                }
            }
        };
        res
    }

    /// Claim a woken read ticket once the reader coroutine resumes from awaiting its Future.
    pub fn claim_read_ticket(&self, ticket_id: WaiterId) {
        let mut guard = self.state.lock();
        if let RWMutexMode::ReadLocked { woken_tickets, .. } = &mut guard.mode {
            woken_tickets.remove(&ticket_id);
        }
    }

    /// Cancel a pending or newly-woken read ticket and forward ownership if needed.
    pub fn cancel_read_ticket(&self, py: Python<'_>, task_token: u64, ticket_id: WaiterId) {
        let (need_wake, dropped_waiter) = {
            let mut guard = self.state.lock();
            if let Some(pos) = guard.read_waiters.iter().position(|w| w.id == ticket_id) {
                (false, guard.read_waiters.remove(pos))
            } else {
                let transitioned = match &mut guard.mode {
                    RWMutexMode::ReadLocked {
                        total_readers,
                        reader_depths,
                        woken_tickets,
                    } => {
                        if woken_tickets.remove(&ticket_id) {
                            if let Some(d) = reader_depths.get_mut(&task_token) {
                                if *d > 1 {
                                    *d -= 1;
                                } else {
                                    reader_depths.remove(&task_token);
                                }
                            }
                            *total_readers = reader_depths.len();
                            *total_readers == 0
                        } else {
                            false
                        }
                    }
                    _ => false,
                };
                if transitioned {
                    guard.mode = RWMutexMode::Unlocked;
                    (
                        !guard.write_waiters.is_empty() || !guard.read_waiters.is_empty(),
                        None,
                    )
                } else {
                    (false, None)
                }
            }
        };
        drop(dropped_waiter);
        if need_wake {
            self.wake_next_after_release(py);
        }
    }

    /// Release a task-tracked read lock (decrements reentrancy depth, wakes next writer when last reader exits).
    pub fn release_read_task(&self, py: Python<'_>, task_token: u64) -> PyResult<()> {
        let need_wake = {
            let mut guard = self.state.lock();
            let transitioned = match &mut guard.mode {
                RWMutexMode::ReadLocked {
                    total_readers,
                    reader_depths,
                    ..
                } => {
                    if let Some(depth) = reader_depths.get_mut(&task_token) {
                        if *depth > 1 {
                            *depth -= 1;
                            false
                        } else {
                            reader_depths.remove(&task_token);
                            *total_readers = reader_depths.len();
                            *total_readers == 0
                        }
                    } else {
                        return Err(PyRuntimeError::new_err(
                            "AsyncRWMutex: release_read called by a task that does not hold a read lock",
                        ));
                    }
                }
                RWMutexMode::Unlocked | RWMutexMode::WriteLocked { .. } => {
                    return Err(PyRuntimeError::new_err(
                        "AsyncRWMutex: release_read called by a task that does not hold a read lock",
                    ));
                }
            };
            if transitioned {
                guard.mode = RWMutexMode::Unlocked;
                !guard.write_waiters.is_empty() || !guard.read_waiters.is_empty()
            } else {
                false
            }
        };
        if need_wake {
            self.wake_next_after_release(py);
        }
        Ok(())
    }

    /// Fast-path non-blocking exclusive write acquisition with deadlock detection.
    pub fn try_acquire_write_task(&self, task_token: u64) -> PyResult<bool> {
        let mut guard = self.state.lock();
        let no_pending_writers = guard.write_waiters.is_empty();
        match &guard.mode {
            RWMutexMode::ReadLocked { reader_depths, .. } => {
                if reader_depths.contains_key(&task_token) {
                    Err(PyRuntimeError::new_err(
                        "AsyncRWMutex: writer() cannot be used while the same task holds reader()",
                    ))
                } else {
                    Ok(false)
                }
            }
            RWMutexMode::WriteLocked { task_token: wt, .. } => {
                if *wt == task_token {
                    Err(PyRuntimeError::new_err(
                        "AsyncRWMutex: writer() is not reentrant",
                    ))
                } else {
                    Ok(false)
                }
            }
            RWMutexMode::Unlocked => {
                if no_pending_writers {
                    guard.mode = RWMutexMode::WriteLocked {
                        task_token,
                        ticket_id: None,
                    };
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    /// Unified ticket-based write acquisition with task deadlock detection.
    /// Returns `Ok(None)` if acquired immediately, or `Ok(Some(ticket_id))` if queued.
    pub fn acquire_write_ticket(
        &self,
        task_token: u64,
        loop_obj: Py<PyAny>,
        fut_obj: Py<PyAny>,
    ) -> PyResult<Option<WaiterId>> {
        let (res, _dropped_args) = {
            let mut guard = self.state.lock();
            let no_pending_writers = guard.write_waiters.is_empty();
            let immediate = match &guard.mode {
                RWMutexMode::ReadLocked { reader_depths, .. } => {
                    if reader_depths.contains_key(&task_token) {
                        Some(Err(PyRuntimeError::new_err(
                            "AsyncRWMutex: writer() cannot be used while the same task holds reader()",
                        )))
                    } else {
                        None
                    }
                }
                RWMutexMode::WriteLocked { task_token: wt, .. } => {
                    if *wt == task_token {
                        Some(Err(PyRuntimeError::new_err(
                            "AsyncRWMutex: writer() is not reentrant",
                        )))
                    } else {
                        None
                    }
                }
                RWMutexMode::Unlocked => {
                    if no_pending_writers {
                        guard.mode = RWMutexMode::WriteLocked {
                            task_token,
                            ticket_id: None,
                        };
                        Some(Ok(None))
                    } else {
                        None
                    }
                }
            };

            match immediate {
                Some(result) => (result, Some((loop_obj, fut_obj))),
                None => {
                    let id = guard.alloc_id();
                    guard.write_waiters.push_back(RWMutexWaiter {
                        id,
                        task_token,
                        loop_obj,
                        fut_obj,
                    });
                    (Ok(Some(id)), None)
                }
            }
        };
        res
    }

    /// Cancel a pending or newly-woken write ticket and forward ownership if needed.
    pub fn cancel_write_ticket(&self, py: Python<'_>, _task_token: u64, ticket_id: WaiterId) {
        let (need_wake, dropped_waiter) = {
            let mut guard = self.state.lock();
            if let Some(pos) = guard.write_waiters.iter().position(|w| w.id == ticket_id) {
                let removed = guard.write_waiters.remove(pos);
                let wake = matches!(guard.mode, RWMutexMode::Unlocked)
                    && guard.write_waiters.is_empty()
                    && !guard.read_waiters.is_empty();
                (wake, removed)
            } else if matches!(
                guard.mode,
                RWMutexMode::WriteLocked {
                    ticket_id: Some(tid),
                    ..
                } if tid == ticket_id
            ) {
                guard.mode = RWMutexMode::Unlocked;
                (
                    !guard.write_waiters.is_empty() || !guard.read_waiters.is_empty(),
                    None,
                )
            } else {
                (false, None)
            }
        };
        drop(dropped_waiter);
        if need_wake {
            self.wake_next_after_release(py);
        }
    }

    /// Release a task-tracked exclusive write lock and wake next writer or all pending readers.
    pub fn release_write_task(&self, py: Python<'_>, task_token: u64) -> PyResult<()> {
        {
            let mut guard = self.state.lock();
            if !matches!(
                guard.mode,
                RWMutexMode::WriteLocked { task_token: wt, .. } if wt == task_token
            ) {
                return Err(PyRuntimeError::new_err(
                    "AsyncRWMutex: release_write called by a task that does not hold the write lock",
                ));
            }
            guard.mode = RWMutexMode::Unlocked;
        }
        self.wake_next_after_release(py);
        Ok(())
    }

    /// Number of woken reader tickets not yet claimed by their tasks.
    pub fn woken_tickets_len(&self) -> usize {
        let guard = self.state.lock();
        match &guard.mode {
            RWMutexMode::ReadLocked { woken_tickets, .. } => woken_tickets.len(),
            _ => 0,
        }
    }

    /// Get current state snapshot: (readers, has_writer, pending_writers)
    pub fn snapshot(&self) -> (usize, bool, usize) {
        let guard = self.state.lock();
        let pending_writers = guard.write_waiters.len();
        match &guard.mode {
            RWMutexMode::Unlocked => (0, false, pending_writers),
            RWMutexMode::WriteLocked { .. } => (0, true, pending_writers),
            RWMutexMode::ReadLocked { total_readers, .. } => {
                (*total_readers, false, pending_writers)
            }
        }
    }
}
