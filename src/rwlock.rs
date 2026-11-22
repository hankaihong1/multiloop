use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::pyclass::{PyTraverseError, PyVisit};
use std::collections::{HashMap, VecDeque};
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
        woken_tickets: HashMap<WaiterId, u64>,
    },
}

enum RWAction {
    None,
    WakeWriter(RWMutexWaiter),
    WakeReaders(Vec<RWMutexWaiter>),
}

#[inline]
fn admit_readers(
    readers: &[RWMutexWaiter],
    reader_depths: &mut HashMap<u64, usize>,
    woken_tickets: &mut HashMap<WaiterId, u64>,
) -> usize {
    for r in readers {
        woken_tickets.insert(r.id, r.task_token);
        *reader_depths.entry(r.task_token).or_insert(0) += 1;
    }
    reader_depths.len()
}

#[inline]
fn decrement_reader_depth(
    reader_depths: &mut HashMap<u64, usize>,
    task_token: u64,
) -> Option<usize> {
    let depth = reader_depths.get_mut(&task_token)?;
    if *depth > 1 {
        *depth -= 1;
    } else {
        reader_depths.remove(&task_token);
    }
    Some(reader_depths.len())
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

    /// Promote queued writers or readers under the mutex and return any wakeups to run outside.
    fn prepare_wake_action(&mut self) -> RWAction {
        match &mut self.mode {
            RWMutexMode::Unlocked => {
                if let Some(w) = self.write_waiters.pop_front() {
                    self.mode = RWMutexMode::WriteLocked {
                        task_token: w.task_token,
                        ticket_id: Some(w.id),
                    };
                    RWAction::WakeWriter(w)
                } else if !self.read_waiters.is_empty() {
                    let readers: Vec<RWMutexWaiter> = self.read_waiters.drain(..).collect();
                    let mut reader_depths: HashMap<u64, usize> =
                        HashMap::with_capacity(readers.len());
                    let mut woken_tickets: HashMap<WaiterId, u64> =
                        HashMap::with_capacity(readers.len());
                    let total_readers =
                        admit_readers(&readers, &mut reader_depths, &mut woken_tickets);
                    self.mode = RWMutexMode::ReadLocked {
                        total_readers,
                        reader_depths,
                        woken_tickets,
                    };
                    RWAction::WakeReaders(readers)
                } else {
                    RWAction::None
                }
            }
            RWMutexMode::ReadLocked {
                total_readers,
                reader_depths,
                woken_tickets,
            } => {
                if self.write_waiters.is_empty() && !self.read_waiters.is_empty() {
                    let readers: Vec<RWMutexWaiter> = self.read_waiters.drain(..).collect();
                    *total_readers = admit_readers(&readers, reader_depths, woken_tickets);
                    RWAction::WakeReaders(readers)
                } else {
                    RWAction::None
                }
            }
            RWMutexMode::WriteLocked { .. } => RWAction::None,
        }
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
    /// Wake promoted waiters outside the lock, rolling back and advancing on closed loops.
    fn dispatch_wake_action(&self, py: Python<'_>, mut action: RWAction) {
        loop {
            match action {
                RWAction::None => break,
                RWAction::WakeWriter(w) => {
                    let ok = wake_pair_outside_lock(py, &w.loop_obj, &w.fut_obj);
                    let wid = w.id;
                    drop(w);
                    if ok {
                        break;
                    }
                    let mut guard = self.state.lock();
                    if matches!(
                        guard.mode,
                        RWMutexMode::WriteLocked {
                            ticket_id: Some(tid),
                            ..
                        } if tid == wid
                    ) {
                        guard.mode = RWMutexMode::Unlocked;
                        action = guard.prepare_wake_action();
                    } else {
                        break;
                    }
                }
                RWAction::WakeReaders(readers) => {
                    let mut failed_readers = Vec::new();
                    for r in readers {
                        if !wake_pair_outside_lock(py, &r.loop_obj, &r.fut_obj) {
                            failed_readers.push(r.id);
                        }
                    }
                    if failed_readers.is_empty() {
                        break;
                    }
                    action = {
                        let mut guard = self.state.lock();
                        let transitioned_to_unlocked = match &mut guard.mode {
                            RWMutexMode::ReadLocked {
                                total_readers,
                                reader_depths,
                                woken_tickets,
                            } => {
                                for rid in failed_readers {
                                    if let Some(tok) = woken_tickets.remove(&rid) {
                                        if let Some(new_total) =
                                            decrement_reader_depth(reader_depths, tok)
                                        {
                                            *total_readers = new_total;
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
                        guard.prepare_wake_action()
                    };
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
                        woken_tickets: HashMap::new(),
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
                            woken_tickets: HashMap::new(),
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
        let _ = task_token;
        let (action, dropped_waiter) = {
            let mut guard = self.state.lock();
            if let Some(pos) = guard.read_waiters.iter().position(|w| w.id == ticket_id) {
                let removed = guard.read_waiters.remove(pos);
                let action = guard.prepare_wake_action();
                (action, removed)
            } else {
                let transitioned = match &mut guard.mode {
                    RWMutexMode::ReadLocked {
                        total_readers,
                        reader_depths,
                        woken_tickets,
                    } => {
                        if let Some(owner_tok) = woken_tickets.remove(&ticket_id) {
                            if let Some(new_total) =
                                decrement_reader_depth(reader_depths, owner_tok)
                            {
                                *total_readers = new_total;
                                new_total == 0
                            } else {
                                *total_readers = reader_depths.len();
                                *total_readers == 0
                            }
                        } else {
                            false
                        }
                    }
                    _ => false,
                };
                if transitioned {
                    guard.mode = RWMutexMode::Unlocked;
                }
                (guard.prepare_wake_action(), None)
            }
        };
        drop(dropped_waiter);
        self.dispatch_wake_action(py, action);
    }

    /// Release a task-tracked read lock (decrements reentrancy depth, wakes next writer when last reader exits).
    pub fn release_read_task(&self, py: Python<'_>, task_token: u64) -> PyResult<()> {
        let action = {
            let mut guard = self.state.lock();
            let transitioned = match &mut guard.mode {
                RWMutexMode::ReadLocked {
                    total_readers,
                    reader_depths,
                    ..
                } => {
                    if let Some(new_total) = decrement_reader_depth(reader_depths, task_token) {
                        *total_readers = new_total;
                        new_total == 0
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
            }
            guard.prepare_wake_action()
        };
        self.dispatch_wake_action(py, action);
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
        let (action, dropped_waiter) = {
            let mut guard = self.state.lock();
            if let Some(pos) = guard.write_waiters.iter().position(|w| w.id == ticket_id) {
                let removed = guard.write_waiters.remove(pos);
                let action = guard.prepare_wake_action();
                (action, removed)
            } else if matches!(
                guard.mode,
                RWMutexMode::WriteLocked {
                    ticket_id: Some(tid),
                    ..
                } if tid == ticket_id
            ) {
                guard.mode = RWMutexMode::Unlocked;
                let action = guard.prepare_wake_action();
                (action, None)
            } else {
                (RWAction::None, None)
            }
        };
        drop(dropped_waiter);
        self.dispatch_wake_action(py, action);
    }

    /// Release a task-tracked exclusive write lock and wake next writer or all pending readers.
    pub fn release_write_task(&self, py: Python<'_>, task_token: u64) -> PyResult<()> {
        let action = {
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
            guard.prepare_wake_action()
        };
        self.dispatch_wake_action(py, action);
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

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let Some(guard) = self.state.try_lock() else {
            return Ok(());
        };
        for w in &guard.read_waiters {
            visit.call(&w.loop_obj)?;
            visit.call(&w.fut_obj)?;
        }
        for w in &guard.write_waiters {
            visit.call(&w.loop_obj)?;
            visit.call(&w.fut_obj)?;
        }
        Ok(())
    }

    fn __clear__(&self) {
        let mut guard = self.state.lock();
        let read_waiters = std::mem::take(&mut guard.read_waiters);
        let write_waiters = std::mem::take(&mut guard.write_waiters);
        drop(guard);
        drop(read_waiters);
        drop(write_waiters);
    }
}
