use parking_lot::Mutex;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pyclass::{PyTraverseError, PyVisit};
use pyo3::types::{PyInt, PyWeakrefMethods, PyWeakrefReference};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use crate::waiter::{wake_pair_outside_lock, wake_waiter_outside_lock, WaiterEntry, WaiterId};

#[inline]
fn visit_waiter_entry(w: &WaiterEntry, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
    visit.call(&w.event_loop)?;
    visit.call(&w.future)?;
    visit.call(w.channel_token.as_ref())?;
    Ok(())
}

// ============================================================================
// RawAsyncLock — fair FIFO task-owned mutex
// ============================================================================

struct LockWaiter {
    id: WaiterId,
    task_token: u64,
    loop_id: usize,
    task_weak: Py<PyWeakrefReference>,
    loop_obj: Py<PyAny>,
    fut_obj: Py<PyAny>,
}

enum LockState {
    Unlocked,
    Locked {
        task_weak: Py<PyWeakrefReference>,
        task_token: u64,
        loop_id: usize,
        ticket_id: Option<WaiterId>,
    },
}

struct LockInner {
    next_waiter_id: WaiterId,
    lock_state: LockState,
    waiters: VecDeque<LockWaiter>,
}

impl LockInner {
    fn alloc_id(&mut self) -> WaiterId {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1).max(1);
        id
    }

    fn handoff_locked(&mut self, py: Python<'_>) -> (LockState, Option<LockWaiter>) {
        if let Some(w) = self.waiters.pop_front() {
            let new_state = LockState::Locked {
                task_weak: w.task_weak.clone_ref(py),
                task_token: w.task_token,
                loop_id: w.loop_id,
                ticket_id: Some(w.id),
            };
            let old_state = std::mem::replace(&mut self.lock_state, new_state);
            (old_state, Some(w))
        } else {
            let old_state = std::mem::replace(&mut self.lock_state, LockState::Unlocked);
            (old_state, None)
        }
    }
}

#[pyclass(frozen, module = "multiloop._multiloop_core")]
pub struct RawAsyncLock {
    state: Arc<Mutex<LockInner>>,
}

impl Default for RawAsyncLock {
    fn default() -> Self {
        Self::new()
    }
}

impl RawAsyncLock {
    fn wake_and_advance_on_closed_loop(&self, py: Python<'_>, mut next_waiter: Option<LockWaiter>) {
        while let Some(w) = next_waiter {
            if wake_pair_outside_lock(py, &w.loop_obj, &w.fut_obj) {
                break;
            }
            let (old_state, next) = {
                let mut guard = self.state.lock();
                if matches!(
                    guard.lock_state,
                    LockState::Locked {
                        ticket_id: Some(tid),
                        ..
                    } if tid == w.id
                ) {
                    guard.handoff_locked(py)
                } else {
                    break;
                }
            };
            drop(old_state);
            next_waiter = next;
        }
    }
}

#[pymethods]
impl RawAsyncLock {
    #[new]
    pub fn new() -> Self {
        RawAsyncLock {
            state: Arc::new(Mutex::new(LockInner {
                next_waiter_id: 1,
                lock_state: LockState::Unlocked,
                waiters: VecDeque::new(),
            })),
        }
    }

    pub fn locked(&self, py: Python<'_>) -> bool {
        loop {
            let locked_ref = {
                let guard = self.state.lock();
                match &guard.lock_state {
                    LockState::Unlocked => return false,
                    LockState::Locked {
                        task_weak,
                        task_token,
                        ..
                    } => (task_weak.clone_ref(py), *task_token),
                }
            };
            let (task_weak, task_token) = locked_ref;
            let is_alive = task_weak.bind(py).upgrade().is_some();
            drop(task_weak);
            if is_alive {
                return true;
            }
            self.on_owner_task_done(py, task_token);
        }
    }

    #[pyo3(signature = (caller_loop_id = None))]
    pub fn owner(&self, py: Python<'_>, caller_loop_id: Option<usize>) -> Option<Py<PyAny>> {
        let locked_ref = {
            let guard = self.state.lock();
            match &guard.lock_state {
                LockState::Locked {
                    task_weak,
                    task_token,
                    loop_id,
                    ..
                } => Some((task_weak.clone_ref(py), *task_token, *loop_id)),
                LockState::Unlocked => None,
            }
        };
        let (task_weak, task_token, owner_loop_id) = locked_ref?;
        match task_weak.bind(py).upgrade() {
            Some(task_obj) => {
                if caller_loop_id == Some(owner_loop_id) {
                    let is_done = task_obj
                        .call_method0("done")
                        .and_then(|d| d.extract::<bool>())
                        .unwrap_or(false);
                    if is_done {
                        drop(task_obj);
                        drop(task_weak);
                        self.on_owner_task_done(py, task_token);
                        return None;
                    }
                }
                Some(task_obj.unbind())
            }
            None => {
                drop(task_weak);
                self.on_owner_task_done(py, task_token);
                None
            }
        }
    }

    pub fn waiter_count(&self) -> usize {
        self.state.lock().waiters.len()
    }

    /// If the lock is currently held by a task on `caller_loop_id`, return that task object
    /// so the caller (running on that same event loop thread) can safely check `task.done()`.
    pub fn check_owner_same_loop(
        &self,
        py: Python<'_>,
        caller_loop_id: usize,
    ) -> Option<Py<PyAny>> {
        let locked_ref = {
            let guard = self.state.lock();
            match &guard.lock_state {
                LockState::Locked {
                    task_weak,
                    task_token,
                    loop_id,
                    ..
                } if *loop_id == caller_loop_id => Some((task_weak.clone_ref(py), *task_token)),
                _ => None,
            }
        };
        let (task_weak, task_token) = locked_ref?;
        match task_weak.bind(py).upgrade() {
            Some(task_obj) => Some(task_obj.unbind()),
            None => {
                drop(task_weak);
                self.on_owner_task_done(py, task_token);
                None
            }
        }
    }

    /// Invoked when the owning task finishes without calling `release()`.
    pub fn on_owner_task_done(&self, py: Python<'_>, task_token: u64) {
        let (old_state, next_waiter) = {
            let mut guard = self.state.lock();
            let is_owner = matches!(
                guard.lock_state,
                LockState::Locked {
                    task_token: t,
                    ..
                } if t == task_token
            );
            if !is_owner {
                return;
            }
            guard.handoff_locked(py)
        };
        drop(old_state);
        self.wake_and_advance_on_closed_loop(py, next_waiter);
    }

    pub fn try_acquire(
        &self,
        task_token: u64,
        loop_id: usize,
        task_weak: Py<PyWeakrefReference>,
    ) -> PyResult<bool> {
        let (res, _dropped_state, _dropped_arg) = {
            let mut guard = self.state.lock();
            match &guard.lock_state {
                LockState::Locked {
                    task_token: owner_tok,
                    ..
                } => {
                    if *owner_tok == task_token {
                        (
                            Err(PyRuntimeError::new_err(
                                "Lock is not reentrant: already held by the current task",
                            )),
                            None,
                            Some(task_weak),
                        )
                    } else {
                        (Ok(false), None, Some(task_weak))
                    }
                }
                LockState::Unlocked => {
                    if guard.waiters.is_empty() {
                        let old = std::mem::replace(
                            &mut guard.lock_state,
                            LockState::Locked {
                                task_weak,
                                task_token,
                                loop_id,
                                ticket_id: None,
                            },
                        );
                        (Ok(true), Some(old), None)
                    } else {
                        (Ok(false), None, Some(task_weak))
                    }
                }
            }
        };
        res
    }

    pub fn acquire_or_register(
        &self,
        task_token: u64,
        loop_id: usize,
        task_weak: Py<PyWeakrefReference>,
        loop_obj: Py<PyAny>,
        fut_obj: Py<PyAny>,
    ) -> PyResult<Option<WaiterId>> {
        let (res, _dropped_state, _dropped_args) = {
            let mut guard = self.state.lock();
            match &guard.lock_state {
                LockState::Locked {
                    task_token: owner_tok,
                    ..
                } if *owner_tok == task_token => (
                    Err(PyRuntimeError::new_err(
                        "Lock is not reentrant: already held by the current task",
                    )),
                    None,
                    Some((Some(task_weak), loop_obj, fut_obj)),
                ),
                LockState::Unlocked if guard.waiters.is_empty() => {
                    let old = std::mem::replace(
                        &mut guard.lock_state,
                        LockState::Locked {
                            task_weak,
                            task_token,
                            loop_id,
                            ticket_id: None,
                        },
                    );
                    (Ok(None), Some(old), Some((None, loop_obj, fut_obj)))
                }
                _ => {
                    let id = guard.alloc_id();
                    guard.waiters.push_back(LockWaiter {
                        id,
                        task_token,
                        loop_id,
                        task_weak,
                        loop_obj,
                        fut_obj,
                    });
                    (Ok(Some(id)), None, None)
                }
            }
        };
        res
    }

    pub fn cancel_waiter(&self, py: Python<'_>, _task_token: u64, ticket_id: WaiterId) {
        let (dropped_waiter, old_state, next_waiter) = {
            let mut guard = self.state.lock();
            if let Some(pos) = guard.waiters.iter().position(|w| w.id == ticket_id) {
                (guard.waiters.remove(pos), None, None)
            } else if matches!(
                guard.lock_state,
                LockState::Locked {
                    ticket_id: Some(tid),
                    ..
                } if tid == ticket_id
            ) {
                let (old, next) = guard.handoff_locked(py);
                (None, Some(old), next)
            } else {
                (None, None, None)
            }
        };
        drop(dropped_waiter);
        drop(old_state);
        self.wake_and_advance_on_closed_loop(py, next_waiter);
    }

    pub fn release(&self, py: Python<'_>, task_token: u64) -> PyResult<()> {
        let (old_state, next_waiter) = {
            let mut guard = self.state.lock();
            let is_owner = matches!(
                guard.lock_state,
                LockState::Locked {
                    task_token: t,
                    ..
                } if t == task_token
            );
            if !is_owner {
                return Err(PyRuntimeError::new_err(
                    "Lock.release() called by a task that does not own the lock",
                ));
            }
            guard.handoff_locked(py)
        };
        drop(old_state);
        self.wake_and_advance_on_closed_loop(py, next_waiter);
        Ok(())
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let Some(guard) = self.state.try_lock() else {
            return Ok(());
        };
        if let LockState::Locked { task_weak, .. } = &guard.lock_state {
            visit.call(task_weak)?;
        }
        for w in &guard.waiters {
            visit.call(&w.task_weak)?;
            visit.call(&w.loop_obj)?;
            visit.call(&w.fut_obj)?;
        }
        Ok(())
    }

    fn __clear__(&self) {
        let mut guard = self.state.lock();
        let old_state = std::mem::replace(&mut guard.lock_state, LockState::Unlocked);
        let waiters = std::mem::take(&mut guard.waiters);
        drop(guard);
        drop(old_state);
        drop(waiters);
    }
}

// ============================================================================
// RawAsyncSemaphore — fair FIFO permit semaphore with ticket conservation
// ============================================================================

struct SemaphoreInner {
    next_waiter_id: WaiterId,
    value: usize,
    max_value: usize,
    waiters: VecDeque<WaiterEntry>,
    granted_tickets: HashSet<WaiterId>,
}

impl SemaphoreInner {
    fn alloc_id(&mut self) -> WaiterId {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1);
        id
    }
}

#[pyclass(frozen, module = "multiloop._multiloop_core")]
pub struct RawAsyncSemaphore {
    state: Arc<Mutex<SemaphoreInner>>,
}

impl RawAsyncSemaphore {
    fn transfer_or_restore_permit(&self, py: Python<'_>) {
        loop {
            let next_waiter = {
                let mut guard = self.state.lock();
                if let Some(w) = guard.waiters.pop_front() {
                    guard.granted_tickets.insert(w.id);
                    Some(w)
                } else {
                    guard.value += 1;
                    None
                }
            };

            match next_waiter {
                None => break,
                Some(w) => {
                    if wake_pair_outside_lock(py, &w.event_loop, &w.future) {
                        break;
                    }
                    // Target loop closed: revoke granted ticket and try next waiter
                    let mut guard = self.state.lock();
                    if !guard.granted_tickets.remove(&w.id) {
                        break;
                    }
                }
            }
        }
    }
}

#[pymethods]
impl RawAsyncSemaphore {
    #[new]
    pub fn new(max_value: isize) -> PyResult<Self> {
        if max_value < 0 {
            return Err(PyValueError::new_err("max_value must be >= 0"));
        }
        let mv = max_value as usize;
        Ok(RawAsyncSemaphore {
            state: Arc::new(Mutex::new(SemaphoreInner {
                next_waiter_id: 1,
                value: mv,
                max_value: mv,
                waiters: VecDeque::new(),
                granted_tickets: HashSet::new(),
            })),
        })
    }

    pub fn value(&self) -> usize {
        self.state.lock().value
    }

    pub fn max_value(&self) -> usize {
        self.state.lock().max_value
    }

    pub fn waiter_count(&self) -> usize {
        self.state.lock().waiters.len()
    }

    pub fn waiter_tickets(&self) -> Vec<WaiterId> {
        self.state.lock().waiters.iter().map(|w| w.id).collect()
    }

    pub fn try_acquire(&self) -> bool {
        let mut guard = self.state.lock();
        if guard.value > 0 && guard.waiters.is_empty() {
            guard.value -= 1;
            true
        } else {
            false
        }
    }

    pub fn acquire_or_register(&self, loop_obj: Py<PyAny>, fut_obj: Py<PyAny>) -> Option<WaiterId> {
        let mut guard = self.state.lock();
        if guard.value > 0 && guard.waiters.is_empty() {
            guard.value -= 1;
            None
        } else {
            let id = guard.alloc_id();
            guard
                .waiters
                .push_back(WaiterEntry::new(id, loop_obj, fut_obj));
            Some(id)
        }
    }

    pub fn claim_acquire(&self, ticket_id: WaiterId) {
        self.state.lock().granted_tickets.remove(&ticket_id);
    }

    pub fn cancel_waiter(&self, py: Python<'_>, ticket_id: WaiterId) {
        let (was_granted, dropped_waiter) = {
            let mut guard = self.state.lock();
            if let Some(pos) = guard.waiters.iter().position(|w| w.id == ticket_id) {
                let removed = guard.waiters.remove(pos);
                (false, removed)
            } else {
                (guard.granted_tickets.remove(&ticket_id), None)
            }
        };
        drop(dropped_waiter);
        if was_granted {
            self.transfer_or_restore_permit(py);
        }
    }

    pub fn release(&self, py: Python<'_>) -> PyResult<()> {
        let has_waiter = {
            let mut guard = self.state.lock();
            if !guard.waiters.is_empty() {
                true
            } else if guard.value >= guard.max_value {
                return Err(PyValueError::new_err("Semaphore released too many times"));
            } else {
                guard.value += 1;
                false
            }
        };
        if has_waiter {
            self.transfer_or_restore_permit(py);
        }
        Ok(())
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let Some(guard) = self.state.try_lock() else {
            return Ok(());
        };
        for w in &guard.waiters {
            visit_waiter_entry(w, &visit)?;
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

// ============================================================================
// RawAsyncCapacityLimiter — dynamic token limiter with ticket conservation
// ============================================================================

struct CapacityLimiterInner {
    next_waiter_id: WaiterId,
    total_tokens: f64,
    borrowed: usize,
    waiters: VecDeque<WaiterEntry>,
    granted_tickets: HashSet<WaiterId>,
}

impl CapacityLimiterInner {
    fn alloc_id(&mut self) -> WaiterId {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1);
        id
    }
}

#[pyclass(frozen, module = "multiloop._multiloop_core")]
pub struct RawAsyncCapacityLimiter {
    state: Arc<Mutex<CapacityLimiterInner>>,
}

impl RawAsyncCapacityLimiter {
    fn wake_available_waiters(&self, py: Python<'_>) {
        loop {
            let next_waiter = {
                let mut guard = self.state.lock();
                let capacity = guard.total_tokens as usize;
                if guard.borrowed < capacity {
                    if let Some(w) = guard.waiters.pop_front() {
                        guard.borrowed += 1;
                        guard.granted_tickets.insert(w.id);
                        Some(w)
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            match next_waiter {
                None => break,
                Some(w) => {
                    if !wake_pair_outside_lock(py, &w.event_loop, &w.future) {
                        // Target loop closed: roll back borrowed token and continue
                        let mut guard = self.state.lock();
                        if guard.granted_tickets.remove(&w.id) {
                            guard.borrowed = guard.borrowed.saturating_sub(1);
                        }
                    }
                }
            }
        }
    }
}

#[inline]
fn validate_total_tokens(value: f64) -> PyResult<()> {
    if !value.is_finite() || value <= 0.0 {
        return Err(PyValueError::new_err(
            "total_tokens must be a finite positive number",
        ));
    }
    Ok(())
}

#[pymethods]
impl RawAsyncCapacityLimiter {
    #[new]
    pub fn new(total_tokens: f64) -> PyResult<Self> {
        validate_total_tokens(total_tokens)?;
        Ok(RawAsyncCapacityLimiter {
            state: Arc::new(Mutex::new(CapacityLimiterInner {
                next_waiter_id: 1,
                total_tokens,
                borrowed: 0,
                waiters: VecDeque::new(),
                granted_tickets: HashSet::new(),
            })),
        })
    }

    pub fn total_tokens(&self) -> f64 {
        self.state.lock().total_tokens
    }

    pub fn set_total_tokens(&self, py: Python<'_>, value: f64) -> PyResult<()> {
        validate_total_tokens(value)?;
        {
            let mut guard = self.state.lock();
            guard.total_tokens = value;
        }
        self.wake_available_waiters(py);
        Ok(())
    }

    pub fn borrowed(&self) -> usize {
        self.state.lock().borrowed
    }

    pub fn waiter_count(&self) -> usize {
        self.state.lock().waiters.len()
    }

    pub fn waiter_tickets(&self) -> Vec<WaiterId> {
        self.state.lock().waiters.iter().map(|w| w.id).collect()
    }

    pub fn snapshot(&self) -> (f64, f64, f64) {
        let guard = self.state.lock();
        let avail = guard.total_tokens - (guard.borrowed as f64);
        (guard.total_tokens, avail, guard.borrowed as f64)
    }

    pub fn try_acquire(&self) -> bool {
        let mut guard = self.state.lock();
        let capacity = guard.total_tokens as usize;
        if guard.borrowed < capacity && guard.waiters.is_empty() {
            guard.borrowed += 1;
            true
        } else {
            false
        }
    }

    pub fn acquire_or_register(&self, loop_obj: Py<PyAny>, fut_obj: Py<PyAny>) -> Option<WaiterId> {
        let mut guard = self.state.lock();
        let capacity = guard.total_tokens as usize;
        if guard.borrowed < capacity && guard.waiters.is_empty() {
            guard.borrowed += 1;
            None
        } else {
            let id = guard.alloc_id();
            guard
                .waiters
                .push_back(WaiterEntry::new(id, loop_obj, fut_obj));
            Some(id)
        }
    }

    pub fn claim_acquire(&self, ticket_id: WaiterId) {
        self.state.lock().granted_tickets.remove(&ticket_id);
    }

    pub fn cancel_waiter(&self, py: Python<'_>, ticket_id: WaiterId) {
        let (was_granted, dropped_waiter) = {
            let mut guard = self.state.lock();
            if let Some(pos) = guard.waiters.iter().position(|w| w.id == ticket_id) {
                let removed = guard.waiters.remove(pos);
                (false, removed)
            } else if guard.granted_tickets.remove(&ticket_id) {
                guard.borrowed = guard.borrowed.saturating_sub(1);
                (true, None)
            } else {
                (false, None)
            }
        };
        drop(dropped_waiter);
        if was_granted {
            self.wake_available_waiters(py);
        }
    }

    pub fn release(&self, py: Python<'_>) -> PyResult<()> {
        {
            let mut guard = self.state.lock();
            if guard.borrowed == 0 {
                return Err(PyValueError::new_err(
                    "CapacityLimiter released too many times",
                ));
            }
            guard.borrowed -= 1;
        }
        self.wake_available_waiters(py);
        Ok(())
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let Some(guard) = self.state.try_lock() else {
            return Ok(());
        };
        for w in &guard.waiters {
            visit_waiter_entry(w, &visit)?;
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

// ============================================================================
// RawAsyncEvent — sticky one-shot cross-loop event
// ============================================================================

struct EventInner {
    next_waiter_id: WaiterId,
    flag: bool,
    waiters: Vec<WaiterEntry>,
}

impl EventInner {
    fn alloc_id(&mut self) -> WaiterId {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1);
        id
    }
}

#[pyclass(frozen, module = "multiloop._multiloop_core")]
pub struct RawAsyncEvent {
    state: Arc<Mutex<EventInner>>,
}

impl Default for RawAsyncEvent {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl RawAsyncEvent {
    #[new]
    pub fn new() -> Self {
        RawAsyncEvent {
            state: Arc::new(Mutex::new(EventInner {
                next_waiter_id: 1,
                flag: false,
                waiters: Vec::new(),
            })),
        }
    }

    pub fn is_set(&self) -> bool {
        self.state.lock().flag
    }

    pub fn waiter_count(&self) -> usize {
        self.state.lock().waiters.len()
    }

    pub fn set(&self, py: Python<'_>) {
        let waiters = {
            let mut guard = self.state.lock();
            guard.flag = true;
            std::mem::take(&mut guard.waiters)
        };
        for w in waiters {
            wake_pair_outside_lock(py, &w.event_loop, &w.future);
        }
    }

    pub fn wait_or_register(&self, loop_obj: Py<PyAny>, fut_obj: Py<PyAny>) -> Option<WaiterId> {
        let mut guard = self.state.lock();
        if guard.flag {
            None
        } else {
            let id = guard.alloc_id();
            guard.waiters.push(WaiterEntry::new(id, loop_obj, fut_obj));
            Some(id)
        }
    }

    pub fn cancel_waiter(&self, ticket_id: WaiterId) {
        let mut guard = self.state.lock();
        let removed = guard
            .waiters
            .iter()
            .position(|w| w.id == ticket_id)
            .map(|pos| guard.waiters.swap_remove(pos));
        drop(guard);
        drop(removed);
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let Some(guard) = self.state.try_lock() else {
            return Ok(());
        };
        for w in &guard.waiters {
            visit_waiter_entry(w, &visit)?;
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

// ============================================================================
// RawAsyncCondition — waiter queue with notification token conservation
// ============================================================================

struct ConditionInner {
    next_waiter_id: WaiterId,
    waiters: VecDeque<WaiterEntry>,
    notified_tickets: HashSet<WaiterId>,
}

impl ConditionInner {
    fn alloc_id(&mut self) -> WaiterId {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1);
        id
    }
}

#[pyclass(frozen, module = "multiloop._multiloop_core")]
pub struct RawAsyncCondition {
    state: Arc<Mutex<ConditionInner>>,
}

impl Default for RawAsyncCondition {
    fn default() -> Self {
        Self::new()
    }
}

impl RawAsyncCondition {
    fn wake_n(&self, py: Python<'_>, n: usize) {
        let mut remaining = n;
        while remaining > 0 {
            let next_waiter = {
                let mut guard = self.state.lock();
                if let Some(w) = guard.waiters.pop_front() {
                    guard.notified_tickets.insert(w.id);
                    Some(w)
                } else {
                    None
                }
            };
            match next_waiter {
                None => break,
                Some(w) => {
                    if wake_pair_outside_lock(py, &w.event_loop, &w.future) {
                        remaining -= 1;
                    } else {
                        self.state.lock().notified_tickets.remove(&w.id);
                    }
                }
            }
        }
    }
}

#[pymethods]
impl RawAsyncCondition {
    #[new]
    pub fn new() -> Self {
        RawAsyncCondition {
            state: Arc::new(Mutex::new(ConditionInner {
                next_waiter_id: 1,
                waiters: VecDeque::new(),
                notified_tickets: HashSet::new(),
            })),
        }
    }

    pub fn waiter_count(&self) -> usize {
        self.state.lock().waiters.len()
    }

    pub fn register_waiter(&self, loop_obj: Py<PyAny>, fut_obj: Py<PyAny>) -> WaiterId {
        let mut guard = self.state.lock();
        let id = guard.alloc_id();
        guard
            .waiters
            .push_back(WaiterEntry::new(id, loop_obj, fut_obj));
        id
    }

    pub fn claim_waiter(&self, ticket_id: WaiterId) {
        self.state.lock().notified_tickets.remove(&ticket_id);
    }

    pub fn cancel_or_forward(&self, py: Python<'_>, ticket_id: WaiterId) {
        let (was_notified, dropped_waiter) = {
            let mut guard = self.state.lock();
            if let Some(pos) = guard.waiters.iter().position(|w| w.id == ticket_id) {
                let removed = guard.waiters.remove(pos);
                (false, removed)
            } else {
                (guard.notified_tickets.remove(&ticket_id), None)
            }
        };
        drop(dropped_waiter);
        if was_notified {
            self.wake_n(py, 1);
        }
    }

    pub fn forward_notify(&self, py: Python<'_>) {
        self.wake_n(py, 1);
    }

    pub fn notify(&self, py: Python<'_>, n: isize) {
        if n <= 0 {
            return;
        }
        self.wake_n(py, n as usize);
    }

    pub fn notify_all(&self, py: Python<'_>) {
        let waiters = {
            let mut guard = self.state.lock();
            let drained = std::mem::take(&mut guard.waiters);
            for w in &drained {
                guard.notified_tickets.insert(w.id);
            }
            drained
        };
        let mut failed_ids = Vec::new();
        for w in waiters {
            if !wake_pair_outside_lock(py, &w.event_loop, &w.future) {
                failed_ids.push(w.id);
            }
        }
        if !failed_ids.is_empty() {
            let mut guard = self.state.lock();
            for id in failed_ids {
                guard.notified_tickets.remove(&id);
            }
        }
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let Some(guard) = self.state.try_lock() else {
            return Ok(());
        };
        for w in &guard.waiters {
            visit_waiter_entry(w, &visit)?;
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

// ============================================================================
// RawAsyncBarrier — N-party cyclic barrier with generation & direct code wake
// ============================================================================

#[derive(Clone, Copy, PartialEq, Eq)]
enum BarrierState {
    Healthy,
    Broken,
    Aborted,
}

struct BarrierInner {
    parties: usize,
    next_waiter_id: WaiterId,
    generation: u64,
    barrier_state: BarrierState,
    waiters: VecDeque<WaiterEntry>,
}

impl BarrierInner {
    fn alloc_id(&mut self) -> WaiterId {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1).max(1);
        id
    }

    fn check_and_try_trip(&mut self) -> PyResult<Option<(usize, Vec<WaiterEntry>)>> {
        match self.barrier_state {
            BarrierState::Aborted => Err(PyRuntimeError::new_err("barrier has been aborted")),
            BarrierState::Broken => Err(PyRuntimeError::new_err("barrier is broken")),
            BarrierState::Healthy => {
                let n = self.waiters.len() + 1;
                if n == self.parties {
                    let drained: Vec<WaiterEntry> = self.waiters.drain(..).collect();
                    self.generation = self.generation.wrapping_add(1);
                    Ok(Some((self.parties - 1, drained)))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

#[pyclass(frozen, module = "multiloop._multiloop_core")]
pub struct RawAsyncBarrier {
    state: Arc<Mutex<BarrierInner>>,
}

impl RawAsyncBarrier {
    fn wake_tripped_waiters(py: Python<'_>, drained: Vec<WaiterEntry>) {
        for (idx, w) in drained.into_iter().enumerate() {
            let code = PyInt::new(py, idx as isize).into_any().unbind();
            wake_waiter_outside_lock(py, &w, Some(code), false);
        }
    }
}

#[pymethods]
impl RawAsyncBarrier {
    #[new]
    pub fn new(parties: isize) -> PyResult<Self> {
        if parties <= 0 {
            return Err(PyValueError::new_err("parties must be >= 1"));
        }
        Ok(RawAsyncBarrier {
            state: Arc::new(Mutex::new(BarrierInner {
                parties: parties as usize,
                next_waiter_id: 1,
                generation: 0,
                barrier_state: BarrierState::Healthy,
                waiters: VecDeque::new(),
            })),
        })
    }

    pub fn parties(&self) -> usize {
        self.state.lock().parties
    }

    pub fn broken(&self) -> bool {
        matches!(
            self.state.lock().barrier_state,
            BarrierState::Broken | BarrierState::Aborted
        )
    }

    pub fn n_waiting(&self) -> usize {
        self.state.lock().waiters.len()
    }

    pub fn try_arrive(&self, py: Python<'_>) -> PyResult<Option<usize>> {
        let tripped = {
            let mut guard = self.state.lock();
            guard.check_and_try_trip()?
        };
        if let Some((trip_idx, drained)) = tripped {
            Self::wake_tripped_waiters(py, drained);
            Ok(Some(trip_idx))
        } else {
            Ok(None)
        }
    }

    pub fn arrive_or_register(
        &self,
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        fut_obj: Py<PyAny>,
    ) -> PyResult<(u64, Option<WaiterId>)> {
        let (outcome, _dropped_args) = {
            let mut guard = self.state.lock();
            match guard.check_and_try_trip() {
                Err(err) => (Err(err), Some((loop_obj, fut_obj))),
                Ok(Some((_trip_idx, drained))) => (
                    Ok((guard.generation, None, drained)),
                    Some((loop_obj, fut_obj)),
                ),
                Ok(None) => {
                    let id = guard.alloc_id();
                    let gen = guard.generation;
                    guard
                        .waiters
                        .push_back(WaiterEntry::new(id, loop_obj, fut_obj));
                    (Ok((gen, Some(id), Vec::new())), None)
                }
            }
        };

        let (gen, ticket_opt, drained) = outcome?;
        if ticket_opt.is_none() {
            Self::wake_tripped_waiters(py, drained);
        }
        Ok((gen, ticket_opt))
    }

    pub fn cancel_waiter(&self, py: Python<'_>, generation: u64, ticket_id: WaiterId) {
        let (drained, dropped_cancelled) = {
            let mut guard = self.state.lock();
            if guard.generation == generation {
                if let Some(pos) = guard.waiters.iter().position(|w| w.id == ticket_id) {
                    let cancelled = guard.waiters.remove(pos);
                    guard.barrier_state = BarrierState::Broken;
                    guard.generation = guard.generation.wrapping_add(1);
                    let others: Vec<WaiterEntry> = guard.waiters.drain(..).collect();
                    (others, cancelled)
                } else {
                    (Vec::new(), None)
                }
            } else {
                (Vec::new(), None)
            }
        };
        drop(dropped_cancelled);
        for w in drained {
            let code = PyInt::new(py, -1isize).into_any().unbind();
            wake_waiter_outside_lock(py, &w, Some(code), false);
        }
    }

    pub fn abort(&self, py: Python<'_>) {
        let drained: Vec<WaiterEntry> = {
            let mut guard = self.state.lock();
            guard.barrier_state = BarrierState::Aborted;
            guard.generation = guard.generation.wrapping_add(1);
            guard.waiters.drain(..).collect()
        };
        for w in drained {
            let code = PyInt::new(py, -2isize).into_any().unbind();
            wake_waiter_outside_lock(py, &w, Some(code), false);
        }
    }

    pub fn reset(&self, py: Python<'_>) {
        let drained: Vec<WaiterEntry> = {
            let mut guard = self.state.lock();
            guard.barrier_state = BarrierState::Healthy;
            guard.generation = guard.generation.wrapping_add(1);
            guard.waiters.drain(..).collect()
        };
        for w in drained {
            let code = PyInt::new(py, -3isize).into_any().unbind();
            wake_waiter_outside_lock(py, &w, Some(code), false);
        }
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let Some(guard) = self.state.try_lock() else {
            return Ok(());
        };
        for w in &guard.waiters {
            visit_waiter_entry(w, &visit)?;
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

// ============================================================================
// RawAsyncOnce — thread-safe single execution state machine
// ============================================================================

enum OnceState {
    Idle,
    Executing(Vec<WaiterEntry>),
    DoneValue(Py<PyAny>),
    DoneError(Py<PyAny>),
}

struct OnceInner {
    next_waiter_id: WaiterId,
    state: OnceState,
}

impl OnceInner {
    fn alloc_id(&mut self) -> WaiterId {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1).max(1);
        id
    }
}

#[pyclass(frozen, module = "multiloop._multiloop_core")]
pub struct RawAsyncOnce {
    state: Arc<Mutex<OnceInner>>,
}

impl Default for RawAsyncOnce {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl RawAsyncOnce {
    #[new]
    pub fn new() -> Self {
        RawAsyncOnce {
            state: Arc::new(Mutex::new(OnceInner {
                next_waiter_id: 1,
                state: OnceState::Idle,
            })),
        }
    }

    pub fn is_done(&self) -> bool {
        matches!(
            self.state.lock().state,
            OnceState::DoneValue(_) | OnceState::DoneError(_)
        )
    }

    /// Lock-free-style fast check: returns `Some((is_exc, val_or_exc))` when already completed.
    pub fn try_get_completed(&self, py: Python<'_>) -> Option<(bool, Py<PyAny>)> {
        let guard = self.state.lock();
        match &guard.state {
            OnceState::DoneValue(v) => Some((false, v.clone_ref(py))),
            OnceState::DoneError(e) => Some((true, e.clone_ref(py))),
            OnceState::Idle | OnceState::Executing(_) => None,
        }
    }

    /// Returns `(status, ticket_id, value_or_exc)` where `status` is:
    /// - `0`: caller is elected leader (`Executing`)
    /// - `1`: caller is registered follower (`ticket_id = Some(id)`)
    /// - `2`: already completed with result (`value_or_exc = result`)
    /// - `3`: already completed with exception (`value_or_exc = exc`)
    pub fn enter_or_register(
        &self,
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        fut_obj: Py<PyAny>,
    ) -> (u8, Option<WaiterId>, Option<Py<PyAny>>) {
        let (res, _dropped_args) = {
            let mut guard = self.state.lock();
            match &mut guard.state {
                OnceState::DoneValue(v) => {
                    ((2, None, Some(v.clone_ref(py))), Some((loop_obj, fut_obj)))
                }
                OnceState::DoneError(e) => {
                    ((3, None, Some(e.clone_ref(py))), Some((loop_obj, fut_obj)))
                }
                OnceState::Idle => {
                    guard.state = OnceState::Executing(Vec::new());
                    ((0, None, None), Some((loop_obj, fut_obj)))
                }
                OnceState::Executing(_) => {
                    let id = guard.alloc_id();
                    if let OnceState::Executing(ref mut waiters) = guard.state {
                        waiters.push(WaiterEntry::new(id, loop_obj, fut_obj));
                    }
                    ((1, Some(id), None), None)
                }
            }
        };
        res
    }

    pub fn cancel_waiter(&self, ticket_id: WaiterId) {
        let _removed = {
            let mut guard = self.state.lock();
            if let OnceState::Executing(ref mut waiters) = guard.state {
                waiters
                    .iter()
                    .position(|w| w.id == ticket_id)
                    .map(|pos| waiters.swap_remove(pos))
            } else {
                None
            }
        };
    }

    pub fn outcome(&self, py: Python<'_>) -> (bool, Option<Py<PyAny>>) {
        let guard = self.state.lock();
        match &guard.state {
            OnceState::DoneError(e) => (true, Some(e.clone_ref(py))),
            OnceState::DoneValue(v) => (false, Some(v.clone_ref(py))),
            OnceState::Idle | OnceState::Executing(_) => (false, None),
        }
    }

    pub fn complete(&self, py: Python<'_>, result: Option<Py<PyAny>>, exc: Option<Py<PyAny>>) {
        let (new_state, _unused_result) = match exc {
            Some(e) => (OnceState::DoneError(e), result),
            None => (
                OnceState::DoneValue(result.unwrap_or_else(|| py.None())),
                None,
            ),
        };
        let old_state = {
            let mut guard = self.state.lock();
            std::mem::replace(&mut guard.state, new_state)
        };
        if let OnceState::Executing(waiters) = old_state {
            for w in waiters {
                wake_pair_outside_lock(py, &w.event_loop, &w.future);
            }
        }
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let Some(guard) = self.state.try_lock() else {
            return Ok(());
        };
        match &guard.state {
            OnceState::Idle => {}
            OnceState::Executing(waiters) => {
                for w in waiters {
                    visit_waiter_entry(w, &visit)?;
                }
            }
            OnceState::DoneValue(val) => visit.call(val)?,
            OnceState::DoneError(exc) => visit.call(exc)?,
        }
        Ok(())
    }

    fn __clear__(&self) {
        let mut guard = self.state.lock();
        let old_state = std::mem::replace(&mut guard.state, OnceState::Idle);
        drop(guard);
        drop(old_state);
    }
}
