use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::pyclass::{PyTraverseError, PyVisit};

use crate::waiter::{alloc_task_token, cancel_waiter_outside_lock, WaiterEntry, WaiterId};

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ScopeMode {
    Plain = 0,
    MoveOn = 1,
    FailAfter = 2,
}

impl ScopeMode {
    #[inline]
    pub fn from_u8(val: u8) -> Self {
        match val {
            1 => ScopeMode::MoveOn,
            2 => ScopeMode::FailAfter,
            _ => ScopeMode::Plain,
        }
    }
}

struct ScopeSlot {
    entry_id: u64,
    deadline: f64,
    #[allow(dead_code)]
    mode: ScopeMode,
    shield: bool,
    cancel_called: bool,
}

struct TaskCancelInner {
    next_entry_id: u64,
    next_park_id: WaiterId,
    scopes: Vec<ScopeSlot>,
    parked: Option<WaiterEntry>,
}

impl TaskCancelInner {
    #[inline]
    fn alloc_entry_id(&mut self) -> u64 {
        let id = self.next_entry_id;
        self.next_entry_id = self.next_entry_id.wrapping_add(1).max(1);
        id
    }

    #[inline]
    fn alloc_park_id(&mut self) -> WaiterId {
        let id = self.next_park_id;
        self.next_park_id = self.next_park_id.wrapping_add(1).max(1);
        id
    }
}

#[pyclass(frozen, module = "multiloop._multiloop_core")]
pub struct RawTaskCancelState {
    task_token: u64,
    effectively_cancelled: AtomicBool,
    effective_deadline_bits: AtomicU64,
    parked_id: AtomicU64,
    state: Mutex<TaskCancelInner>,
}

impl Default for RawTaskCancelState {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn effective_start(scopes: &[ScopeSlot]) -> usize {
    scopes.iter().rposition(|s| s.shield).unwrap_or(0)
}

#[inline]
fn is_cancelled_inner(scopes: &[ScopeSlot]) -> bool {
    if scopes.is_empty() {
        return false;
    }
    let start = effective_start(scopes);
    scopes[start..].iter().any(|s| s.cancel_called)
}

#[inline]
fn effective_deadline_inner(scopes: &[ScopeSlot]) -> f64 {
    if scopes.is_empty() {
        return f64::INFINITY;
    }
    let start = effective_start(scopes);
    let mut min_deadline = f64::INFINITY;
    for slot in &scopes[start..] {
        if slot.deadline < min_deadline {
            min_deadline = slot.deadline;
        }
    }
    min_deadline
}

impl RawTaskCancelState {
    #[inline]
    fn sync_effectively_cancelled(&self, scopes: &[ScopeSlot]) -> bool {
        let effective = is_cancelled_inner(scopes);
        self.effectively_cancelled
            .store(effective, Ordering::Release);
        effective
    }

    #[inline]
    fn sync_caches(&self, scopes: &[ScopeSlot]) -> bool {
        let effective = self.sync_effectively_cancelled(scopes);
        self.effective_deadline_bits.store(
            effective_deadline_inner(scopes).to_bits(),
            Ordering::Release,
        );
        effective
    }
}

#[pymethods]
impl RawTaskCancelState {
    #[new]
    pub fn new() -> Self {
        Self {
            task_token: alloc_task_token(),
            effectively_cancelled: AtomicBool::new(false),
            effective_deadline_bits: AtomicU64::new(f64::INFINITY.to_bits()),
            parked_id: AtomicU64::new(0),
            state: Mutex::new(TaskCancelInner {
                next_entry_id: 1,
                next_park_id: 1,
                scopes: Vec::with_capacity(4),
                parked: None,
            }),
        }
    }

    #[getter]
    pub fn task_token(&self) -> u64 {
        self.task_token
    }

    pub fn push_scope(&self, deadline: f64, mode: u8, shield: bool, pre_cancelled: bool) -> u64 {
        let mut guard = self.state.lock();
        let entry_id = guard.alloc_entry_id();
        guard.scopes.push(ScopeSlot {
            entry_id,
            deadline,
            mode: ScopeMode::from_u8(mode),
            shield,
            cancel_called: pre_cancelled,
        });
        self.sync_caches(&guard.scopes);
        entry_id
    }

    pub fn pop_scope(&self, entry_id: u64) -> PyResult<bool> {
        let mut guard = self.state.lock();
        match guard.scopes.iter().rposition(|s| s.entry_id == entry_id) {
            Some(pos) => {
                let slot = guard.scopes.remove(pos);
                self.sync_caches(&guard.scopes);
                Ok(slot.cancel_called)
            }
            None => Err(PyRuntimeError::new_err(format!(
                "CancelScope entry_id {entry_id} not found on stack"
            ))),
        }
    }

    /// Atomically pop `entry_id` from the task cancel stack and return
    /// `(self_cancelled, outer_unshielded_cancelled)`.
    pub fn pop_scope_status(&self, entry_id: u64) -> PyResult<(bool, bool)> {
        let mut guard = self.state.lock();
        match guard.scopes.iter().rposition(|s| s.entry_id == entry_id) {
            Some(pos) => {
                let slot = guard.scopes.remove(pos);
                self.sync_caches(&guard.scopes);
                let outer_cancelled = !slot.shield && is_cancelled_inner(&guard.scopes[..pos]);
                Ok((slot.cancel_called, outer_cancelled))
            }
            None => Err(PyRuntimeError::new_err(format!(
                "CancelScope entry_id {entry_id} not found on stack"
            ))),
        }
    }

    pub fn park_waiter(&self, loop_obj: Py<PyAny>, fut_obj: Py<PyAny>) -> WaiterId {
        if self.effectively_cancelled.load(Ordering::Acquire) {
            return 0;
        }
        let (park_id, _dropped_old, _dropped_args) = {
            let mut guard = self.state.lock();
            if is_cancelled_inner(&guard.scopes) {
                (0, None, Some((loop_obj, fut_obj)))
            } else {
                let id = guard.alloc_park_id();
                self.parked_id.store(id, Ordering::Release);
                let old = guard
                    .parked
                    .replace(WaiterEntry::new(id, loop_obj, fut_obj));
                (id, old, None)
            }
        };
        park_id
    }

    pub fn unpark_waiter(&self, park_id: WaiterId) {
        if park_id == 0 || self.parked_id.load(Ordering::Acquire) != park_id {
            return;
        }
        let _removed = {
            let mut guard = self.state.lock();
            if guard.parked.as_ref().is_some_and(|w| w.id == park_id) {
                self.parked_id.store(0, Ordering::Release);
                guard.parked.take()
            } else {
                None
            }
        };
    }

    pub fn cancel_scope(&self, py: Python<'_>, entry_id: u64) -> bool {
        let (effective, to_wake) = {
            let mut guard = self.state.lock();
            if let Some(slot) = guard.scopes.iter_mut().find(|s| s.entry_id == entry_id) {
                slot.cancel_called = true;
            } else {
                return false;
            }
            if self.sync_effectively_cancelled(&guard.scopes) {
                self.parked_id.store(0, Ordering::Release);
                (true, guard.parked.take())
            } else {
                (false, None)
            }
        };
        if let Some(entry) = to_wake {
            cancel_waiter_outside_lock(py, &entry);
        }
        effective
    }

    pub fn set_shield(&self, py: Python<'_>, entry_id: u64, shield: bool) {
        let to_wake = {
            let mut guard = self.state.lock();
            if let Some(slot) = guard.scopes.iter_mut().find(|s| s.entry_id == entry_id) {
                slot.shield = shield;
            } else {
                return;
            }
            if self.sync_caches(&guard.scopes) {
                self.parked_id.store(0, Ordering::Release);
                guard.parked.take()
            } else {
                None
            }
        };
        if let Some(entry) = to_wake {
            cancel_waiter_outside_lock(py, &entry);
        }
    }

    pub fn set_deadline(&self, entry_id: u64, deadline: f64) {
        let mut guard = self.state.lock();
        if let Some(slot) = guard.scopes.iter_mut().find(|s| s.entry_id == entry_id) {
            slot.deadline = deadline;
            self.sync_caches(&guard.scopes);
        }
    }

    pub fn is_effectively_cancelled(&self) -> bool {
        self.effectively_cancelled.load(Ordering::Acquire)
    }

    pub fn is_scope_effectively_cancelled(&self, entry_id: u64) -> bool {
        let guard = self.state.lock();
        match guard.scopes.iter().rposition(|s| s.entry_id == entry_id) {
            Some(pos) => is_cancelled_inner(&guard.scopes[..=pos]),
            None => false,
        }
    }

    pub fn effective_deadline(&self) -> f64 {
        f64::from_bits(self.effective_deadline_bits.load(Ordering::Acquire))
    }

    pub fn has_outer_cancellation(&self, entry_id: u64) -> bool {
        let guard = self.state.lock();
        match guard.scopes.iter().rposition(|s| s.entry_id == entry_id) {
            Some(pos) => is_cancelled_inner(&guard.scopes[..pos]),
            None => false,
        }
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let Some(guard) = self.state.try_lock() else {
            return Ok(());
        };
        if let Some(ref w) = guard.parked {
            visit.call(&w.event_loop)?;
            visit.call(&w.future)?;
            visit.call(w.channel_token.as_ref())?;
        }
        Ok(())
    }

    fn __clear__(&self) {
        let mut guard = self.state.lock();
        let parked = guard.parked.take();
        self.parked_id.store(0, Ordering::Release);
        self.sync_effectively_cancelled(&guard.scopes);
        drop(guard);
        drop(parked);
    }
}
