use parking_lot::{Condvar, Mutex};
use pyo3::prelude::*;
use pyo3::types::PyWeakrefReference;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::errors::{channel_closed_err, timeout_err, would_block_err};
use crate::waiter::{
    _complete_select_watcher, get_complete_select_watcher_fn, wake_waiter_outside_lock,
    WaiterEntry, WaiterId,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncOutcome {
    Waiting,
    Ready,
    Closed,
}

pub(crate) struct SyncSlot {
    pub(crate) id: WaiterId,
    pub(crate) state: Mutex<SyncOutcome>,
    pub(crate) condvar: Condvar,
}

impl SyncSlot {
    #[inline]
    pub(crate) fn new(id: WaiterId) -> Arc<Self> {
        Arc::new(Self {
            id,
            state: Mutex::new(SyncOutcome::Waiting),
            condvar: Condvar::new(),
        })
    }

    pub(crate) fn wait_until(&self, deadline: Option<std::time::Instant>) -> Option<SyncOutcome> {
        let mut guard = self.state.lock();
        loop {
            match *guard {
                SyncOutcome::Waiting => {
                    if let Some(dl) = deadline {
                        let now = std::time::Instant::now();
                        if now >= dl {
                            return None;
                        }
                        self.condvar.wait_for(&mut guard, dl - now);
                    } else {
                        self.condvar.wait(&mut guard);
                    }
                }
                done => return Some(done),
            }
        }
    }
}

pub(crate) enum ChannelWaiter {
    Async(WaiterEntry),
    Sync(Arc<SyncSlot>),
}

impl ChannelWaiter {
    #[inline]
    pub(crate) fn id(&self) -> WaiterId {
        match self {
            Self::Async(entry) => entry.id,
            Self::Sync(slot) => slot.id,
        }
    }
}

pub(crate) struct SelectWatcherEntry {
    pub(crate) id: WaiterId,
    pub(crate) event_loop: Py<PyAny>,
    pub(crate) arbiter_fut: Py<PyAny>,
    pub(crate) channel_weakref: Py<PyWeakrefReference>,
}

pub(crate) struct ChannelState {
    pub(crate) next_waiter_id: WaiterId,
    pub(crate) buffer: VecDeque<Py<PyAny>>,
    pub(crate) maxsize: usize,
    pub(crate) is_closed: bool,
    pub(crate) getters: VecDeque<ChannelWaiter>,
    pub(crate) putters: VecDeque<ChannelWaiter>,
    pub(crate) select_watchers: VecDeque<SelectWatcherEntry>,
    pub(crate) in_flight_getters: VecDeque<(WaiterId, Py<PyAny>)>,
    pub(crate) in_flight_putters: VecDeque<WaiterId>,
}

impl ChannelState {
    #[inline]
    pub(crate) fn alloc_id(&mut self) -> WaiterId {
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.wrapping_add(1).max(1);
        id
    }

    #[inline]
    pub(crate) fn occupied(&self) -> usize {
        self.buffer.len() + self.in_flight_getters.len() + self.in_flight_putters.len()
    }

    #[inline]
    pub(crate) fn can_accept_put(&self) -> bool {
        self.putters.is_empty() && (self.maxsize == 0 || self.occupied() < self.maxsize)
    }

    #[inline]
    pub(crate) fn is_drained(&self) -> bool {
        self.is_closed && self.buffer.is_empty() && self.in_flight_getters.is_empty()
    }

    #[inline]
    pub(crate) fn drain_closed_waiters_locked(&mut self, to_wake: &mut Vec<WakeTarget>) {
        for g in self.getters.drain(..) {
            to_wake.push(WakeTarget::Waiter {
                waiter: g,
                closed: true,
                kind: None,
            });
        }
        for w in self.select_watchers.drain(..) {
            to_wake.push(WakeTarget::SelectWatcher(w));
        }
    }

    #[inline]
    pub(crate) fn wake_putters_locked(&mut self, to_wake: &mut Vec<WakeTarget>) {
        if self.is_closed || self.maxsize == 0 {
            return;
        }
        while !self.putters.is_empty() && self.occupied() < self.maxsize {
            if let Some(next_putter) = self.putters.pop_front() {
                let pid = next_putter.id();
                self.in_flight_putters.push_back(pid);
                to_wake.push(WakeTarget::Waiter {
                    waiter: next_putter,
                    closed: false,
                    kind: Some(WakeKind::Putter(pid)),
                });
            }
        }
    }
}

#[pyclass(frozen, module = "multiloop._multiloop_core")]
pub struct RawAsyncChannel {
    pub(crate) state: Arc<Mutex<ChannelState>>,
}

#[derive(Clone, Copy)]
pub(crate) enum WakeKind {
    Getter(WaiterId),
    Putter(WaiterId),
}

/// Target to wake strictly outside of the Mutex lock.
pub(crate) enum WakeTarget {
    Waiter {
        waiter: ChannelWaiter,
        closed: bool,
        kind: Option<WakeKind>,
    },
    SelectWatcher(SelectWatcherEntry),
}

impl RawAsyncChannel {
    fn requeue_item_locked(
        guard: &mut ChannelState,
        item: Py<PyAny>,
        to_wake: &mut Vec<WakeTarget>,
    ) {
        if let Some(next_getter) = guard.getters.pop_front() {
            let gid = next_getter.id();
            guard.in_flight_getters.push_back((gid, item));
            to_wake.push(WakeTarget::Waiter {
                waiter: next_getter,
                closed: false,
                kind: Some(WakeKind::Getter(gid)),
            });
            return;
        }
        guard.buffer.push_front(item);
        if let Some(watcher) = guard.select_watchers.pop_front() {
            to_wake.push(WakeTarget::SelectWatcher(watcher));
        }
    }

    fn wake_select_watcher(
        &self,
        py: Python<'_>,
        self_py: &Bound<'_, Self>,
        watcher: &SelectWatcherEntry,
    ) -> bool {
        let loop_obj = watcher.event_loop.bind(py);
        let fut = watcher.arbiter_fut.bind(py);
        let weakref = watcher.channel_weakref.bind(py);
        if loop_obj.is_none() {
            if !fut.is_none() {
                let _ = _complete_select_watcher(py, self_py.as_any(), fut, weakref.as_any());
            }
            return true;
        }
        let complete_fn = get_complete_select_watcher_fn(py);
        loop_obj
            .call_method1("call_soon_threadsafe", (complete_fn, self_py, fut, weakref))
            .is_ok()
    }

    pub(crate) fn dispatch_wake(
        &self,
        py: Python<'_>,
        self_py: &Bound<'_, Self>,
        target: WakeTarget,
    ) {
        match target {
            WakeTarget::Waiter {
                waiter,
                closed,
                kind,
            } => match waiter {
                ChannelWaiter::Async(entry) => {
                    let ok = wake_waiter_outside_lock(py, &entry, None, false);
                    if !ok {
                        match kind {
                            Some(WakeKind::Getter(gid)) => {
                                let _ = RawAsyncChannel::cancel_getter(self_py, py, gid);
                            }
                            Some(WakeKind::Putter(pid)) => {
                                let _ = RawAsyncChannel::cancel_putter(self_py, py, pid);
                            }
                            None => {}
                        }
                    }
                }
                ChannelWaiter::Sync(slot) => {
                    let outcome = if closed {
                        SyncOutcome::Closed
                    } else {
                        SyncOutcome::Ready
                    };
                    {
                        let mut state = slot.state.lock();
                        *state = outcome;
                    }
                    slot.condvar.notify_one();
                }
            },
            WakeTarget::SelectWatcher(entry) => {
                if !self.wake_select_watcher(py, self_py, &entry) {
                    RawAsyncChannel::forward_select_wakeup(self_py, py);
                }
            }
        }
    }

    fn claim_put_internal(
        &self,
        py: Python<'_>,
        self_py: &Bound<'_, Self>,
        item: Py<PyAny>,
        waiter_id: WaiterId,
    ) -> PyResult<Option<Py<PyAny>>> {
        enum Step {
            Done,
            Closed(Py<PyAny>),
            BufferFull(Py<PyAny>),
        }
        let mut to_wake = Vec::new();
        let step = {
            let mut guard = self.state.lock();
            if let Some(pos) = guard
                .in_flight_putters
                .iter()
                .position(|id| *id == waiter_id)
            {
                guard.in_flight_putters.remove(pos);
            }
            if guard.is_closed {
                Step::Closed(item)
            } else if let Some(getter) = guard.getters.pop_front() {
                let gid = getter.id();
                guard.in_flight_getters.push_back((gid, item));
                to_wake.push(WakeTarget::Waiter {
                    waiter: getter,
                    closed: false,
                    kind: Some(WakeKind::Getter(gid)),
                });
                guard.wake_putters_locked(&mut to_wake);
                Step::Done
            } else if guard.maxsize == 0 || guard.buffer.len() < guard.maxsize {
                guard.buffer.push_back(item);
                if let Some(watcher) = guard.select_watchers.pop_front() {
                    to_wake.push(WakeTarget::SelectWatcher(watcher));
                }
                guard.wake_putters_locked(&mut to_wake);
                Step::Done
            } else {
                Step::BufferFull(item)
            }
        };
        for target in to_wake {
            self.dispatch_wake(py, self_py, target);
        }
        match step {
            Step::Done => Ok(None),
            Step::Closed(rejected) => {
                drop(rejected);
                Err(channel_closed_err(py, "Channel is closed"))
            }
            Step::BufferFull(item_back) => Ok(Some(item_back)),
        }
    }
}

#[pymethods]
impl RawAsyncChannel {
    #[new]
    #[pyo3(signature = (maxsize = 0))]
    pub fn new(maxsize: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(ChannelState {
                next_waiter_id: 1,
                maxsize,
                buffer: VecDeque::new(),
                is_closed: false,
                getters: VecDeque::new(),
                putters: VecDeque::new(),
                select_watchers: VecDeque::new(),
                in_flight_getters: VecDeque::new(),
                in_flight_putters: VecDeque::new(),
            })),
        }
    }

    pub fn close(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let ch = slf.get();
        let mut to_wake = Vec::new();
        {
            let mut guard = ch.state.lock();
            if guard.is_closed {
                return Ok(());
            }
            guard.is_closed = true;
            guard.in_flight_putters.clear();
            for p in guard.putters.drain(..) {
                to_wake.push(WakeTarget::Waiter {
                    waiter: p,
                    closed: true,
                    kind: None,
                });
            }
            if guard.is_drained() {
                guard.drain_closed_waiters_locked(&mut to_wake);
            }
        }
        for target in to_wake {
            ch.dispatch_wake(py, slf, target);
        }
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.state.lock().is_closed
    }

    pub fn is_drained(&self) -> bool {
        self.state.lock().is_drained()
    }

    pub fn qsize(&self) -> usize {
        self.state.lock().buffer.len()
    }

    #[getter]
    pub fn maxsize(&self) -> usize {
        self.state.lock().maxsize
    }

    pub fn empty(&self) -> bool {
        self.state.lock().buffer.is_empty()
    }

    pub fn full(&self) -> bool {
        let guard = self.state.lock();
        guard.maxsize > 0 && guard.occupied() >= guard.maxsize
    }

    pub fn try_send(slf: &Bound<'_, Self>, py: Python<'_>, item: Py<PyAny>) -> PyResult<bool> {
        enum Step {
            Sent,
            Full(Py<PyAny>),
            Closed(Py<PyAny>),
        }
        let ch = slf.get();
        let mut to_wake = Vec::new();
        let step = {
            let mut guard = ch.state.lock();
            if guard.is_closed {
                Step::Closed(item)
            } else if (guard.maxsize == 0 || guard.occupied() < guard.maxsize)
                && !guard.getters.is_empty()
            {
                let getter = guard.getters.pop_front().unwrap();
                let gid = getter.id();
                guard.in_flight_getters.push_back((gid, item));
                to_wake.push(WakeTarget::Waiter {
                    waiter: getter,
                    closed: false,
                    kind: Some(WakeKind::Getter(gid)),
                });
                guard.wake_putters_locked(&mut to_wake);
                Step::Sent
            } else if guard.can_accept_put() {
                guard.buffer.push_back(item);
                if let Some(watcher) = guard.select_watchers.pop_front() {
                    to_wake.push(WakeTarget::SelectWatcher(watcher));
                }
                Step::Sent
            } else {
                Step::Full(item)
            }
        };
        for target in to_wake {
            ch.dispatch_wake(py, slf, target);
        }
        match step {
            Step::Sent => Ok(true),
            Step::Full(rejected) => {
                drop(rejected);
                Ok(false)
            }
            Step::Closed(rejected) => {
                drop(rejected);
                Err(channel_closed_err(py, "Channel is closed"))
            }
        }
    }

    pub fn send_or_register(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        item: Py<PyAny>,
        loop_obj: Py<PyAny>,
        fut: Py<PyAny>,
    ) -> PyResult<Option<WaiterId>> {
        enum Step {
            Done(Py<PyAny>, Py<PyAny>),
            Queued(WaiterId),
            Closed(Py<PyAny>, Py<PyAny>, Py<PyAny>),
        }
        let ch = slf.get();
        let mut to_wake = Vec::new();
        let step = {
            let mut guard = ch.state.lock();
            if guard.is_closed {
                Step::Closed(item, loop_obj, fut)
            } else if (guard.maxsize == 0 || guard.occupied() < guard.maxsize)
                && !guard.getters.is_empty()
            {
                let getter = guard.getters.pop_front().unwrap();
                let gid = getter.id();
                guard.in_flight_getters.push_back((gid, item));
                to_wake.push(WakeTarget::Waiter {
                    waiter: getter,
                    closed: false,
                    kind: Some(WakeKind::Getter(gid)),
                });
                guard.wake_putters_locked(&mut to_wake);
                Step::Done(loop_obj, fut)
            } else if guard.can_accept_put() {
                guard.buffer.push_back(item);
                if let Some(watcher) = guard.select_watchers.pop_front() {
                    to_wake.push(WakeTarget::SelectWatcher(watcher));
                }
                Step::Done(loop_obj, fut)
            } else {
                let id = guard.alloc_id();
                guard
                    .putters
                    .push_back(ChannelWaiter::Async(WaiterEntry::new(id, loop_obj, fut)));
                Step::Queued(id)
            }
        };
        for target in to_wake {
            ch.dispatch_wake(py, slf, target);
        }
        match step {
            Step::Done(l, f) => {
                drop(l);
                drop(f);
                Ok(None)
            }
            Step::Queued(id) => Ok(Some(id)),
            Step::Closed(i, l, f) => {
                drop(i);
                drop(l);
                drop(f);
                Err(channel_closed_err(py, "Channel is closed"))
            }
        }
    }

    pub fn try_recv(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        enum Step {
            Item(Py<PyAny>),
            Empty,
            Closed,
        }
        let ch = slf.get();
        let mut to_wake = Vec::new();
        let step = {
            let mut guard = ch.state.lock();
            if guard.getters.is_empty() && !guard.buffer.is_empty() {
                let item = guard.buffer.pop_front().unwrap();
                guard.wake_putters_locked(&mut to_wake);
                if guard.is_drained() {
                    guard.drain_closed_waiters_locked(&mut to_wake);
                }
                Step::Item(item)
            } else if guard.is_drained() {
                Step::Closed
            } else {
                Step::Empty
            }
        };
        for target in to_wake {
            ch.dispatch_wake(py, slf, target);
        }
        match step {
            Step::Item(item) => Ok(item),
            Step::Empty => Err(would_block_err(py, "Channel is empty")),
            Step::Closed => Err(channel_closed_err(py, "Channel is closed")),
        }
    }

    #[pyo3(signature = (item, timeout = None))]
    pub fn send_sync(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        item: Py<PyAny>,
        timeout: Option<f64>,
    ) -> PyResult<()> {
        let ch = slf.get();
        let deadline = timeout
            .map(|t| std::time::Instant::now() + std::time::Duration::from_secs_f64(t.max(0.0)));
        let mut to_wake = Vec::new();
        enum InitialSend {
            Sent,
            Queued(WaiterId, Arc<SyncSlot>, Py<PyAny>),
            TimedOut(Py<PyAny>),
            Closed(Py<PyAny>),
        }
        let init = {
            let mut guard = ch.state.lock();
            if guard.is_closed {
                InitialSend::Closed(item)
            } else if (guard.maxsize == 0 || guard.occupied() < guard.maxsize)
                && !guard.getters.is_empty()
            {
                let getter = guard.getters.pop_front().unwrap();
                let gid = getter.id();
                guard.in_flight_getters.push_back((gid, item));
                to_wake.push(WakeTarget::Waiter {
                    waiter: getter,
                    closed: false,
                    kind: Some(WakeKind::Getter(gid)),
                });
                guard.wake_putters_locked(&mut to_wake);
                InitialSend::Sent
            } else if guard.can_accept_put() {
                guard.buffer.push_back(item);
                if let Some(watcher) = guard.select_watchers.pop_front() {
                    to_wake.push(WakeTarget::SelectWatcher(watcher));
                }
                InitialSend::Sent
            } else if deadline.is_some_and(|dl| std::time::Instant::now() >= dl) {
                InitialSend::TimedOut(item)
            } else {
                let id = guard.alloc_id();
                let slot = SyncSlot::new(id);
                guard.putters.push_back(ChannelWaiter::Sync(slot.clone()));
                InitialSend::Queued(id, slot, item)
            }
        };
        for target in to_wake {
            ch.dispatch_wake(py, slf, target);
        }
        let (mut id, mut slot, mut pending_item) = match init {
            InitialSend::Sent => return Ok(()),
            InitialSend::Closed(rejected) => {
                drop(rejected);
                return Err(channel_closed_err(py, "Channel is closed"));
            }
            InitialSend::TimedOut(rejected) => {
                drop(rejected);
                return Err(timeout_err(py, "send_sync timed out"));
            }
            InitialSend::Queued(id, slot, item) => (id, slot, item),
        };
        loop {
            let slot_clone = slot.clone();
            let outcome = py.detach(move || slot_clone.wait_until(deadline));
            match outcome {
                None => {
                    let _ = RawAsyncChannel::cancel_putter(slf, py, id);
                    drop(pending_item);
                    return Err(timeout_err(py, "send_sync timed out"));
                }
                Some(SyncOutcome::Closed) => {
                    drop(pending_item);
                    return Err(channel_closed_err(py, "Channel is closed"));
                }
                Some(SyncOutcome::Ready) | Some(SyncOutcome::Waiting) => {
                    match ch.claim_put_internal(py, slf, pending_item, id)? {
                        None => return Ok(()),
                        Some(item_back) => {
                            if deadline.is_some_and(|dl| std::time::Instant::now() >= dl) {
                                drop(item_back);
                                return Err(timeout_err(py, "send_sync timed out"));
                            }
                            let mut front_wakes = Vec::new();
                            let next = {
                                let mut guard = ch.state.lock();
                                if guard.is_closed {
                                    Err(item_back)
                                } else {
                                    let new_id = guard.alloc_id();
                                    let new_slot = SyncSlot::new(new_id);
                                    guard
                                        .putters
                                        .push_front(ChannelWaiter::Sync(new_slot.clone()));
                                    if guard.maxsize == 0 || guard.occupied() < guard.maxsize {
                                        guard.wake_putters_locked(&mut front_wakes);
                                    }
                                    Ok((new_id, new_slot, item_back))
                                }
                            };
                            for target in front_wakes {
                                ch.dispatch_wake(py, slf, target);
                            }
                            match next {
                                Err(rejected) => {
                                    drop(rejected);
                                    return Err(channel_closed_err(py, "Channel is closed"));
                                }
                                Ok((new_id, new_slot, item_back)) => {
                                    id = new_id;
                                    slot = new_slot;
                                    pending_item = item_back;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[pyo3(signature = (timeout = None))]
    pub fn recv_sync(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        timeout: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        let ch = slf.get();
        let deadline = timeout
            .map(|t| std::time::Instant::now() + std::time::Duration::from_secs_f64(t.max(0.0)));
        loop {
            let mut to_wake = Vec::new();
            enum InitialRecv {
                Got(Py<PyAny>),
                Closed,
                TimedOut,
                Queued(WaiterId, Arc<SyncSlot>),
            }
            let init = {
                let mut guard = ch.state.lock();
                if guard.getters.is_empty() && !guard.buffer.is_empty() {
                    let item = guard.buffer.pop_front().unwrap();
                    guard.wake_putters_locked(&mut to_wake);
                    if guard.is_drained() {
                        guard.drain_closed_waiters_locked(&mut to_wake);
                    }
                    InitialRecv::Got(item)
                } else if guard.is_drained() {
                    InitialRecv::Closed
                } else if deadline.is_some_and(|dl| std::time::Instant::now() >= dl) {
                    InitialRecv::TimedOut
                } else {
                    let id = guard.alloc_id();
                    let slot = SyncSlot::new(id);
                    guard.getters.push_back(ChannelWaiter::Sync(slot.clone()));
                    InitialRecv::Queued(id, slot)
                }
            };
            for target in to_wake {
                ch.dispatch_wake(py, slf, target);
            }
            let (id, slot) = match init {
                InitialRecv::Got(item) => return Ok(item),
                InitialRecv::Closed => return Err(channel_closed_err(py, "Channel is closed")),
                InitialRecv::TimedOut => return Err(timeout_err(py, "recv_sync timed out")),
                InitialRecv::Queued(id, slot) => (id, slot),
            };
            let slot_clone = slot.clone();
            let outcome = py.detach(move || slot_clone.wait_until(deadline));
            match outcome {
                None => {
                    let _ = RawAsyncChannel::cancel_getter(slf, py, id);
                    return Err(timeout_err(py, "recv_sync timed out"));
                }
                Some(SyncOutcome::Closed) => {
                    return Err(channel_closed_err(py, "Channel is closed"));
                }
                Some(SyncOutcome::Ready) | Some(SyncOutcome::Waiting) => {
                    let (claimed, item_opt) = RawAsyncChannel::claim_get(slf, py, id)?;
                    if claimed {
                        if let Some(item) = item_opt {
                            return Ok(item);
                        }
                    }
                }
            }
        }
    }

    /// Ticket-based getter registration: returns `(0, Some(item))` on immediate fast-path hit,
    /// or `(waiter_id, None)` when queued (`waiter_id >= 1`).
    pub fn register_getter_ticket(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        fut: Py<PyAny>,
    ) -> PyResult<(WaiterId, Option<Py<PyAny>>)> {
        enum Step {
            FastItem(Py<PyAny>, Py<PyAny>, Py<PyAny>),
            Queued(WaiterId),
            Closed(Py<PyAny>, Py<PyAny>),
        }
        let ch = slf.get();
        let mut to_wake = Vec::new();
        let step = {
            let mut guard = ch.state.lock();
            if guard.getters.is_empty() && !guard.buffer.is_empty() {
                let item = guard.buffer.pop_front().unwrap();
                guard.wake_putters_locked(&mut to_wake);
                if guard.is_drained() {
                    guard.drain_closed_waiters_locked(&mut to_wake);
                }
                Step::FastItem(item, loop_obj, fut)
            } else if guard.is_drained() {
                Step::Closed(loop_obj, fut)
            } else {
                let id = guard.alloc_id();
                guard
                    .getters
                    .push_back(ChannelWaiter::Async(WaiterEntry::new(id, loop_obj, fut)));
                Step::Queued(id)
            }
        };
        for target in to_wake {
            ch.dispatch_wake(py, slf, target);
        }
        match step {
            Step::FastItem(item, l, f) => {
                drop(l);
                drop(f);
                Ok((0, Some(item)))
            }
            Step::Queued(id) => Ok((id, None)),
            Step::Closed(l, f) => {
                drop(l);
                drop(f);
                Err(channel_closed_err(py, "Channel is closed"))
            }
        }
    }

    /// Claim an item reserved for `waiter_id` after `await fut` succeeds.
    pub fn claim_get(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        waiter_id: WaiterId,
    ) -> PyResult<(bool, Option<Py<PyAny>>)> {
        enum Step {
            Claimed(Py<PyAny>),
            NotReady,
            Closed,
        }
        let ch = slf.get();
        let mut to_wake = Vec::new();
        let step = {
            let mut guard = ch.state.lock();
            if let Some(pos) = guard
                .in_flight_getters
                .iter()
                .position(|(id, _)| *id == waiter_id)
            {
                let (_, item) = guard.in_flight_getters.remove(pos).unwrap();
                guard.wake_putters_locked(&mut to_wake);
                if guard.is_drained() {
                    guard.drain_closed_waiters_locked(&mut to_wake);
                }
                Step::Claimed(item)
            } else if guard.is_drained() {
                Step::Closed
            } else {
                Step::NotReady
            }
        };
        for target in to_wake {
            ch.dispatch_wake(py, slf, target);
        }
        match step {
            Step::Claimed(item) => Ok((true, Some(item))),
            Step::NotReady => Ok((false, None)),
            Step::Closed => Err(channel_closed_err(py, "Channel is closed")),
        }
    }

    /// Cancel a getter by `WaiterId`, reclaiming any in-flight item and forwarding it FIFO.
    pub fn cancel_getter(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        waiter_id: WaiterId,
    ) -> PyResult<bool> {
        let ch = slf.get();
        let mut to_wake = Vec::new();
        let (removed, reclaimed, dropped_getter) = {
            let mut guard = ch.state.lock();
            let (removed, dropped_getter) =
                if let Some(pos) = guard.getters.iter().position(|g| g.id() == waiter_id) {
                    let r = guard.getters.remove(pos);
                    (true, r)
                } else {
                    (false, None)
                };
            let in_flight_item = if let Some(pos) = guard
                .in_flight_getters
                .iter()
                .position(|(id, _)| *id == waiter_id)
            {
                guard.in_flight_getters.remove(pos).map(|(_, item)| item)
            } else {
                None
            };
            let reclaimed = in_flight_item.is_some();
            if let Some(item) = in_flight_item {
                Self::requeue_item_locked(&mut guard, item, &mut to_wake);
            }
            if guard.is_drained() {
                guard.drain_closed_waiters_locked(&mut to_wake);
            }
            (removed, reclaimed, dropped_getter)
        };
        drop(dropped_getter);
        for target in to_wake {
            ch.dispatch_wake(py, slf, target);
        }
        Ok(removed || reclaimed)
    }

    /// Re-register a woken putter at the front of the queue after a concurrent getter rollback
    /// filled the bounded buffer before `claim_put` could push.
    pub fn register_putter_front(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        fut: Py<PyAny>,
    ) -> PyResult<WaiterId> {
        enum Step {
            Queued(WaiterId),
            Closed(Py<PyAny>, Py<PyAny>),
        }
        let ch = slf.get();
        let mut to_wake = Vec::new();
        let step = {
            let mut guard = ch.state.lock();
            if guard.is_closed {
                Step::Closed(loop_obj, fut)
            } else {
                let id = guard.alloc_id();
                guard
                    .putters
                    .push_front(ChannelWaiter::Async(WaiterEntry::new(id, loop_obj, fut)));
                if guard.maxsize == 0 || guard.occupied() < guard.maxsize {
                    guard.wake_putters_locked(&mut to_wake);
                }
                Step::Queued(id)
            }
        };
        for target in to_wake {
            ch.dispatch_wake(py, slf, target);
        }
        match step {
            Step::Queued(id) => Ok(id),
            Step::Closed(l, f) => {
                drop(l);
                drop(f);
                Err(channel_closed_err(py, "Channel is closed"))
            }
        }
    }

    /// Cancel a putter by `WaiterId`, forwarding any in-flight slot reservation to the next putter.
    pub fn cancel_putter(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        waiter_id: WaiterId,
    ) -> PyResult<bool> {
        let ch = slf.get();
        let mut to_wake = Vec::new();
        let (removed, removed_in_flight, dropped_putter) = {
            let mut guard = ch.state.lock();
            let (removed, dropped_putter) =
                if let Some(pos) = guard.putters.iter().position(|p| p.id() == waiter_id) {
                    let r = guard.putters.remove(pos);
                    (true, r)
                } else {
                    (false, None)
                };
            let removed_in_flight = if let Some(pos) = guard
                .in_flight_putters
                .iter()
                .position(|id| *id == waiter_id)
            {
                guard.in_flight_putters.remove(pos);
                true
            } else {
                false
            };
            if removed_in_flight {
                guard.wake_putters_locked(&mut to_wake);
            }
            (removed, removed_in_flight, dropped_putter)
        };
        drop(dropped_putter);
        for target in to_wake {
            ch.dispatch_wake(py, slf, target);
        }
        Ok(removed || removed_in_flight)
    }

    pub fn claim_put(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        item: Py<PyAny>,
        waiter_id: WaiterId,
    ) -> PyResult<bool> {
        Ok(slf
            .get()
            .claim_put_internal(py, slf, item, waiter_id)?
            .is_none())
    }

    pub fn register_select_watcher_ticket(
        &self,
        loop_obj: Py<PyAny>,
        arbiter_fut: Py<PyAny>,
        channel_weakref: Py<PyWeakrefReference>,
    ) -> PyResult<WaiterId> {
        let id = {
            let mut guard = self.state.lock();
            if guard.is_drained() || !guard.buffer.is_empty() {
                0
            } else {
                let id = guard.alloc_id();
                guard.select_watchers.push_back(SelectWatcherEntry {
                    id,
                    event_loop: loop_obj,
                    arbiter_fut,
                    channel_weakref,
                });
                id
            }
        };
        Ok(id)
    }

    pub fn cancel_select_watcher(&self, waiter_id: WaiterId) -> bool {
        let (removed, dropped_watcher) = {
            let mut guard = self.state.lock();
            if let Some(pos) = guard.select_watchers.iter().position(|w| w.id == waiter_id) {
                let r = guard.select_watchers.remove(pos);
                (true, r)
            } else {
                (false, None)
            }
        };
        drop(dropped_watcher);
        removed
    }

    pub fn forward_select_wakeup(slf: &Bound<'_, Self>, py: Python<'_>) {
        let ch = slf.get();
        loop {
            let next = {
                let mut guard = ch.state.lock();
                if (!guard.buffer.is_empty() && guard.getters.is_empty()) || guard.is_drained() {
                    guard.select_watchers.pop_front()
                } else {
                    None
                }
            };
            let Some(watcher) = next else {
                break;
            };
            if ch.wake_select_watcher(py, slf, &watcher) {
                break;
            }
        }
    }

    pub fn getters_len(&self) -> usize {
        self.state.lock().getters.len()
    }

    pub fn putters_len(&self) -> usize {
        self.state.lock().putters.len()
    }

    pub fn notifiers_len(&self) -> usize {
        self.state.lock().select_watchers.len()
    }
}
