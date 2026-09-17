use parking_lot::{Condvar, Mutex};
use pyo3::exceptions::{PyRuntimeError, PyTimeoutError};
use pyo3::prelude::*;
use std::collections::VecDeque;
use std::sync::Arc;

/// A registered async waiter: (event loop, future, optional channel token).
pub(crate) struct ChannelWaiter {
    pub(crate) event_loop: Py<PyAny>,
    pub(crate) future: Py<PyAny>,
    pub(crate) channel_token: Option<Py<PyAny>>,
}

pub(crate) struct ChannelState {
    pub(crate) buffer: VecDeque<Py<PyAny>>,
    pub(crate) maxsize: usize,
    pub(crate) is_closed: bool,
    pub(crate) getters: VecDeque<ChannelWaiter>,
    pub(crate) putters: VecDeque<ChannelWaiter>,
    pub(crate) select_watchers: VecDeque<ChannelWaiter>,
    pub(crate) in_flight_putters: usize,
}

#[pyclass(module = "multiloop._multiloop_core")]
pub struct RawAsyncChannel {
    pub(crate) state: Arc<Mutex<ChannelState>>,
    condvar: Arc<Condvar>,
    wake_fn: Option<Py<PyAny>>,
    select_wake_fn: Option<Py<PyAny>>,
}

/// Target to wake outside of the Mutex lock to eliminate GIL-Mutex deadlocks.
pub(crate) enum WakeTarget {
    Waiter(ChannelWaiter, Option<Py<PyAny>>, bool),
    SelectWatcher(ChannelWaiter, Py<PyAny>),
}

impl RawAsyncChannel {
    fn wake_waiter(
        &self,
        py: Python<'_>,
        waiter: &ChannelWaiter,
        result: Option<Py<PyAny>>,
        is_exception: bool,
    ) {
        if let Some(ref w_fn) = self.wake_fn {
            let loop_obj = waiter.event_loop.bind(py);
            let fut = waiter.future.bind(py);
            let has_val = result.is_some();
            let is_exc = pyo3::types::PyBool::new(py, is_exception);
            let _ = match result {
                Some(v) => loop_obj.call_method1(
                    "call_soon_threadsafe",
                    (w_fn.bind(py), fut, v.bind(py), is_exc, has_val),
                ),
                None => loop_obj.call_method1(
                    "call_soon_threadsafe",
                    (w_fn.bind(py), fut, py.None(), is_exc, has_val),
                ),
            };
        } else {
            let fut = waiter.future.bind(py);
            if is_exception {
                if let Some(exc) = result {
                    let _ = fut.call_method1("set_exception", (exc,));
                }
            } else {
                let res = result.unwrap_or_else(|| py.None());
                let _ = fut.call_method1("set_result", (res,));
            }
        }
    }

    fn wake_select_watcher(
        &self,
        py: Python<'_>,
        watcher: &ChannelWaiter,
        token: Bound<'_, PyAny>,
    ) {
        if let Some(ref sw_fn) = self.select_wake_fn {
            let loop_obj = watcher.event_loop.bind(py);
            let fut = watcher.future.bind(py);
            let _ = loop_obj.call_method1("call_soon_threadsafe", (sw_fn.bind(py), fut, token));
        } else {
            let _ = watcher.future.bind(py).call_method1("set_result", (token,));
        }
    }

    pub(crate) fn dispatch_wake(&self, py: Python<'_>, target: WakeTarget) {
        match target {
            WakeTarget::Waiter(w, res, is_exc) => {
                self.wake_waiter(py, &w, res, is_exc);
            }
            WakeTarget::SelectWatcher(w, token) => {
                self.wake_select_watcher(py, &w, token.into_bound(py));
            }
        }
    }
}

#[pymethods]
impl RawAsyncChannel {
    #[new]
    #[pyo3(signature = (maxsize = 0, wake_fn = None, select_wake_fn = None))]
    pub fn new(
        maxsize: usize,
        wake_fn: Option<Py<PyAny>>,
        select_wake_fn: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ChannelState {
                maxsize,
                buffer: VecDeque::new(),
                is_closed: false,
                getters: VecDeque::new(),
                putters: VecDeque::new(),
                select_watchers: VecDeque::new(),
                in_flight_putters: 0,
            })),
            condvar: Arc::new(Condvar::new()),
            wake_fn,
            select_wake_fn,
        }
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let mut to_wake = Vec::new();
        {
            let mut guard = self.state.lock();
            if guard.is_closed {
                return Ok(());
            }
            guard.is_closed = true;
            guard.in_flight_putters = 0;
            let closed_exc: Py<PyAny> = PyRuntimeError::new_err("Channel is closed")
                .into_value(py)
                .into_any();
            for g in guard.getters.drain(..) {
                to_wake.push(WakeTarget::Waiter(g, Some(closed_exc.clone_ref(py)), true));
            }
            for p in guard.putters.drain(..) {
                to_wake.push(WakeTarget::Waiter(p, Some(closed_exc.clone_ref(py)), true));
            }
            for w in guard.select_watchers.drain(..) {
                let token = w
                    .channel_token
                    .as_ref()
                    .map(|t| t.clone_ref(py))
                    .unwrap_or_else(|| py.None());
                to_wake.push(WakeTarget::SelectWatcher(w, token));
            }
        }
        for target in to_wake {
            self.dispatch_wake(py, target);
        }
        self.condvar.notify_all();
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.state.lock().is_closed
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
        guard.maxsize > 0 && (guard.buffer.len() + guard.in_flight_putters >= guard.maxsize)
    }

    pub fn try_send(&self, py: Python<'_>, item: Py<PyAny>) -> PyResult<bool> {
        let mut to_wake = Vec::new();
        {
            let mut guard = self.state.lock();
            if guard.is_closed {
                return Err(PyRuntimeError::new_err("Channel is closed"));
            }
            if let Some(getter) = guard.getters.pop_front() {
                to_wake.push(WakeTarget::Waiter(getter, Some(item), false));
                if guard.maxsize > 0 {
                    while !guard.putters.is_empty()
                        && (guard.buffer.len() + guard.in_flight_putters) < guard.maxsize
                    {
                        if let Some(next_putter) = guard.putters.pop_front() {
                            guard.in_flight_putters += 1;
                            to_wake.push(WakeTarget::Waiter(next_putter, None, false));
                        }
                    }
                }
            } else if let Some(watcher) = guard.select_watchers.pop_front() {
                guard.buffer.push_back(item);
                let token = watcher
                    .channel_token
                    .as_ref()
                    .map(|t| t.clone_ref(py))
                    .unwrap_or_else(|| py.None());
                to_wake.push(WakeTarget::SelectWatcher(watcher, token));
            } else {
                if guard.maxsize > 0
                    && (guard.buffer.len() + guard.in_flight_putters >= guard.maxsize
                        || !guard.putters.is_empty())
                {
                    return Ok(false);
                }
                guard.buffer.push_back(item);
            }
        }
        for target in to_wake {
            self.dispatch_wake(py, target);
        }
        self.condvar.notify_all();
        Ok(true)
    }

    pub fn try_recv(&self, py: Python<'_>) -> PyResult<(bool, Option<Py<PyAny>>)> {
        let mut to_wake = None;
        let res = {
            let mut guard = self.state.lock();
            if let Some(item) = guard.buffer.pop_front() {
                if guard.maxsize > 0 {
                    if let Some(putter) = guard.putters.pop_front() {
                        guard.in_flight_putters += 1;
                        to_wake = Some(WakeTarget::Waiter(putter, None, false));
                    }
                }
                Ok((true, Some(item)))
            } else if guard.is_closed {
                Err(PyRuntimeError::new_err("Channel is closed"))
            } else {
                Ok((false, None))
            }
        };
        if let Some(target) = to_wake {
            self.dispatch_wake(py, target);
        }
        if res.as_ref().is_ok_and(|r| r.0) {
            self.condvar.notify_all();
        }
        res
    }

    /// Synchronously send an item into the channel from a worker or background OS thread.
    ///
    /// Blocks the current thread using `parking_lot::Condvar` without busy-waiting.
    /// Automatically detaches from Python via `py.detach()` during condvar waits so other
    /// threads can run uninterrupted.
    #[pyo3(signature = (item, timeout = None))]
    pub fn send_sync(&self, py: Python<'_>, item: Py<PyAny>, timeout: Option<f64>) -> PyResult<()> {
        let deadline = timeout
            .map(|t| std::time::Instant::now() + std::time::Duration::from_secs_f64(t.max(0.0)));
        let state = self.state.clone();
        let condvar = self.condvar.clone();
        loop {
            if self.try_send(py, item.clone_ref(py))? {
                return Ok(());
            }
            let is_closed = self.state.lock().is_closed;
            if is_closed {
                return Err(PyRuntimeError::new_err("Channel is closed"));
            }
            let state_clone = state.clone();
            let condvar_clone = condvar.clone();
            let timed_out = py.detach(move || {
                let mut guard = state_clone.lock();
                if guard.is_closed {
                    return false;
                }
                if guard.maxsize == 0
                    || (guard.buffer.len() + guard.in_flight_putters < guard.maxsize
                        && guard.putters.is_empty())
                {
                    return false;
                }
                if let Some(dl) = deadline {
                    let now = std::time::Instant::now();
                    if now >= dl {
                        true
                    } else {
                        condvar_clone.wait_for(&mut guard, dl - now).timed_out()
                    }
                } else {
                    condvar_clone.wait(&mut guard);
                    false
                }
            });
            if timed_out {
                let guard = self.state.lock();
                if guard.maxsize > 0
                    && (guard.buffer.len() + guard.in_flight_putters >= guard.maxsize
                        || !guard.putters.is_empty())
                {
                    return Err(PyTimeoutError::new_err("send_sync timed out"));
                }
            }
        }
    }

    /// Synchronously receive an item from the channel from a worker or background OS thread.
    ///
    /// Blocks the current thread using `parking_lot::Condvar` without busy-waiting.
    /// Automatically detaches from Python via `py.detach()` during condvar waits so other
    /// threads can run uninterrupted.
    #[pyo3(signature = (timeout = None))]
    pub fn recv_sync(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        let deadline = timeout
            .map(|t| std::time::Instant::now() + std::time::Duration::from_secs_f64(t.max(0.0)));
        let state = self.state.clone();
        let condvar = self.condvar.clone();
        loop {
            let (has_item, item_opt) = self.try_recv(py)?;
            if has_item {
                if let Some(item) = item_opt {
                    return Ok(item);
                }
            }
            {
                let guard = self.state.lock();
                if guard.is_closed && guard.buffer.is_empty() {
                    return Err(PyRuntimeError::new_err("Channel is closed"));
                }
            }
            let state_clone = state.clone();
            let condvar_clone = condvar.clone();
            let timed_out = py.detach(move || {
                let mut guard = state_clone.lock();
                if guard.is_closed || !guard.buffer.is_empty() {
                    return false;
                }
                if let Some(dl) = deadline {
                    let now = std::time::Instant::now();
                    if now >= dl {
                        true
                    } else {
                        condvar_clone.wait_for(&mut guard, dl - now).timed_out()
                    }
                } else {
                    condvar_clone.wait(&mut guard);
                    false
                }
            });
            if timed_out {
                let guard = self.state.lock();
                if guard.buffer.is_empty() {
                    return Err(PyTimeoutError::new_err("recv_sync timed out"));
                }
            }
        }
    }

    pub fn register_getter(
        &self,
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        fut: Py<PyAny>,
    ) -> PyResult<(bool, Option<Py<PyAny>>)> {
        let mut to_wake = None;
        let res = {
            let mut guard = self.state.lock();
            if guard.getters.is_empty() && !guard.buffer.is_empty() {
                let item = guard.buffer.pop_front().unwrap();
                if guard.maxsize > 0 {
                    if let Some(putter) = guard.putters.pop_front() {
                        guard.in_flight_putters += 1;
                        to_wake = Some(WakeTarget::Waiter(putter, None, false));
                    }
                }
                Ok((true, Some(item)))
            } else if guard.is_closed {
                Err(PyRuntimeError::new_err("Channel is closed"))
            } else {
                guard.getters.push_back(ChannelWaiter {
                    event_loop: loop_obj,
                    future: fut,
                    channel_token: None,
                });
                Ok((false, None))
            }
        };
        if let Some(target) = to_wake {
            self.dispatch_wake(py, target);
        }
        res
    }

    pub fn unregister_getter(&self, py: Python<'_>, fut: &Bound<'_, PyAny>) -> PyResult<bool> {
        let mut to_wake = Vec::new();
        let target_ptr = fut.as_ptr();
        let removed = {
            let mut guard = self.state.lock();
            let removed = if let Some(pos) = guard
                .getters
                .iter()
                .position(|g| g.future.as_ptr() == target_ptr)
            {
                guard.getters.remove(pos);
                true
            } else {
                false
            };
            if !removed && !guard.buffer.is_empty() {
                if let Some(next_getter) = guard.getters.pop_front() {
                    let item = guard.buffer.pop_front().unwrap();
                    to_wake.push(WakeTarget::Waiter(next_getter, Some(item), false));
                    if guard.maxsize > 0 {
                        if let Some(putter) = guard.putters.pop_front() {
                            guard.in_flight_putters += 1;
                            to_wake.push(WakeTarget::Waiter(putter, None, false));
                        }
                    }
                } else if let Some(watcher) = guard.select_watchers.pop_front() {
                    let token = watcher
                        .channel_token
                        .as_ref()
                        .map(|t| t.clone_ref(py))
                        .unwrap_or_else(|| py.None());
                    to_wake.push(WakeTarget::SelectWatcher(watcher, token));
                }
            }
            removed
        };
        for target in to_wake {
            self.dispatch_wake(py, target);
        }
        if removed {
            self.condvar.notify_all();
        }
        Ok(removed)
    }

    pub fn register_putter(
        &self,
        _py: Python<'_>,
        loop_obj: Py<PyAny>,
        fut: Py<PyAny>,
    ) -> PyResult<bool> {
        let mut guard = self.state.lock();
        if guard.is_closed {
            return Err(PyRuntimeError::new_err("Channel is closed"));
        }
        if guard.putters.is_empty()
            && (guard.maxsize == 0
                || (guard.buffer.len() + guard.in_flight_putters) < guard.maxsize)
        {
            return Ok(true);
        }
        guard.putters.push_back(ChannelWaiter {
            event_loop: loop_obj,
            future: fut,
            channel_token: None,
        });
        Ok(false)
    }

    pub fn unregister_putter(&self, py: Python<'_>, fut: &Bound<'_, PyAny>) -> PyResult<bool> {
        let mut to_wake = None;
        let target_ptr = fut.as_ptr();
        let removed = {
            let mut guard = self.state.lock();
            let removed = if let Some(pos) = guard
                .putters
                .iter()
                .position(|p| p.future.as_ptr() == target_ptr)
            {
                guard.putters.remove(pos);
                true
            } else {
                false
            };
            if !removed {
                if guard.in_flight_putters > 0 {
                    guard.in_flight_putters -= 1;
                }
                if guard.maxsize == 0
                    || (guard.buffer.len() + guard.in_flight_putters) < guard.maxsize
                {
                    if let Some(next_putter) = guard.putters.pop_front() {
                        guard.in_flight_putters += 1;
                        to_wake = Some(WakeTarget::Waiter(next_putter, None, false));
                    }
                }
            }
            removed
        };
        if let Some(target) = to_wake {
            self.dispatch_wake(py, target);
        }
        if removed {
            self.condvar.notify_all();
        }
        Ok(removed)
    }

    pub fn claim_put(&self, py: Python<'_>, item: Py<PyAny>) -> PyResult<bool> {
        let mut to_wake = Vec::new();
        {
            let mut guard = self.state.lock();
            if guard.is_closed {
                return Err(PyRuntimeError::new_err("Channel is closed"));
            }
            if guard.in_flight_putters > 0 {
                guard.in_flight_putters -= 1;
            }
            if let Some(getter) = guard.getters.pop_front() {
                to_wake.push(WakeTarget::Waiter(getter, Some(item), false));
                if guard.maxsize > 0 {
                    while !guard.putters.is_empty()
                        && (guard.buffer.len() + guard.in_flight_putters) < guard.maxsize
                    {
                        if let Some(next_putter) = guard.putters.pop_front() {
                            guard.in_flight_putters += 1;
                            to_wake.push(WakeTarget::Waiter(next_putter, None, false));
                        }
                    }
                }
            } else if let Some(watcher) = guard.select_watchers.pop_front() {
                guard.buffer.push_back(item);
                let token = watcher
                    .channel_token
                    .as_ref()
                    .map(|t| t.clone_ref(py))
                    .unwrap_or_else(|| py.None());
                to_wake.push(WakeTarget::SelectWatcher(watcher, token));
            } else {
                guard.buffer.push_back(item);
            }
        }
        for target in to_wake {
            self.dispatch_wake(py, target);
        }
        self.condvar.notify_all();
        Ok(true)
    }

    pub fn register_select_watcher(
        &self,
        _py: Python<'_>,
        loop_obj: Py<PyAny>,
        arbiter_fut: Py<PyAny>,
        channel_token: Py<PyAny>,
    ) -> PyResult<bool> {
        let mut guard = self.state.lock();
        if guard.is_closed && guard.buffer.is_empty() {
            return Ok(false);
        }
        if !guard.buffer.is_empty() {
            return Ok(false);
        }
        guard.select_watchers.push_back(ChannelWaiter {
            event_loop: loop_obj,
            future: arbiter_fut,
            channel_token: Some(channel_token),
        });
        Ok(true)
    }

    pub fn unregister_select_watcher(&self, _py: Python<'_>, fut: &Bound<'_, PyAny>) -> bool {
        let mut guard = self.state.lock();
        let target_ptr = fut.as_ptr();
        if let Some(pos) = guard
            .select_watchers
            .iter()
            .position(|w| w.future.as_ptr() == target_ptr)
        {
            guard.select_watchers.remove(pos);
            true
        } else {
            false
        }
    }

    pub fn forward_select_wakeup(&self, py: Python<'_>) {
        let mut to_wake = None;
        {
            let mut guard = self.state.lock();
            if !guard.buffer.is_empty() && guard.getters.is_empty() {
                if let Some(watcher) = guard.select_watchers.pop_front() {
                    let token = watcher
                        .channel_token
                        .as_ref()
                        .map(|t| t.clone_ref(py))
                        .unwrap_or_else(|| py.None());
                    to_wake = Some(WakeTarget::SelectWatcher(watcher, token));
                }
            }
        }
        if let Some(target) = to_wake {
            self.dispatch_wake(py, target);
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
