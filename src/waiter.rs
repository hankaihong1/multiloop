use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique 64-bit monotonically increasing ticket ID for a registered waiter.
/// `0` is reserved to indicate immediate fast-path completion without queuing.
pub type WaiterId = u64;

#[allow(dead_code)]
static NEXT_TASK_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Monotonically allocates a unique 64-bit task token (starting at 1).
#[allow(dead_code)]
#[inline]
pub(crate) fn alloc_task_token() -> u64 {
    NEXT_TASK_TOKEN.fetch_add(1, Ordering::Relaxed)
}

/// Opaque cross-thread waiter registration entry stored inside Rust state machines.
///
/// Follows Boundary Law 2: `event_loop` and `future` are treated as opaque handles
/// and are NEVER inspected (e.g. `fut.done()`) or invoked while holding a Rust mutex.
pub struct WaiterEntry {
    pub id: WaiterId,
    pub event_loop: Py<PyAny>,
    pub future: Py<PyAny>,
    pub task_id: usize,
    pub channel_token: Option<Py<PyAny>>,
}

impl WaiterEntry {
    #[inline]
    pub fn new(id: WaiterId, event_loop: Py<PyAny>, future: Py<PyAny>) -> Self {
        Self {
            id,
            event_loop,
            future,
            task_id: 0,
            channel_token: None,
        }
    }

    #[inline]
    pub fn with_task(
        id: WaiterId,
        event_loop: Py<PyAny>,
        future: Py<PyAny>,
        task_id: usize,
    ) -> Self {
        Self {
            id,
            event_loop,
            future,
            task_id,
            channel_token: None,
        }
    }

    #[inline]
    pub fn with_token(
        id: WaiterId,
        event_loop: Py<PyAny>,
        future: Py<PyAny>,
        channel_token: Py<PyAny>,
    ) -> Self {
        Self {
            id,
            event_loop,
            future,
            task_id: 0,
            channel_token: Some(channel_token),
        }
    }
}

static COMPLETE_WAITER_FN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static COMPLETE_SELECT_WATCHER_FN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static CANCEL_WAITER_FN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// Callback executed on the target event loop's owning OS thread via `call_soon_threadsafe`.
#[pyfunction]
#[pyo3(signature = (fut, val = None, is_exc = false))]
pub fn _complete_waiter(
    py: Python<'_>,
    fut: &Bound<'_, PyAny>,
    val: Option<Bound<'_, PyAny>>,
    is_exc: bool,
) {
    if fut.hasattr("set_result").unwrap_or(false) {
        let is_done = fut
            .call_method0("done")
            .and_then(|d| d.extract::<bool>())
            .unwrap_or(false);
        if is_done {
            return;
        }
        if is_exc {
            if let Some(exc) = val {
                let _ = fut.call_method1("set_exception", (exc,));
            }
        } else {
            let res = val.unwrap_or_else(|| py.None().into_bound(py));
            let _ = fut.call_method1("set_result", (res,));
        }
    } else if fut.hasattr("set").unwrap_or(false) {
        let _ = fut.call_method0("set");
    }
}

/// Backwards compatibility alias for _complete_waiter.
#[pyfunction]
#[pyo3(signature = (fut, val = None, is_exc = false))]
pub fn _complete_waiter_future(
    py: Python<'_>,
    fut: &Bound<'_, PyAny>,
    val: Option<Bound<'_, PyAny>>,
    is_exc: bool,
) {
    _complete_waiter(py, fut, val, is_exc)
}

#[inline]
pub fn get_complete_waiter_fn(py: Python<'_>) -> &Bound<'_, PyAny> {
    COMPLETE_WAITER_FN
        .get_or_init(py, || {
            pyo3::wrap_pyfunction!(_complete_waiter, py)
                .expect("failed to wrap _complete_waiter")
                .into_any()
                .unbind()
        })
        .bind(py)
}

/// Callback executed on target event loop thread to arbitrate select watcher fulfillment.
#[pyfunction]
pub fn _complete_select_watcher(
    _py: Python<'_>,
    raw_ch: &Bound<'_, PyAny>,
    arbiter_fut: &Bound<'_, PyAny>,
    token_ref: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let ch_obj = if token_ref.is_callable() {
        token_ref.call0()?
    } else {
        token_ref.clone()
    };

    let is_done = if arbiter_fut.hasattr("done")? {
        arbiter_fut.call_method0("done")?.extract::<bool>()?
    } else {
        false
    };

    if !ch_obj.is_none() && !is_done {
        let _ = arbiter_fut.call_method1("set_result", (ch_obj,));
    } else {
        let _ = raw_ch.call_method0("forward_select_wakeup");
    }
    Ok(())
}

#[inline]
pub fn get_complete_select_watcher_fn(py: Python<'_>) -> &Bound<'_, PyAny> {
    COMPLETE_SELECT_WATCHER_FN
        .get_or_init(py, || {
            pyo3::wrap_pyfunction!(_complete_select_watcher, py)
                .expect("failed to wrap _complete_select_watcher")
                .into_any()
                .unbind()
        })
        .bind(py)
}

/// Wake a `WaiterEntry` strictly outside of any Rust `Mutex`.
///
/// Returns `true` if the wakeup was successfully scheduled on the target event loop
/// (or handled in loop-less Rust unit test mode), or `false` if `call_soon_threadsafe`
/// failed (e.g., target `AbstractEventLoop` is closed), signaling the caller to
/// roll back the in-flight reservation via `cancel_waiter(entry.id)`.
pub fn wake_waiter_outside_lock(
    py: Python<'_>,
    entry: &WaiterEntry,
    result: Option<Py<PyAny>>,
    is_exception: bool,
) -> bool {
    let loop_bound = entry.event_loop.bind(py);
    if loop_bound.is_none() {
        let fut = entry.future.bind(py);
        if fut.is_none() {
            return true;
        }
        _complete_waiter(py, fut, result.map(|r| r.into_bound(py)), is_exception);
        return true;
    }

    let fut = entry.future.bind(py);
    let wake_fn = get_complete_waiter_fn(py);
    let res = match result {
        Some(v) => {
            let is_exc = pyo3::types::PyBool::new(py, is_exception);
            loop_bound.call_method1("call_soon_threadsafe", (wake_fn, fut, v.bind(py), is_exc))
        }
        None if !is_exception => loop_bound.call_method1("call_soon_threadsafe", (wake_fn, fut)),
        None => {
            let is_exc = pyo3::types::PyBool::new(py, true);
            loop_bound.call_method1("call_soon_threadsafe", (wake_fn, fut, py.None(), is_exc))
        }
    };
    res.is_ok()
}

/// Wake a `(loop_obj, fut_obj)` pair outside of any Rust `Mutex` with `None`.
#[inline]
pub fn wake_pair_outside_lock(py: Python<'_>, loop_obj: &Py<PyAny>, fut_obj: &Py<PyAny>) -> bool {
    let loop_bound = loop_obj.bind(py);
    if loop_bound.is_none() {
        let fut = fut_obj.bind(py);
        if !fut.is_none() {
            _complete_waiter(py, fut, None, false);
        }
        return true;
    }
    let wake_fn = get_complete_waiter_fn(py);
    loop_bound
        .call_method1("call_soon_threadsafe", (wake_fn, fut_obj.bind(py)))
        .is_ok()
}

/// Callback executed on the target event loop thread to cancel a parked waiter Future.
#[pyfunction]
pub fn _cancel_waiter(fut: &Bound<'_, PyAny>) {
    if !fut.is_none() {
        let _ = fut.call_method0("cancel");
    }
}

#[inline]
pub fn get_cancel_waiter_fn(py: Python<'_>) -> &Bound<'_, PyAny> {
    CANCEL_WAITER_FN
        .get_or_init(py, || {
            pyo3::wrap_pyfunction!(_cancel_waiter, py)
                .expect("failed to wrap _cancel_waiter")
                .into_any()
                .unbind()
        })
        .bind(py)
}

/// Cancel a parked waiter `WaiterEntry` strictly outside of any Rust `Mutex`.
#[inline]
pub fn cancel_waiter_outside_lock(py: Python<'_>, entry: &WaiterEntry) -> bool {
    let loop_bound = entry.event_loop.bind(py);
    let fut = entry.future.bind(py);
    if loop_bound.is_none() {
        _cancel_waiter(fut);
        return true;
    }
    let cancel_fn = get_cancel_waiter_fn(py);
    loop_bound
        .call_method1("call_soon_threadsafe", (cancel_fn, fut))
        .is_ok()
}
