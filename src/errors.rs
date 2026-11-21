#![allow(dead_code)]

use pyo3::exceptions::{PyRuntimeError, PyTimeoutError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyType;

static CHANNEL_CLOSED_ERR: PyOnceLock<Option<Py<PyType>>> = PyOnceLock::new();
static WOULD_BLOCK_ERR: PyOnceLock<Option<Py<PyType>>> = PyOnceLock::new();
static POOL_CLOSED_ERR: PyOnceLock<Option<Py<PyType>>> = PyOnceLock::new();
static TIMEOUT_ERR: PyOnceLock<Option<Py<PyType>>> = PyOnceLock::new();

fn get_exception_class<'py>(
    py: Python<'py>,
    lock: &PyOnceLock<Option<Py<PyType>>>,
    name: &str,
) -> Option<Bound<'py, PyType>> {
    lock.get_or_init(py, || {
        py.import("multiloop.exceptions")
            .ok()
            .and_then(|m| m.getattr(name).ok())
            .and_then(|cls| cls.extract::<Py<PyType>>().ok())
    })
    .as_ref()
    .map(|cls| cls.bind(py).clone())
}

pub(crate) fn channel_closed_err(py: Python<'_>, msg: &'static str) -> PyErr {
    if let Some(cls) = get_exception_class(py, &CHANNEL_CLOSED_ERR, "ChannelClosedError") {
        if let Ok(err) = cls.call1((msg,)) {
            return PyErr::from_value(err.into_any());
        }
    }
    PyRuntimeError::new_err(msg)
}

pub(crate) fn would_block_err(py: Python<'_>, msg: &'static str) -> PyErr {
    if let Some(cls) = get_exception_class(py, &WOULD_BLOCK_ERR, "WouldBlock") {
        if let Ok(err) = cls.call1((msg,)) {
            return PyErr::from_value(err.into_any());
        }
    }
    PyRuntimeError::new_err(msg)
}

pub(crate) fn pool_closed_err(py: Python<'_>, msg: &'static str) -> PyErr {
    if let Some(cls) = get_exception_class(py, &POOL_CLOSED_ERR, "ThreadPoolClosedError") {
        if let Ok(err) = cls.call1((msg,)) {
            return PyErr::from_value(err.into_any());
        }
    }
    PyRuntimeError::new_err(msg)
}

pub(crate) fn timeout_err(py: Python<'_>, msg: &'static str) -> PyErr {
    if let Some(cls) = get_exception_class(py, &TIMEOUT_ERR, "TimeoutError") {
        if let Ok(err) = cls.call1((msg,)) {
            return PyErr::from_value(err.into_any());
        }
    }
    PyTimeoutError::new_err(msg)
}
