pub mod cancel;
pub mod channel;
pub mod errors;
pub mod metrics;
pub mod pool;
pub mod rwlock;
pub mod sync;
pub mod waiter;
pub mod waitgroup;

use pyo3::prelude::*;

pub use cancel::RawTaskCancelState;
pub use channel::RawAsyncChannel;
pub use metrics::AtomicMetrics;
pub use pool::NativeWorkerPool;
pub use rwlock::RawAsyncRWMutex;
pub use sync::{
    RawAsyncBarrier, RawAsyncCapacityLimiter, RawAsyncCondition, RawAsyncEvent, RawAsyncLock,
    RawAsyncOnce, RawAsyncSemaphore,
};
pub use waitgroup::RawAsyncWaitGroup;

#[pymodule(gil_used = false)]
fn _multiloop_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RawTaskCancelState>()?;
    m.add_class::<AtomicMetrics>()?;
    m.add_class::<NativeWorkerPool>()?;
    m.add_class::<RawAsyncChannel>()?;
    m.add_class::<RawAsyncWaitGroup>()?;
    m.add_class::<RawAsyncRWMutex>()?;
    m.add_class::<RawAsyncLock>()?;
    m.add_class::<RawAsyncSemaphore>()?;
    m.add_class::<RawAsyncCapacityLimiter>()?;
    m.add_class::<RawAsyncEvent>()?;
    m.add_class::<RawAsyncCondition>()?;
    m.add_class::<RawAsyncBarrier>()?;
    m.add_class::<RawAsyncOnce>()?;
    m.add_function(wrap_pyfunction!(waiter::_complete_select_watcher, m)?)?;
    m.add_function(wrap_pyfunction!(waiter::_complete_waiter, m)?)?;
    m.add_function(wrap_pyfunction!(waiter::_cancel_waiter, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
