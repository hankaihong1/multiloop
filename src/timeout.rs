//! Panic-free timeout validation and deadline computation helpers.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::time::{Duration, Instant};

/// Upper bound (10 years in seconds) applied to positive finite timeouts so
/// `Duration::try_from_secs_f64` and `Instant::checked_add` never overflow
/// platform clock/timespec limits on huge floats such as `1e20`.
pub(crate) const MAX_FINITE_TIMEOUT_SECS: f64 = 315_360_000.0;

/// Validate a floating-point timeout (in seconds) and convert it into an
/// optional bounded `Duration`.
///
/// - `NaN` and `-inf` return `Err(PyValueError)`.
/// - `+inf` returns `Ok(None)` (unbounded wait).
/// - Finite values `<= 0.0` return `Ok(Some(Duration::ZERO))`.
/// - Finite values `> 0.0` are capped at `MAX_FINITE_TIMEOUT_SECS` and
///   converted via `Duration::try_from_secs_f64` without panicking.
pub(crate) fn timeout_to_duration(timeout_secs: f64) -> PyResult<Option<Duration>> {
    if timeout_secs.is_nan() {
        return Err(PyValueError::new_err("timeout must not be NaN"));
    }
    if timeout_secs.is_infinite() {
        if timeout_secs.is_sign_negative() {
            return Err(PyValueError::new_err(
                "timeout must not be negative infinity",
            ));
        }
        return Ok(None);
    }
    if timeout_secs <= 0.0 {
        return Ok(Some(Duration::ZERO));
    }
    let clamped = timeout_secs.min(MAX_FINITE_TIMEOUT_SECS);
    match Duration::try_from_secs_f64(clamped) {
        Ok(dur) => Ok(Some(dur)),
        Err(_) => Ok(None),
    }
}

/// Convert a floating-point timeout (in seconds) into an optional monotonic
/// `Instant` deadline.
///
/// Returns `Ok(None)` for `+inf` or if adding the bounded duration to
/// `Instant::now()` would overflow the platform clock range.
pub(crate) fn deadline_from_secs(timeout_secs: f64) -> PyResult<Option<Instant>> {
    match timeout_to_duration(timeout_secs)? {
        Some(dur) if dur.is_zero() => Ok(Some(Instant::now())),
        Some(dur) => Ok(Instant::now().checked_add(dur)),
        None => Ok(None),
    }
}

/// Convert an optional floating-point timeout (`None` means unbounded wait)
/// into an optional monotonic `Instant` deadline.
pub(crate) fn deadline_from_opt_secs(timeout_secs: Option<f64>) -> PyResult<Option<Instant>> {
    match timeout_secs {
        Some(secs) => deadline_from_secs(secs),
        None => Ok(None),
    }
}
