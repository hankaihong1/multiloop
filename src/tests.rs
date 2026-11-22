use super::*;
use parking_lot::Mutex;
use pyo3::{Py, PyAny, Python};

// CPython 3.14t gc.collect() returns 0 without collecting if called concurrently by another thread.
static GC_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn test_waitgroup_done_underflow() {
    Python::attach(|py| {
        let wg = RawAsyncWaitGroup::new();
        // Counter starts at 0, done() should return error (not wrap around).
        let result = wg.done(py);
        assert!(result.is_err(), "done() on counter=0 should return Err");
    });
}

#[test]
fn test_waitgroup_double_check() {
    Python::attach(|py| {
        let wg = RawAsyncWaitGroup::new();
        wg.add(py, 1).unwrap();
        // done() decrements from 1 → 0, wakes waiters and returns count.
        let result = wg.done(py);
        assert!(result.is_ok(), "done() on counter=1 should succeed");
        assert_eq!(result.unwrap(), 0);
        // After done(), counter is 0, so register_waiter_ticket returns None
        // (meaning "already done, wake immediately").
        assert!(
            wg.register_waiter_ticket(py.None(), py.None()).is_none(),
            "register_waiter_ticket after done should return None"
        );
    });
}

#[test]
fn test_waitgroup_add_negative() {
    Python::attach(|py| {
        let wg = RawAsyncWaitGroup::new();
        // add(-1) on a zero counter must error (Go panics on a negative
        // counter; we mirror that with RuntimeError).
        assert!(
            wg.add(py, -1).is_err(),
            "add(-1) on counter=0 should return Err"
        );
        wg.add(py, 2).unwrap();
        // add(-3) would drive 2 → -1: must error.
        assert!(
            wg.add(py, -3).is_err(),
            "add(-3) on counter=2 should return Err"
        );
        // add(-2) brings 2 → 0: legal, mirrors done().
        wg.add(py, -2).unwrap();
        // Counter is 0 again: a positive add is legal.
        wg.add(py, 1).unwrap();
    });
}

#[test]
fn test_waitgroup_unregister_waiter() {
    Python::attach(|py| {
        let wg = RawAsyncWaitGroup::new();
        wg.add(py, 1).unwrap();
        let fut = py.None();
        let ticket = wg.register_waiter_ticket(py.None(), fut.clone_ref(py));
        assert!(ticket.is_some(), "register on counter=1 should queue");
        let ticket_id = ticket.unwrap();
        assert!(
            wg.cancel_waiter_ticket(ticket_id),
            "first cancel should remove the entry"
        );
        assert!(
            !wg.cancel_waiter_ticket(ticket_id),
            "second cancel is a no-op"
        );
        let woken = wg.done(py).unwrap();
        assert_eq!(woken, 0, "no stale entries after cancel");
    });
}

#[test]
fn test_channel_try_send_recv() {
    Python::attach(|py| {
        let ch = Bound::new(py, RawAsyncChannel::new(0)).unwrap(); // unbounded channel
        let item: Py<PyAny> = py.None();
        let send_result = RawAsyncChannel::try_send(&ch, py, item);
        assert!(send_result.is_ok(), "try_send should succeed");
        assert!(send_result.unwrap(), "try_send should return Ok(true)");
        let recv_result = RawAsyncChannel::try_recv(&ch, py);
        assert!(recv_result.is_ok(), "try_recv should succeed");
        assert!(recv_result.unwrap().bind(py).is_none());
    });
}

#[test]
fn test_channel_close_then_recv() {
    Python::attach(|py| {
        let ch = Bound::new(py, RawAsyncChannel::new(0)).unwrap(); // unbounded channel
        RawAsyncChannel::close(&ch, py).unwrap();
        assert!(ch.get().is_drained());
        // After close, try_recv on an empty channel returns Err.
        let result = RawAsyncChannel::try_recv(&ch, py);
        assert!(
            result.is_err(),
            "try_recv on closed empty channel should return Err"
        );
    });
}

#[test]
fn test_atomicmetrics_inc_dec() {
    let metrics = AtomicMetrics::new(4);
    metrics.inc_active(0);
    metrics.inc_active(0);
    metrics.inc_active(0);
    metrics.dec_active(0);
    metrics.dec_active(0);
    assert_eq!(metrics.get_active(0), 1, "active should be 3 - 2 = 1");
    assert_eq!(metrics.get_completed(0), 2, "completed should be 2");
}

#[test]
fn test_atomicmetrics_no_crash() {
    let metrics = AtomicMetrics::new(4);
    // dec without prior inc: must not panic.
    // AtomicUsize wraps around on underflow (well-defined behavior).
    metrics.dec_active(0);
    // If we got here without panicking, the test passes.
}

#[test]
fn test_waitgroup_add_negative_returns_waiters() {
    Python::attach(|py| {
        let wg = RawAsyncWaitGroup::new();
        wg.add(py, 2).unwrap();
        let fut = py.None();
        let ticket = wg.register_waiter_ticket(py.None(), fut.clone_ref(py));
        assert!(ticket.is_some());
        let woken = wg.add(py, -2).unwrap();
        assert_eq!(woken, 1);
    });
}

#[test]
fn test_nativeworkerpool_shutdown_drain_is_drained() {
    Python::attach(|py| {
        let pool = NativeWorkerPool::new(2);
        let task1: Py<PyAny> = pyo3::types::PyInt::new(py, 1).into_any().unbind();
        let task2: Py<PyAny> = pyo3::types::PyInt::new(py, 2).into_any().unbind();
        pool.push_global(py, task1).unwrap();
        pool.push_local(0, task2, py).unwrap();

        assert!(!pool.is_drained());
        pool.close();
        assert!(pool.is_closed());
        assert!(!pool.is_drained());

        // Pop first task (from global queue batch pull)
        let popped1 = pool.pop_work(0, py).unwrap();
        assert!(popped1.is_some());

        // Pop second task (from worker 0's local queue)
        let popped2 = pool.pop_work(0, py).unwrap();
        assert!(popped2.is_some());

        assert!(pool.is_drained());
        // Once drained, pop_work must return ThreadPoolClosedError
        let result = pool.pop_work(0, py);
        assert!(result.is_err());
    });
}

#[test]
fn test_channel_bounded_full_and_fifo() {
    Python::attach(|py| {
        let ch = Bound::new(py, RawAsyncChannel::new(2)).unwrap();
        assert_eq!(ch.get().qsize(), 0);
        let item1: Py<PyAny> = pyo3::types::PyInt::new(py, 10).into_any().unbind();
        let item2: Py<PyAny> = pyo3::types::PyInt::new(py, 20).into_any().unbind();
        let item3: Py<PyAny> = pyo3::types::PyInt::new(py, 30).into_any().unbind();

        assert!(RawAsyncChannel::try_send(&ch, py, item1).unwrap());
        assert!(RawAsyncChannel::try_send(&ch, py, item2).unwrap());
        assert_eq!(ch.get().qsize(), 2);
        // Channel is full: try_send returns Ok(false)
        assert!(!RawAsyncChannel::try_send(&ch, py, item3).unwrap());

        // Receive item 1 (FIFO order: 10)
        let val1 = RawAsyncChannel::try_recv(&ch, py).unwrap();
        let num1: i32 = val1.extract(py).unwrap();
        assert_eq!(num1, 10);

        // Now space is available: send item 3
        let item3_new: Py<PyAny> = pyo3::types::PyInt::new(py, 30).into_any().unbind();
        assert!(RawAsyncChannel::try_send(&ch, py, item3_new).unwrap());

        // Receive item 2 (20)
        let val2 = RawAsyncChannel::try_recv(&ch, py).unwrap();
        let num2: i32 = val2.extract(py).unwrap();
        assert_eq!(num2, 20);

        // Receive item 3 (30)
        let val3 = RawAsyncChannel::try_recv(&ch, py).unwrap();
        let num3: i32 = val3.extract(py).unwrap();
        assert_eq!(num3, 30);

        // Channel now empty -> WouldBlock error
        assert!(RawAsyncChannel::try_recv(&ch, py).is_err());
    });
}

#[test]
fn test_channel_drain_after_close() {
    Python::attach(|py| {
        let ch = Bound::new(py, RawAsyncChannel::new(0)).unwrap();
        let item1: Py<PyAny> = pyo3::types::PyString::new(py, "first").into_any().unbind();
        let item2: Py<PyAny> = pyo3::types::PyString::new(py, "second").into_any().unbind();
        RawAsyncChannel::try_send(&ch, py, item1).unwrap();
        RawAsyncChannel::try_send(&ch, py, item2).unwrap();

        RawAsyncChannel::close(&ch, py).unwrap();
        assert!(ch.get().is_closed());
        assert!(!ch.get().is_drained());

        // After close, items remaining in the channel must drain cleanly
        let val1 = RawAsyncChannel::try_recv(&ch, py).unwrap();
        let s1: String = val1.extract(py).unwrap();
        assert_eq!(s1, "first");

        let val2 = RawAsyncChannel::try_recv(&ch, py).unwrap();
        let s2: String = val2.extract(py).unwrap();
        assert_eq!(s2, "second");
        assert!(ch.get().is_drained());

        // Once fully drained, try_recv on closed channel must return Err
        assert!(RawAsyncChannel::try_recv(&ch, py).is_err());
    });
}

#[test]
fn test_nativeworkerpool_local_unbounded_strict_affinity() {
    Python::attach(|py| {
        let pool = NativeWorkerPool::new(1);
        // Push tasks beyond 256 to worker 0 local channel (unbounded, strict affinity)
        for i in 0..256 {
            let task: Py<PyAny> = pyo3::types::PyInt::new(py, i).into_any().unbind();
            pool.push_local(0, task, py).unwrap();
        }
        // 257th push to local should succeed into unbounded local channel
        let task257: Py<PyAny> = pyo3::types::PyInt::new(py, 256).into_any().unbind();
        pool.push_local(0, task257, py).unwrap();

        // Verify all 257 tasks can be popped
        let mut popped_count = 0;
        for _ in 0..257 {
            let popped = pool.pop_work(0, py).unwrap();
            if popped.is_some() {
                popped_count += 1;
            }
        }
        assert_eq!(popped_count, 257);
    });
}

#[test]
fn test_nativeworkerpool_over_64_bitmap_and_drain_all() {
    Python::attach(|py| {
        let pool = NativeWorkerPool::new(130);
        pool.mark_worker_idle(0);
        pool.mark_worker_idle(68);
        pool.mark_worker_idle(129);
        assert_eq!(pool.claim_one_idle_worker(), Some(0));
        assert_eq!(pool.claim_one_idle_worker(), Some(68));
        pool.clear_worker_idle(129);
        assert_eq!(pool.claim_one_idle_worker(), None);

        let t1: Py<PyAny> = pyo3::types::PyInt::new(py, 1).into_any().unbind();
        let t2: Py<PyAny> = pyo3::types::PyInt::new(py, 2).into_any().unbind();
        let t3: Py<PyAny> = pyo3::types::PyInt::new(py, 3).into_any().unbind();
        pool.push_global(py, t1).unwrap();
        pool.push_local(120, t2, py).unwrap();
        assert_eq!(pool.push_global_batch(py, vec![t3]).unwrap(), 1);
        assert_eq!(pool.outstanding_count(), 3);
        assert!(!pool.wait_drained(py, 0.0).unwrap());

        let leftover = pool.drain_all(py);
        assert_eq!(leftover.len(), 3);
        assert!(pool.is_drained());
        for _ in 0..leftover.len() {
            pool.complete_task();
        }
        assert_eq!(pool.outstanding_count(), 0);
        assert!(pool.wait_drained(py, 0.01).unwrap());
    });
}

#[test]
fn test_atomicmetrics_concurrent_multiworker() {
    use std::sync::Arc;
    use std::thread;

    let num_workers = 4;
    let metrics = Arc::new(AtomicMetrics::new(num_workers));
    let iterations = 10_000;

    let mut handles = Vec::new();
    for worker_id in 0..num_workers {
        let m = Arc::clone(&metrics);
        handles.push(thread::spawn(move || {
            for _ in 0..iterations {
                m.inc_active(worker_id);
                m.inc_global_pull(worker_id);
                m.dec_active(worker_id);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    for worker_id in 0..num_workers {
        assert_eq!(metrics.get_active(worker_id), 0);
        assert_eq!(metrics.get_completed(worker_id), iterations);
        assert_eq!(metrics.get_global_pull(worker_id), iterations);
    }
}

#[test]
fn test_raw_async_channel_send_recv_basic() {
    Python::attach(|py| {
        let ch = Bound::new(py, RawAsyncChannel::new(0)).unwrap();
        assert_eq!(ch.get().qsize(), 0);
        assert!(ch.get().empty());
        assert!(!ch.get().full());
        assert!(!ch.get().is_closed());

        let item1: Py<PyAny> = pyo3::types::PyString::new(py, "hello").into_any().unbind();
        assert!(RawAsyncChannel::try_send(&ch, py, item1).unwrap());
        assert_eq!(ch.get().qsize(), 1);
        assert!(!ch.get().empty());

        let val = RawAsyncChannel::try_recv(&ch, py).unwrap();
        let s: String = val.extract(py).unwrap();
        assert_eq!(s, "hello");
        assert_eq!(ch.get().qsize(), 0);
        assert!(ch.get().empty());

        // Empty channel recv raises WouldBlock
        assert!(RawAsyncChannel::try_recv(&ch, py).is_err());
    });
}

#[test]
fn test_raw_async_channel_bounded_full_and_drain() {
    Python::attach(|py| {
        let ch = Bound::new(py, RawAsyncChannel::new(2)).unwrap();
        assert_eq!(ch.get().maxsize(), 2);
        let item1: Py<PyAny> = pyo3::types::PyInt::new(py, 10).into_any().unbind();
        let item2: Py<PyAny> = pyo3::types::PyInt::new(py, 20).into_any().unbind();
        let item3: Py<PyAny> = pyo3::types::PyInt::new(py, 30).into_any().unbind();

        assert!(RawAsyncChannel::try_send(&ch, py, item1).unwrap());
        assert!(RawAsyncChannel::try_send(&ch, py, item2).unwrap());
        assert!(ch.get().full());

        // Bounded full: try_send returns Ok(false)
        assert!(!RawAsyncChannel::try_send(&ch, py, item3).unwrap());

        // Drain first item
        let val1 = RawAsyncChannel::try_recv(&ch, py).unwrap();
        assert_eq!(val1.extract::<i32>(py).unwrap(), 10);
        assert!(!ch.get().full());

        // Now space available: send third item
        let item3_new: Py<PyAny> = pyo3::types::PyInt::new(py, 30).into_any().unbind();
        assert!(RawAsyncChannel::try_send(&ch, py, item3_new).unwrap());

        // Drain remaining
        let val2 = RawAsyncChannel::try_recv(&ch, py).unwrap();
        assert_eq!(val2.extract::<i32>(py).unwrap(), 20);
        let val3 = RawAsyncChannel::try_recv(&ch, py).unwrap();
        assert_eq!(val3.extract::<i32>(py).unwrap(), 30);
        assert!(ch.get().empty());
    });
}

#[test]
fn test_raw_async_channel_register_unregister_getter() {
    Python::attach(|py| {
        let ch = Bound::new(py, RawAsyncChannel::new(0)).unwrap();
        let loop_obj = py.None();
        let fut = py.None();

        // Register getter on empty channel
        let (ticket, item) = RawAsyncChannel::register_getter_ticket(
            &ch,
            py,
            loop_obj.clone_ref(py),
            fut.clone_ref(py),
        )
        .unwrap();
        assert!(ticket > 0);
        assert!(item.is_none());
        assert_eq!(ch.get().getters_len(), 1);

        // Cancel getter
        let removed = RawAsyncChannel::cancel_getter(&ch, py, ticket).unwrap();
        assert!(removed);
        assert_eq!(ch.get().getters_len(), 0);

        // Double-check fast path: if item is already present, register_getter_ticket returns it immediately with ticket 0
        let item_val: Py<PyAny> = pyo3::types::PyString::new(py, "fast").into_any().unbind();
        RawAsyncChannel::try_send(&ch, py, item_val).unwrap();

        let (ticket2, val2) = RawAsyncChannel::register_getter_ticket(
            &ch,
            py,
            loop_obj.clone_ref(py),
            fut.clone_ref(py),
        )
        .unwrap();
        assert_eq!(ticket2, 0);
        assert_eq!(val2.unwrap().extract::<String>(py).unwrap(), "fast");
        assert_eq!(ch.get().getters_len(), 0);
    });
}

#[test]
fn test_raw_async_channel_register_unregister_putter() {
    Python::attach(|py| {
        let ch = Bound::new(py, RawAsyncChannel::new(1)).unwrap();
        let item: Py<PyAny> = pyo3::types::PyInt::new(py, 1).into_any().unbind();
        RawAsyncChannel::try_send(&ch, py, item).unwrap();
        assert!(ch.get().full());

        let loop_obj = py.None();
        let fut = py.None();

        // Register putter on full channel via single-lock send_or_register
        let item2: Py<PyAny> = pyo3::types::PyInt::new(py, 2).into_any().unbind();
        let ticket = RawAsyncChannel::send_or_register(
            &ch,
            py,
            item2.clone_ref(py),
            loop_obj.clone_ref(py),
            fut.clone_ref(py),
        )
        .unwrap()
        .unwrap();
        assert!(ticket > 0);
        assert_eq!(ch.get().putters_len(), 1);

        // Cancel putter
        let removed = RawAsyncChannel::cancel_putter(&ch, py, ticket).unwrap();
        assert!(removed);
        assert_eq!(ch.get().putters_len(), 0);

        // Drain item so space is available
        let _ = RawAsyncChannel::try_recv(&ch, py).unwrap();

        // When space available, send_or_register completes immediately and returns None
        let ticket_now = RawAsyncChannel::send_or_register(&ch, py, item2, loop_obj, fut).unwrap();
        assert!(ticket_now.is_none());
        assert_eq!(ch.get().qsize(), 1);
    });
}

#[test]
fn test_raw_async_channel_select_watcher_and_forwarding() {
    Python::attach(|py| {
        let ch = Bound::new(py, RawAsyncChannel::new(0)).unwrap();
        let loop_obj = py.None();
        let fut = py.None();
        let globals = pyo3::types::PyDict::new(py);
        py.run(
            c"class DummyChannel: pass\nd = DummyChannel()",
            Some(&globals),
            None,
        )
        .unwrap();
        let dummy = globals.get_item("d").unwrap().unwrap();
        let ch_weak = pyo3::types::PyWeakrefReference::new(&dummy)
            .unwrap()
            .unbind();

        // Register watcher on empty channel
        let ticket = ch
            .get()
            .register_select_watcher_ticket(
                loop_obj.clone_ref(py),
                fut.clone_ref(py),
                ch_weak.clone_ref(py),
            )
            .unwrap();
        assert!(ticket > 0);
        assert_eq!(ch.get().notifiers_len(), 1);

        // Cancel watcher
        let removed = ch.get().cancel_select_watcher(ticket);
        assert!(removed);
        assert_eq!(ch.get().notifiers_len(), 0);

        // If channel has item, register_select_watcher_ticket returns 0 (caller probes immediately)
        let item: Py<PyAny> = pyo3::types::PyInt::new(py, 99).into_any().unbind();
        RawAsyncChannel::try_send(&ch, py, item).unwrap();
        let ticket2 = ch
            .get()
            .register_select_watcher_ticket(loop_obj, fut, ch_weak)
            .unwrap();
        assert_eq!(ticket2, 0);
    });
}

#[test]
fn test_raw_async_channel_close_wakes_all() {
    Python::attach(|py| {
        let ch = Bound::new(py, RawAsyncChannel::new(1)).unwrap();
        let loop_obj = py.None();
        let fut1 = py.None();
        let fut2 = py.None();

        // Fill channel
        let item: Py<PyAny> = pyo3::types::PyInt::new(py, 1).into_any().unbind();
        RawAsyncChannel::try_send(&ch, py, item).unwrap();

        // Queue a putter
        let item2: Py<PyAny> = pyo3::types::PyInt::new(py, 2).into_any().unbind();
        let t1 = RawAsyncChannel::send_or_register(
            &ch,
            py,
            item2.clone_ref(py),
            loop_obj.clone_ref(py),
            fut1,
        )
        .unwrap()
        .unwrap();
        assert!(t1 > 0);
        assert_eq!(ch.get().putters_len(), 1);

        RawAsyncChannel::close(&ch, py).unwrap();
        assert!(ch.get().is_closed());
        assert!(!ch.get().is_drained());
        assert_eq!(ch.get().putters_len(), 0);

        // Queue a select watcher on an empty channel and close it to verify watcher drain
        let globals = pyo3::types::PyDict::new(py);
        py.run(
            c"class DummyChannel: pass\nd = DummyChannel()",
            Some(&globals),
            None,
        )
        .unwrap();
        let dummy = globals.get_item("d").unwrap().unwrap();
        let ch_weak = pyo3::types::PyWeakrefReference::new(&dummy)
            .unwrap()
            .unbind();
        let empty_ch = Bound::new(py, RawAsyncChannel::new(1)).unwrap();
        let t2 = empty_ch
            .get()
            .register_select_watcher_ticket(loop_obj, fut2, ch_weak)
            .unwrap();
        assert!(t2 > 0);
        assert_eq!(empty_ch.get().notifiers_len(), 1);
        RawAsyncChannel::close(&empty_ch, py).unwrap();
        assert!(empty_ch.get().is_drained());
        assert_eq!(empty_ch.get().notifiers_len(), 0);

        // Send on closed channel raises error
        assert!(RawAsyncChannel::try_send(&ch, py, item2).is_err());

        // Drain buffered item
        let val = RawAsyncChannel::try_recv(&ch, py).unwrap();
        assert_eq!(val.extract::<i32>(py).unwrap(), 1);
        assert!(ch.get().is_drained());

        // After drain, recv on closed channel raises error
        assert!(RawAsyncChannel::try_recv(&ch, py).is_err());
    });
}

#[test]
fn test_raw_async_channel_occupied_and_closed_in_flight_requeue() {
    Python::attach(|py| {
        // 1. Bounded capacity conservation across in_flight_getters
        let ch = Bound::new(py, RawAsyncChannel::new(1)).unwrap();
        let (gid, _) =
            RawAsyncChannel::register_getter_ticket(&ch, py, py.None(), py.None()).unwrap();
        assert!(gid > 0);
        let item_a: Py<PyAny> = pyo3::types::PyString::new(py, "a").into_any().unbind();
        assert!(RawAsyncChannel::try_send(&ch, py, item_a).unwrap());
        // Slot is occupied by in_flight_getters[(gid, "a")]!
        assert!(ch.get().full());
        let item_b: Py<PyAny> = pyo3::types::PyString::new(py, "b").into_any().unbind();
        assert!(!RawAsyncChannel::try_send(&ch, py, item_b).unwrap());

        // 2. Closing preserves in_flight_getters and waiting getters until drained
        let (gid2, _) =
            RawAsyncChannel::register_getter_ticket(&ch, py, py.None(), py.None()).unwrap();
        assert!(gid2 > 0);
        RawAsyncChannel::close(&ch, py).unwrap();
        assert!(ch.get().is_closed());
        assert!(!ch.get().is_drained());
        // gid cancels -> "a" forwards FIFO to gid2 even though channel is closed
        assert!(RawAsyncChannel::cancel_getter(&ch, py, gid).unwrap());
        assert!(!ch.get().is_drained());
        let (claimed, reclaimed_item) = RawAsyncChannel::claim_get(&ch, py, gid2).unwrap();
        assert!(claimed);
        assert_eq!(reclaimed_item.unwrap().extract::<String>(py).unwrap(), "a");
        assert!(ch.get().is_drained());
    });
}

#[test]
fn test_raw_async_channel_concurrent_threads() {
    use std::sync::Arc;
    use std::thread;

    let ch = Python::attach(|py| Py::new(py, RawAsyncChannel::new(10)).unwrap());
    let total_items = 200;
    let num_producers = 4;
    let num_consumers = 4;
    let items_per_producer = total_items / num_producers;
    let items_per_consumer = total_items / num_consumers;

    let received = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    // Spawn producers
    for p in 0..num_producers {
        let channel = Python::attach(|py| ch.clone_ref(py));
        handles.push(thread::spawn(move || {
            for i in 0..items_per_producer {
                let val = (p * items_per_producer + i) as i64;
                loop {
                    let sent = Python::attach(|py| {
                        let py_val: Py<PyAny> =
                            pyo3::types::PyInt::new(py, val).into_any().unbind();
                        RawAsyncChannel::try_send(channel.bind(py), py, py_val).unwrap()
                    });
                    if sent {
                        break;
                    }
                    thread::yield_now();
                }
            }
        }));
    }

    // Spawn consumers concurrently
    for _ in 0..num_consumers {
        let channel = Python::attach(|py| ch.clone_ref(py));
        let rec = Arc::clone(&received);
        handles.push(thread::spawn(move || {
            for _ in 0..items_per_consumer {
                loop {
                    let rec_opt = Python::attach(|py| {
                        match RawAsyncChannel::try_recv(channel.bind(py), py) {
                            Ok(val) => Some(val.extract::<i64>(py).unwrap()),
                            Err(_) => None,
                        }
                    });
                    if let Some(num) = rec_opt {
                        rec.lock().push(num);
                        break;
                    }
                    thread::yield_now();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    Python::attach(|py| {
        assert_eq!(ch.bind(py).get().qsize(), 0);
    });
    let mut results = received.lock().clone();
    assert_eq!(results.len(), total_items);
    results.sort();
    let expected: Vec<i64> = (0..total_items as i64).collect();
    assert_eq!(results, expected);
}

#[test]
fn test_rwlock_basic_read_write_exclusion() {
    Python::attach(|py| {
        let lock = RawAsyncRWMutex::new();
        assert!(lock
            .acquire_read_ticket(1, py.None(), py.None())
            .unwrap()
            .is_none());
        assert!(lock
            .acquire_read_ticket(2, py.None(), py.None())
            .unwrap()
            .is_none());
        assert_eq!(lock.snapshot().0, 2);

        // Write attempt fails (queues) while readers are active
        let write_ticket = lock.acquire_write_ticket(3, py.None(), py.None()).unwrap();
        assert!(write_ticket.is_some());

        lock.release_read_task(py, 1).unwrap();
        lock.release_read_task(py, 2).unwrap();
        // Releasing all readers transfers ownership to writer 3
        assert_eq!(lock.snapshot().0, 0);
        assert!(lock.snapshot().1);

        // Read fails (queues) while writer active
        let read_ticket = lock.acquire_read_ticket(4, py.None(), py.None()).unwrap();
        assert!(read_ticket.is_some());

        lock.release_write_task(py, 3).unwrap();
        assert!(!lock.snapshot().1);
    });
}

#[test]
fn test_rwlock_cancellation_ownership_transfer() {
    Python::attach(|py| {
        let lock = RawAsyncRWMutex::new();
        // Initial writer
        assert!(lock
            .acquire_write_ticket(1, py.None(), py.None())
            .unwrap()
            .is_none());
        assert!(lock.snapshot().1);

        // Register writer 2 and writer 3
        let t2 = lock.acquire_write_ticket(2, py.None(), py.None()).unwrap();
        let t3 = lock.acquire_write_ticket(3, py.None(), py.None()).unwrap();
        assert!(t2.is_some());
        assert!(t3.is_some());
        assert_eq!(lock.snapshot().2, 2);

        // Release writer 1: wakes writer 2
        lock.release_write_task(py, 1).unwrap();
        assert!(lock.snapshot().1);
        assert_eq!(lock.snapshot().2, 1);

        // Cancel writer 2 (which had woken ownership):
        // Should detect ownership transfer and forward lock to writer 3!
        lock.cancel_write_ticket(py, 2, t2.unwrap());
        assert!(lock.snapshot().1); // handed to writer 3
        assert_eq!(lock.snapshot().2, 0);

        // Cancel writer 3: releases lock and leaves writer = false
        lock.cancel_write_ticket(py, 3, t3.unwrap());
        assert!(!lock.snapshot().1);

        // Now write lock can be acquired freshly
        assert!(lock
            .acquire_write_ticket(4, py.None(), py.None())
            .unwrap()
            .is_none());
        assert!(lock.snapshot().1);
    });
}

#[test]
fn test_lock_and_rwmutex_ticket_cancel_idempotency() {
    Python::attach(|py| {
        let lock = crate::sync::RawAsyncLock::new();
        let globals = pyo3::types::PyDict::new(py);
        py.run(
            c"class DummyTask: pass\nt = DummyTask()",
            Some(&globals),
            None,
        )
        .unwrap();
        let task_obj = globals.get_item("t").unwrap().unwrap();
        let task_weak = pyo3::types::PyWeakrefReference::new(&task_obj)
            .unwrap()
            .unbind();
        assert!(lock.try_acquire(42, 1, task_weak).unwrap());
        assert!(lock.locked(py));
        // Stale ticket_id must not release active lock held by task_id 42
        lock.cancel_waiter(py, 42, 999);
        assert!(lock.locked(py));
        lock.release(py, 42).unwrap();
        assert!(!lock.locked(py));

        let rw = RawAsyncRWMutex::new();
        assert!(rw.try_acquire_write_task(42).unwrap());
        assert!(rw.snapshot().1);
        rw.cancel_write_ticket(py, 42, 999);
        assert!(rw.snapshot().1);
        assert!(rw.release_write_task(py, 999).is_err());
        rw.release_write_task(py, 42).unwrap();
        assert!(!rw.snapshot().1);

        assert!(rw.try_acquire_read_task(42).unwrap());
        assert_eq!(rw.snapshot().0, 1);
        rw.cancel_read_ticket(py, 42, 999);
        assert_eq!(rw.snapshot().0, 1);
        assert!(rw.release_read_task(py, 999).is_err());
        rw.release_read_task(py, 42).unwrap();
        assert_eq!(rw.snapshot().0, 0);
    });
}

#[test]
fn test_cancel_state_stack_shield_and_deadlines() {
    Python::attach(|py| {
        let state = RawTaskCancelState::new();
        assert!(state.task_token() >= 1);
        assert!(!state.is_effectively_cancelled());
        assert_eq!(state.effective_deadline(), f64::INFINITY);

        let outer = state.push_scope(100.0, 0, false, false);
        assert_eq!(state.effective_deadline(), 100.0);
        assert!(!state.has_outer_cancellation(outer));

        let mid = state.push_scope(50.0, 0, false, false);
        assert_eq!(state.effective_deadline(), 50.0);

        // Shield blocks outer cancellation and outer deadlines
        let shield = state.push_scope(200.0, 0, true, false);
        assert_eq!(state.effective_deadline(), 200.0);

        assert!(!state.cancel_scope(py, outer));
        assert!(!state.is_effectively_cancelled());
        assert!(state.is_scope_effectively_cancelled(outer));
        assert!(state.is_scope_effectively_cancelled(mid));
        assert!(!state.is_scope_effectively_cancelled(shield));
        assert!(state.has_outer_cancellation(shield));

        // Cancelling the shielded scope itself is effective
        assert!(state.cancel_scope(py, shield));
        assert!(state.is_effectively_cancelled());
        assert!(state.is_scope_effectively_cancelled(shield));

        assert!(state.pop_scope(shield).unwrap());
        assert!(state.is_effectively_cancelled());
        assert_eq!(state.effective_deadline(), 50.0);
        assert!(state.has_outer_cancellation(mid));

        assert!(!state.pop_scope(mid).unwrap());
        assert!(state.pop_scope(outer).unwrap());
        assert!(!state.is_effectively_cancelled());
        assert_eq!(state.effective_deadline(), f64::INFINITY);
    });
}

#[test]
fn test_cancel_state_park_unpark_and_cross_thread_wakeup() {
    use std::sync::Arc;
    use std::thread;

    let state = Arc::new(RawTaskCancelState::new());
    let (outer, fut_obj) = Python::attach(|py| {
        let globals = pyo3::types::PyDict::new(py);
        py.run(
            c"class DummyFut:\n    def __init__(self):\n        self.cancelled = 0\n    def cancel(self):\n        self.cancelled += 1\n        return True\nf = DummyFut()",
            Some(&globals),
            None,
        )
        .unwrap();
        let fut = globals.get_item("f").unwrap().unwrap().unbind();
        let outer = state.push_scope(f64::INFINITY, 0, false, false);
        let park_id = state.park_waiter(py.None(), fut.clone_ref(py));
        assert!(park_id >= 1);
        (outer, fut)
    });

    // Trigger cancellation from a background OS thread
    let state_bg = Arc::clone(&state);
    thread::spawn(move || {
        Python::attach(|py| {
            assert!(state_bg.cancel_scope(py, outer));
        });
    })
    .join()
    .unwrap();

    Python::attach(|py| {
        let cancelled: i64 = fut_obj
            .getattr(py, "cancelled")
            .unwrap()
            .extract(py)
            .unwrap();
        assert_eq!(cancelled, 1);

        // Already cancelled: park_waiter returns 0 immediately
        let pid2 = state.park_waiter(py.None(), fut_obj.clone_ref(py));
        assert_eq!(pid2, 0);

        // Shield hides outer cancellation; lowering shield wakes parked waiter
        let shield = state.push_scope(f64::INFINITY, 0, true, false);
        let pid3 = state.park_waiter(py.None(), fut_obj.clone_ref(py));
        assert!(pid3 >= 1);
        state.set_shield(py, shield, false);
        let cancelled2: i64 = fut_obj
            .getattr(py, "cancelled")
            .unwrap()
            .extract(py)
            .unwrap();
        assert_eq!(cancelled2, 2);

        state.pop_scope(shield).unwrap();
        state.pop_scope(outer).unwrap();
    });
}

#[test]
fn test_cancel_state_out_of_lock_drop_reentrancy() {
    Python::attach(|py| {
        let state = Py::new(py, RawTaskCancelState::new()).unwrap();
        let globals = pyo3::types::PyDict::new(py);
        globals.set_item("state", state.bind(py)).unwrap();
        py.run(
            c"class ReentrantDrop:\n    def __init__(self, st):\n        self.st = st\n        self.reentered = False\n    def __del__(self):\n        _ = self.st.is_effectively_cancelled()\n        self.st.unpark_waiter(9999)\n        self.reentered = True\nobj = ReentrantDrop(state)",
            Some(&globals),
            None,
        )
        .unwrap();
        let obj = globals.get_item("obj").unwrap().unwrap().unbind();
        globals.del_item("obj").unwrap();

        let park_id = state.borrow(py).park_waiter(py.None(), obj);
        assert!(park_id >= 1);
        // Dropping ReentrantDrop inside unpark_waiter must occur strictly outside the mutex
        state.borrow(py).unpark_waiter(park_id);
        assert!(!state.borrow(py).is_effectively_cancelled());
    });
}

#[test]
fn test_timeout_helpers_edge_values() {
    use std::time::{Duration, Instant};

    Python::attach(|py| {
        // NaN and -inf must return PyValueError without panicking
        for bad in [f64::NAN, -f64::NAN, f64::NEG_INFINITY] {
            let dur_err = crate::timeout::timeout_to_duration(bad).unwrap_err();
            assert!(dur_err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
            let dl_err = crate::timeout::deadline_from_secs(bad).unwrap_err();
            assert!(dl_err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
            let opt_err = crate::timeout::deadline_from_opt_secs(Some(bad)).unwrap_err();
            assert!(opt_err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        }

        // +inf and None map to unbounded wait (Ok(None)) without panicking
        assert_eq!(
            crate::timeout::timeout_to_duration(f64::INFINITY).unwrap(),
            None
        );
        assert!(crate::timeout::deadline_from_secs(f64::INFINITY)
            .unwrap()
            .is_none());
        assert!(crate::timeout::deadline_from_opt_secs(Some(f64::INFINITY))
            .unwrap()
            .is_none());
        assert!(crate::timeout::deadline_from_opt_secs(None)
            .unwrap()
            .is_none());

        // Huge finite floats (1e20, f64::MAX) must be capped safely without panicking
        for huge in [1e20, f64::MAX] {
            let dur = crate::timeout::timeout_to_duration(huge)
                .unwrap()
                .expect("capped finite timeout should produce a bounded Duration");
            assert_eq!(
                dur,
                Duration::from_secs_f64(crate::timeout::MAX_FINITE_TIMEOUT_SECS)
            );
            let before = Instant::now();
            let dl = crate::timeout::deadline_from_secs(huge).unwrap();
            if let Some(deadline) = dl {
                assert!(deadline > before);
            }
        }

        // Finite <= 0.0 (-1.0, -0.0, 0.0) map to immediate zero duration / expired deadline
        for non_pos in [-1.0, -0.0, 0.0] {
            assert_eq!(
                crate::timeout::timeout_to_duration(non_pos).unwrap(),
                Some(Duration::ZERO)
            );
            let dl = crate::timeout::deadline_from_secs(non_pos)
                .unwrap()
                .expect("finite <= 0.0 should produce an immediate Instant deadline");
            assert!(dl <= Instant::now());
        }

        // Positive finite float (0.5) produces exact Duration and future deadline
        assert_eq!(
            crate::timeout::timeout_to_duration(0.5).unwrap(),
            Some(Duration::from_millis(500))
        );
        let before = Instant::now();
        let dl = crate::timeout::deadline_from_secs(0.5)
            .unwrap()
            .expect("positive finite timeout should produce a deadline");
        assert!(dl > before);
    });
}

#[test]
fn test_channel_sync_timeout_edge_cases() {
    use std::thread;
    use std::time::Duration;

    Python::attach(|py| {
        let ch = Bound::new(py, RawAsyncChannel::new(1)).unwrap();

        // NaN and -inf must return PyValueError (even if channel has space/data)
        for bad in [f64::NAN, f64::NEG_INFINITY] {
            let item: Py<PyAny> = pyo3::types::PyInt::new(py, 1).into_any().unbind();
            let send_err = RawAsyncChannel::send_sync(&ch, py, item, Some(bad)).unwrap_err();
            assert!(send_err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
            let recv_err = RawAsyncChannel::recv_sync(&ch, py, Some(bad)).unwrap_err();
            assert!(recv_err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        }

        // Non-positive finite timeouts (-1.0, 0.0) allow fast-path send/recv and time out immediately when blocked
        for non_pos in [-1.0, 0.0] {
            let item: Py<PyAny> = pyo3::types::PyInt::new(py, 7).into_any().unbind();
            RawAsyncChannel::send_sync(&ch, py, item, Some(non_pos)).unwrap();
            // Channel is now full: send_sync with <= 0.0 times out (TimeoutError, not ValueError)
            let extra: Py<PyAny> = pyo3::types::PyInt::new(py, 8).into_any().unbind();
            let full_err = RawAsyncChannel::send_sync(&ch, py, extra, Some(non_pos)).unwrap_err();
            assert!(!full_err.is_instance_of::<pyo3::exceptions::PyValueError>(py));

            // Drain via recv_sync with <= 0.0 succeeds
            let val = RawAsyncChannel::recv_sync(&ch, py, Some(non_pos)).unwrap();
            assert_eq!(val.extract::<i32>(py).unwrap(), 7);
            // Channel is now empty: recv_sync with <= 0.0 times out
            let empty_err = RawAsyncChannel::recv_sync(&ch, py, Some(non_pos)).unwrap_err();
            assert!(!empty_err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        }

        // +inf and 1e20 fast-path and blocking wakeup without panic
        let item_inf: Py<PyAny> = pyo3::types::PyInt::new(py, 11).into_any().unbind();
        RawAsyncChannel::send_sync(&ch, py, item_inf, Some(f64::INFINITY)).unwrap();
        let ch_bg = ch.clone().unbind();
        let bg_recv = thread::spawn(move || {
            thread::sleep(Duration::from_millis(15));
            Python::attach(|py| {
                let v = RawAsyncChannel::try_recv(ch_bg.bind(py), py).unwrap();
                assert_eq!(v.extract::<i32>(py).unwrap(), 11);
            });
        });
        let item_wait: Py<PyAny> = pyo3::types::PyInt::new(py, 22).into_any().unbind();
        RawAsyncChannel::send_sync(&ch, py, item_wait, Some(f64::INFINITY)).unwrap();
        py.detach(|| bg_recv.join()).unwrap();

        let val_1e20 = RawAsyncChannel::recv_sync(&ch, py, Some(1e20)).unwrap();
        assert_eq!(val_1e20.extract::<i32>(py).unwrap(), 22);

        let ch_bg2 = ch.clone().unbind();
        let bg_send = thread::spawn(move || {
            thread::sleep(Duration::from_millis(15));
            Python::attach(|py| {
                let v: Py<PyAny> = pyo3::types::PyInt::new(py, 33).into_any().unbind();
                assert!(RawAsyncChannel::try_send(ch_bg2.bind(py), py, v).unwrap());
            });
        });
        let got = RawAsyncChannel::recv_sync(&ch, py, Some(1e20)).unwrap();
        assert_eq!(got.extract::<i32>(py).unwrap(), 33);
        py.detach(|| bg_send.join()).unwrap();

        // Finite positive timeout (0.5) succeeds when ready
        let item_ok: Py<PyAny> = pyo3::types::PyInt::new(py, 44).into_any().unbind();
        RawAsyncChannel::send_sync(&ch, py, item_ok, Some(0.5)).unwrap();
        let got_ok = RawAsyncChannel::recv_sync(&ch, py, Some(0.5)).unwrap();
        assert_eq!(got_ok.extract::<i32>(py).unwrap(), 44);
    });
}

#[test]
fn test_nativeworkerpool_wait_drained_timeout_edges() {
    use std::thread;
    use std::time::Duration;

    Python::attach(|py| {
        let pool = Py::new(py, NativeWorkerPool::new(2)).unwrap();
        let p = pool.bind(py).get();

        // Already drained: NaN and -inf still raise ValueError
        for bad in [f64::NAN, f64::NEG_INFINITY] {
            let err = p.wait_drained(py, bad).unwrap_err();
            assert!(err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        }

        // Already drained: <= 0.0, +inf, 1e20, 0.5 all return Ok(true)
        for ok_val in [-1.0, 0.0, 0.5, f64::INFINITY, 1e20] {
            assert!(p.wait_drained(py, ok_val).unwrap());
        }

        // Push a task so pool is not drained
        let t1: Py<PyAny> = pyo3::types::PyInt::new(py, 1).into_any().unbind();
        p.push_global(py, t1).unwrap();

        // Non-drained: NaN/-inf -> Err(ValueError); -1.0/0.0 -> Ok(false)
        for bad in [f64::NAN, f64::NEG_INFINITY] {
            let err = p.wait_drained(py, bad).unwrap_err();
            assert!(err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        }
        assert!(!p.wait_drained(py, -1.0).unwrap());
        assert!(!p.wait_drained(py, 0.0).unwrap());

        // +inf and 1e20 wait safely until background thread drains and completes the task
        let pool_bg = pool.clone_ref(py);
        let bg = thread::spawn(move || {
            thread::sleep(Duration::from_millis(15));
            Python::attach(|py| {
                let pb = pool_bg.bind(py).get();
                let items = pb.drain_all(py);
                assert_eq!(items.len(), 1);
                pb.complete_task();
            });
        });
        assert!(p.wait_drained(py, f64::INFINITY).unwrap());
        py.detach(|| bg.join()).unwrap();

        let t2: Py<PyAny> = pyo3::types::PyInt::new(py, 2).into_any().unbind();
        p.push_global(py, t2).unwrap();
        let pool_bg2 = pool.clone_ref(py);
        let bg2 = thread::spawn(move || {
            thread::sleep(Duration::from_millis(15));
            Python::attach(|py| {
                let pb = pool_bg2.bind(py).get();
                let items = pb.drain_all(py);
                assert_eq!(items.len(), 1);
                pb.complete_task();
            });
        });
        assert!(p.wait_drained(py, 1e20).unwrap());
        py.detach(|| bg2.join()).unwrap();
    });
}

#[test]
fn test_nativeworkerpool_complete_task_underflow_protection() {
    use std::thread;
    use std::time::Duration;

    Python::attach(|py| {
        let pool = Py::new(py, NativeWorkerPool::new(2)).unwrap();
        let p = pool.bind(py).get();

        // Extra complete_task() calls when already 0 must NOT panic or wrap to usize::MAX
        assert_eq!(p.outstanding_count(), 0);
        p.complete_task();
        p.complete_task();
        p.complete_task();
        assert_eq!(p.outstanding_count(), 0);
        assert!(p.wait_drained(py, 0.0).unwrap());

        // Transition 1 -> 0 wakes wait_drained condvar, and subsequent extra calls stay at 0
        let t: Py<PyAny> = pyo3::types::PyInt::new(py, 99).into_any().unbind();
        p.push_global(py, t).unwrap();
        assert_eq!(p.outstanding_count(), 1);

        let pool_waiter = pool.clone_ref(py);
        let waiter = thread::spawn(move || {
            Python::attach(|py| pool_waiter.bind(py).get().wait_drained(py, 2.0).unwrap())
        });
        thread::sleep(Duration::from_millis(15));
        let drained = p.drain_all(py);
        assert_eq!(drained.len(), 1);
        p.complete_task();
        assert!(py.detach(|| waiter.join()).unwrap());
        assert_eq!(p.outstanding_count(), 0);

        p.complete_task();
        assert_eq!(p.outstanding_count(), 0);
    });
}

#[test]
fn test_channel_gc_traverse_clear_and_reentrancy() {
    let _gc_guard = GC_TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    Python::attach(|py| {
        // 1. Real cyclic GC collection across buffer, getters, putters, select_watchers, and in_flight_getters
        let globals = pyo3::types::PyDict::new(py);
        {
            let ch_buf = Py::new(py, RawAsyncChannel::new(4)).unwrap();
            let ch_wait = Py::new(py, RawAsyncChannel::new(1)).unwrap();
            let ch_sel = Py::new(py, RawAsyncChannel::new(1)).unwrap();
            let ch_inf = Py::new(py, RawAsyncChannel::new(1)).unwrap();
            globals.set_item("ch_buf", ch_buf.bind(py)).unwrap();
            globals.set_item("ch_wait", ch_wait.bind(py)).unwrap();
            globals.set_item("ch_sel", ch_sel.bind(py)).unwrap();
            globals.set_item("ch_inf", ch_inf.bind(py)).unwrap();
        }
        py.run(
            c"import gc, weakref

class CycleNode:
    def call_soon_threadsafe(self, *args):
        pass

assert gc.is_tracked(ch_buf)
assert gc.is_tracked(ch_wait)
assert gc.is_tracked(ch_sel)
assert gc.is_tracked(ch_inf)

n_buf = CycleNode()
n_buf.ch = ch_buf
ref_buf = weakref.ref(n_buf)
ch_buf.try_send([n_buf])

n_wait = CycleNode()
n_wait.ch = ch_wait
ref_wait = weakref.ref(n_wait)
ch_wait.try_send(1)
ch_wait.send_or_register(2, n_wait, n_wait)

n_get = CycleNode()
n_get.ch = ch_sel
ref_get = weakref.ref(n_get)
n_sel = CycleNode()
n_sel.ch = ch_sel
ref_sel = weakref.ref(n_sel)
ch_sel.register_getter_ticket(n_get, n_get)
ch_sel.register_select_watcher_ticket(n_sel, n_sel, weakref.ref(n_sel))

n_inf_waiter = CycleNode()
n_inf_waiter.ch = ch_inf
n_inf_payload = CycleNode()
n_inf_payload.ch = ch_inf
ref_inf = weakref.ref(n_inf_payload)
ch_inf.register_getter_ticket(n_inf_waiter, n_inf_waiter)
ch_inf.try_send(n_inf_payload)

del n_buf, n_wait, n_get, n_sel, n_inf_waiter, n_inf_payload
del ch_buf, ch_wait, ch_sel, ch_inf
gc.collect()
gc.collect()
survived = [
    name
    for name, r in (
        ('buf', ref_buf()),
        ('wait', ref_wait()),
        ('get', ref_get()),
        ('sel', ref_sel()),
        ('inf', ref_inf()),
    )
    if r is not None
]
",
            Some(&globals),
            None,
        )
        .unwrap();
        let survived: Vec<String> = globals
            .get_item("survived")
            .unwrap()
            .unwrap()
            .extract()
            .unwrap();
        assert!(
            survived.is_empty(),
            "cyclic RawAsyncChannel instances must be collected by gc.collect(), survived={survived:?}"
        );

        // 2. Direct __clear__() drops payloads strictly outside the mutex (re-entrant __del__ safe)
        let ch = Py::new(py, RawAsyncChannel::new(2)).unwrap();
        let globals2 = pyo3::types::PyDict::new(py);
        globals2.set_item("ch", ch.bind(py)).unwrap();
        py.run(
            c"reentered = []
class ReentrantPayload:
    def __init__(self, c):
        self.c = c
    def __del__(self):
        reentered.append((self.c.qsize(), self.c.is_closed(), self.c.empty()))
ch.try_send(ReentrantPayload(ch))
",
            Some(&globals2),
            None,
        )
        .unwrap();
        ch.bind(py).get().__clear__();
        let reentered_len: usize = globals2
            .get_item("reentered")
            .unwrap()
            .unwrap()
            .len()
            .unwrap();
        assert_eq!(reentered_len, 1);
        assert!(ch.bind(py).get().is_closed());
        assert_eq!(ch.bind(py).get().qsize(), 0);
    });
}

#[test]
fn test_nativeworkerpool_gc_traverse_clear_and_reentrancy() {
    let _gc_guard = GC_TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    Python::attach(|py| {
        // 1. Real cyclic GC collection across worker buffers and metrics
        let globals = pyo3::types::PyDict::new(py);
        {
            let pool = Py::new(py, NativeWorkerPool::new(2)).unwrap();
            let metrics = Py::new(py, AtomicMetrics::new(2)).unwrap();
            globals.set_item("pool", pool.bind(py)).unwrap();
            globals.set_item("metrics", metrics.bind(py)).unwrap();
        }
        py.run(
            c"import gc, weakref

class CycleNode:
    pass

assert gc.is_tracked(pool)

pool.set_metrics(metrics)
refs = gc.get_referents(pool)
metrics_visited = metrics in refs
del refs, metrics

n_first = CycleNode()
n_first.pool = pool
ref_first = weakref.ref(n_first)

n_buf = CycleNode()
n_buf.pool = pool
ref_buf = weakref.ref(n_buf)

pool.push_global_batch([[n_first], [n_buf]])
# pop_work(0) batch-pulls both items into worker 0's buffer and pops one,
# leaving [n_buf] inside buffers[0] forming pool -> buffers[0] -> [n_buf] -> pool
first = pool.pop_work(0)
del first, n_first, n_buf, pool
gc.collect()
gc.collect()
pool_collected = metrics_visited and ref_first() is None and ref_buf() is None
",
            Some(&globals),
            None,
        )
        .unwrap();
        let pool_collected: bool = globals
            .get_item("pool_collected")
            .unwrap()
            .unwrap()
            .extract()
            .unwrap();
        assert!(
            pool_collected,
            "cyclic NativeWorkerPool with item in worker buffer must be collected by gc.collect()"
        );

        // 2. Direct __clear__() drains buffers + global/local flume receivers + metrics outside locks
        let pool = Py::new(py, NativeWorkerPool::new(2)).unwrap();
        let metrics = Py::new(py, AtomicMetrics::new(2)).unwrap();
        pool.bind(py).get().set_metrics(metrics);
        let globals2 = pyo3::types::PyDict::new(py);
        globals2.set_item("pool", pool.bind(py)).unwrap();
        py.run(
            c"reentered = []
class ReentrantWork:
    def __init__(self, p, tag):
        self.p = p
        self.tag = tag
    def __del__(self):
        reentered.append((self.tag, self.p.is_closed(), self.p.is_drained(), self.p.outstanding_count()))
pool.push_global_batch([ReentrantWork(pool, 'buf0'), ReentrantWork(pool, 'buf1')])
popped = pool.pop_work(0)
assert popped.tag == 'buf0'
del popped
pool.push_global(ReentrantWork(pool, 'global'))
pool.push_local(1, ReentrantWork(pool, 'local'))
assert len(reentered) == 1
",
            Some(&globals2),
            None,
        )
        .unwrap();
        pool.bind(py).get().__clear__();
        let reentered_len: usize = globals2
            .get_item("reentered")
            .unwrap()
            .unwrap()
            .len()
            .unwrap();
        assert_eq!(reentered_len, 4);
        assert!(pool.bind(py).get().is_closed());
        assert!(pool.bind(py).get().is_drained());
    });
}
