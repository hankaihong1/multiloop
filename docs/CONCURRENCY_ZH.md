# multiloop 并发正确性指南（Concurrency Correctness Guide）
**[English](CONCURRENCY.md)**


> **修改任何并发相关代码前必读**。本指南面向两类读者：AI 代理（写代码前
> 逐条过第 3 节检查清单）与人类贡献者（理解每个原语的并发模型再动手）。
> 每个论断都标注了源码位置，可跳转验证。

---

## 1. 并发模型总览（谁锁什么、谁等什么）

multiloop 的同步原语遵循同一个骨架：**Rust 核心（`_multiloop_core`）在
`parking_lot::Mutex` 下 100% 独占跨线程共享可变状态，Python 侧仅作为极薄异步门面**。
所有等待者通过单调递增的 `WaiterId (u64)` 票据与不透明 `(loop_obj, fut)` 句柄注册（`src/waiter.rs`），
跨线程唤醒一律在**释放 Rust 互斥锁之后**通过 `loop.call_soon_threadsafe(...)` 派发，并在目标事件循环已关闭时自动执行令牌回滚或向后转发。

### 1.0 并发物理假设公理体系（Physical Concurrency Axioms）

multiloop 运行在 **Python 3.14t（自由线程 / 无 GIL）多核物理真并行**之上的多 OS 线程与独立事件循环中。并发正确性不能依赖单线程启发式假设，必须建立在以下六项严密的形式化物理并发公理之上：

1. **Python 3.14t 自由线程（无 GIL）真并行假设**：字节码与对象读写在多核 CPU 上真并行执行，无全局解释器锁保护。任何涉及跨线程复合状态的操作（如计数器增减 + 等待者队列出入队）收敛在单一 Rust `parking_lot::Mutex` 临界区内，杜绝复合锁竞争撕裂与双轨制脑裂。
2. **跨线程 EventLoop 物理隔离与不透明句柄公理**：`asyncio.Task`、`Future`、`Event` 与 `TimerHandle` 严格物理绑定于单一 OS 线程及其 EventLoop。Rust 核心绝不跨线程调用 `.done()`，也绝不在持有 `parking_lot::Mutex` 期间调用任何 Python 方法；跨线程唤醒一律在锁外通过 `loop.call_soon_threadsafe(...)` 派发。
3. **ContextVar 线程/任务局部性公理**：`contextvars.ContextVar` 严格归属于当前 OS 线程与当前 Task。跨线程取消（`scope.cancel()`）严禁跨线程读取 ContextVar 栈，仅依赖线程互斥锁与 `call_soon_threadsafe`。
4. **纯 Rust 任务取消栈（`RawTaskCancelState`）公理**：`CancelScope`、`fail_after`/`fail_at`、`move_on_after`/`move_on_at`、`sleep`、`wait_cancellable` 与 `checkpoint` 在每任务的 Rust `RawTaskCancelState` 栈上运行（直接存储在任务的 `_multiloop_cancel_state` 属性上，仅对只含 `__slots__` 的任务代理由 `_FALLBACK_STATES` `WeakKeyDictionary` 兜底），**全程绝不调用 `task.cancel()`、`task.uncancel()` 或读写修改 `task.cancelling()`**。等待点经 `_await_cancellable`（`park_waiter` / `unpark_waiter`）订阅取消，外部协程经 `wait_cancellable()` 桥接，`shield=True` 屏蔽外层 `CancelScope` 截止时间与取消，`__aexit__` 严格执行三模式退出规则（`PLAIN` 抛 `CancelledError`、`MOVE_ON` 静默吸收并置 `cancelled_caught=True`、`FAIL_AFTER` 转换为 `TimeoutError`）。
5. **统一票据等待者协议与防插队 FIFO 公理**：所有 Rust 同步原语统一分配单调递增的 `WaiterId (u64)` 票据（`src/waiter.rs`），严禁使用裸指针地址（`fut.as_ptr()`）作身份标识，彻底根除内存地址复用引发的 ABA 误删；通道（`RawAsyncChannel`）通过 `in_flight_putters` 预留名额机制严格维护 FIFO 防插队不变量与令牌守恒。
6. **结构化并发物理作用域公理**：`TaskGroup` 的宿主上下文及派生调用（`start_soon` / `start`）物理绑定于宿主 `asyncio.AbstractEventLoop`，并关联宿主 `CancelScope`，当子任务异常崩溃或调用 `cancel_all()` 时立即中断阻塞在 `async with TaskGroup()` 块内的宿主协程。传入 `TaskGroup(pool=pool)` 时，子任务通过独立子 `CancelScope` 分发至 `EventLoopThreadPool` 各工作线程 Loop 执行，并在退出时经屏蔽栅栏安全排空；跨 Loop 消息传递与级联取消同时由 `Channel` 与 `AsyncContext` 协同编排。

### 1.1 通道类（Channel）

| 组件 | 锁/原语 | waiter 结构 | 关键不变量 |
|---|---|---|---|
| Rust `RawAsyncChannel` | `parking_lot::Mutex<ChannelState>` | `getters` / `putters` / `select_watchers` `VecDeque<WaiterEntry>` | 基于 `WaiterId (u64)` 票据的 FIFO 防插队：当存在排队 putter 或缓冲满时 `try_send` 严格拒绝；唤醒的 putter 通过 `in_flight_putters` 持有预留名额直到 `claim_put`。锁外通过 `call_soon_threadsafe` 安全唤醒并在目标 Loop 已关闭时自动回滚转发（`src/channel.rs`） |
| Python `Channel` | `RawAsyncChannel` 的极薄门面 | — | 直接透传 Rust 核心，无双轨制分叉；支持 `select_channel` 单仲裁器注册与防插队重试（`src/multiloop/primitives.py`） |
| 唤醒协议 | — | — | waiter 在锁内从 `VecDeque` 头部 FIFO 弹出，并在锁外通过 `wake_waiter_outside_lock` 调度回归属 Loop。被唤醒但在申领前取消（`cancel_putter` / `cancel_getter`）或目标 Loop 已关闭的 waiter 负责令牌向后转发以保证活性（`src/waiter.rs`, `src/channel.rs`） |

**全收敛单轨架构**：缓冲数据与异步 waiter 统一由 Rust `RawAsyncChannel` 核心管理。`send`/`recv` 均遵循
「快路径尝试 → 锁内尝试入队或注册 future 并获取 `WaiterId`（`send_or_register` / `register_getter_ticket`）→ await → 取消时按 `WaiterId` 锁内取消（`cancel_putter` / `cancel_getter`）→ 消费预留名额入队/出队（`claim_put` / `claim_get`）」。

### 1.2 锁与信号量

| 原语 | 保护锁 | waiter 结构 | 关键不变量 |
|---|---|---|---|
| `Lock` | Rust：`parking_lot::Mutex<LockInner>`（`RawAsyncLock`） | `VecDeque<LockWaiter>`（`WaiterId` 票据） | 严格 FIFO；非阻塞 `try_acquire()` + `acquire_or_register() -> Option<WaiterId>` + `cancel_waiter(task_id, ticket_id)`（已获授所有权时向后转发）+ 同 Loop 死亡持有者回收（`check_owner_same_loop` / `on_owner_task_done`）；`release()` 锁内弹出下一个 waiter 并在锁外唤醒，若目标 Loop 已关闭则自动回滚并向后转发所有权（`src/sync.rs`, `_sync.py`） |
| `AsyncRWMutex` | Rust：`parking_lot::Mutex<RWMutexInner>`（`RawAsyncRWMutex`） | `read_waiters` / `write_waiters` `VecDeque<RWMutexWaiter>`（`WaiterId` 票据）+ `woken_readers` | 写者优先（`pending_writers` 阻断新读者）与同任务读者可重入（`reader_depths`）；提供 `acquire_read_ticket()` / `cancel_read_ticket()` / `release_read_task()` 与 `acquire_write_ticket()` / `cancel_write_ticket()` / `release_write_task()`，锁外唤醒并支持关闭 Loop 自动回滚（`src/rwlock.rs`, `rwlock.py`） |
| `Semaphore` | Rust：`parking_lot::Mutex<SemaphoreInner>`（`RawAsyncSemaphore`） | `VecDeque<WaiterEntry>`（`WaiterId` 票据）+ `granted_tickets` | 严格 FIFO；`acquire_or_register()` + `claim_acquire(ticket_id)` + `cancel_waiter(ticket_id)`；取消的 waiter 若已被 `release()` 授予许可，`cancel_waiter(ticket_id)` 会将许可**向后转发**给下一个 waiter 或归还池（`src/sync.rs`, `_sync.py`） |
| `CapacityLimiter` | Rust：`parking_lot::Mutex<CapacityLimiterInner>`（`RawAsyncCapacityLimiter`） | `VecDeque<WaiterEntry>`（`WaiterId` 票据）+ `granted_tickets` | Rust 单锁原子模型：`acquire_or_register()`、`claim_acquire()`、`cancel_waiter()` 与 `snapshot()`（`available == total_tokens - borrowed`，含动态缩容亏空吸收）在 `parking_lot::Mutex` 下原子维护；锁外唤醒并支持关闭 Loop 自动回滚转发（`src/sync.rs`, `_sync.py`） |

### 1.3 事件与条件

| 原语 | 保护锁 | waiter 结构 | 关键不变量 |
|---|---|---|---|
| `Event` | Rust：`parking_lot::Mutex<EventInner>`（`RawAsyncEvent`） | `Vec<WaiterEntry>`（`WaiterId` 票据） | **sticky**（单向触发，不可逆，无 clear）；`wait_or_register()` + `cancel_waiter()`；`set()` 锁内取出全部列表、锁外逐个 `call_soon_threadsafe` 唤醒（`src/sync.rs`, `_sync.py`） |
| `Condition` | Rust：`parking_lot::Mutex<ConditionInner>`（`RawAsyncCondition`）+ 底层 `Lock` | `VecDeque<WaiterEntry>`（`WaiterId` 票据）+ `notified_tickets` | `notify()` **不需要**持底层锁；`wait()` 先通过 `register_waiter()` 注册 `WaiterId` 票据再释放底层 `Lock` → 等通知（取消调 `cancel_or_forward()`，唤醒调 `claim_waiter()`）→ **通过 `_reacquire_lock()` 吸收取消重试循环重获 `Lock`，若重获期间被取消则调 `forward_notify()` 向后转发通知**（`src/sync.rs`, `_sync.py`） |

### 1.4 屏障与组同步

| 原语 | 保护锁 | waiter 结构 | 关键不变量 |
|---|---|---|---|
| `Barrier` | Rust：`parking_lot::Mutex<BarrierInner>`（`RawAsyncBarrier`） | `VecDeque<BarrierWaiter>`（`WaiterId` 票据） | `generation` 计数器通过 `arrive_or_register()` 绑定轮次；轮次完成前有任意 party 被取消，`cancel_waiter(generation, ticket_id)` 自动将屏障置为 Broken 状态并在锁外唤醒其余所有 party 抛出异常，同时自增 `generation`（`src/sync.rs`, `_sync.py`） |
| `AsyncWaitGroup` | Rust：`parking_lot::Mutex<WaitGroupInner>`（`RawAsyncWaitGroup`） | `Vec<WaiterEntry>`（`WaiterId` 票据） | 单互斥锁统一管理 `next_waiter_id`、`counter` 和 `waiters`（单调递增 `WaiterId` 票据防 ABA，无需 `generation` 字段）；`add(-n)` 与 `done()` 归零时原子排空 `waiters` 列表并在锁外经 `wake_waiter_outside_lock` 唤醒（`src/waitgroup.rs`, `src/waiter.rs`, `primitives.py`）；`register_waiter_ticket()` 返回 `Option<WaiterId>`（归零时为 `None`），取消时由 `cancel_waiter_ticket(ticket_id)` 移除 |
| `AsyncOnce` | Rust：`parking_lot::Mutex<OnceInner>`（`RawAsyncOnce`） | `Vec<WaiterEntry>`（`WaiterId` 票据） | Rust 单一四状态机（`done` / `executing` / `result` / `exc`）：`enter_or_register()` 原子选举唯一 leader（`status = 0`）、为 follower 分配 `WaiterId`（`status = 1`，取消调 `cancel_waiter()`）或直接返回缓存结果（`status = 2` / `3`）；`complete(result, exc)` 更新状态并在锁外唤醒全部 follower，follower 通过 `outcome()` 读取结果（`src/sync.rs`, `primitives.py`） |

### 1.5 取消与结构化并发

| 组件 | 机制 | 关键点 |
|---|---|---|
| `CancelScope` | 纯 Rust `RawTaskCancelState` 栈（任务直接属性 `_multiloop_cancel_state`，仅只含 `__slots__` 的任务代理走 `_FALLBACK_STATES` `WeakKeyDictionary`）+ `_await_cancellable`（`park_waiter` / `unpark_waiter`） | 零修改 CPython `task.cancel()` / `uncancel()` / `cancelling()`；`shield=True` 屏蔽外层 `CancelScope` 并保持外部 CPython `task.cancelling()` 原封不动；严格三模式退出（`PLAIN` / `MOVE_ON` / `FAIL_AFTER`）；`sleep()` 与 `wait_cancellable()` 提供可取消挂起（`_cancel.py`, `src/cancel.rs`） |
| `TaskGroup` | `_TGState` 枚举（`IDLE`..`FAILED`）+ `_CancelReason` 枚举 + `_children_lock`（`threading.Lock`）+ 每子任务独立 `CancelScope`（本地与池统一）+ `_wake_fut`（`_await_cancellable`） | 宿主上下文与派生调用绑定于 `self._loop`；本地与 `pool=pool` 子任务统一通过子 `CancelScope` 级联取消；`_wait_children` 在 `CancelScope(shield=True)` 内排空并保留外部 CPython `host_task.cancelling()`（`_taskgroup.py`） |
| `select_channel` | 两阶段仲裁器（Phase 1 轮询起始索引快速 `try_recv`，Phase 2 基于 `WaiterId` 票据与 `weakref.ref(ch)` 的 `register_select_watcher_ticket`） | 通过 `_await_cancellable(loop, arbiter_fut)` 零引用环报告就绪；赢家通过 `try_recv()` 消费并在 `finally` 中调用 `cancel_select_watcher` 切除所有 `WaiterId` watcher 票据（`primitives.py`, `src/channel.rs`） |

### 1.6 多核调度与线程池并发模型（Multi-Core Scheduling & Pool Concurrency Model）

| 组件 | 架构职责 | 线程 / 状态保护 | 核心不变式 |
|---|---|---|---|
| Rust `NativeWorkerPool` | 无锁任务队列与工作窃取 | Flume 全局队列 + 各 Worker 专用通道 + `Mutex<VecDeque>` 私有缓冲 + `Box<[AtomicU64]>` 位图 + `outstanding_tasks` 排空条件变量 | 软 Poller 门禁（`num_polling < max(workers/2, 1)`）限制并发探测；>64 核 CAS 空闲位图；四级消费漏斗配合 `wait_drained` / `drain_all` 保障零任务饥饿与关池彻底排空（`src/pool.rs`）。 |
| Python `EventLoopThreadPool` | 多事件循环协调器 | 不可变快照（`_loops_snapshot` / `_notify_events_snapshot`）+ `threading.Lock`（`_lifecycle_lock`）+ `_PoolWorkItem`（`weakref.ref(pool)`） | 提交热路径完全去锁（`itertools.count()` + 原子指标与 Rust 队列）；各 Worker 本地 `active_items` 配合 `drain_all()` 清理；`submit_many` 拒绝多目标传入 `cancel_scope`（抛 `ValueError`）（`src/multiloop/pool.py`）。 |
| Python `AsyncContext` | 跨线程级联取消树 | `threading.Lock` + Loop 安全的 `CancelScope`（`_scopes`）/ 子上下文 / `WeakSet[TaskGroup]` 派发 | 跨线程级联取消子 `AsyncContext`、关联 `TaskGroup`（`_cancel_from_context()`）与活动 Worker 的 `CancelScope`（`src/multiloop/context.py`）。 |

---

## 2. 已知陷阱模式（Race-Condition Trap Patterns）

每个模式：症状 → 根因 → 正确姿势 → 源码例证。改代码时对照这里，改完
逐条过第 3 节清单。

### 模式 1：check-then-act 窗口

**症状**：先检查后操作的两步之间被并发修改，操作失败或读到脏状态。
**根因**：检查与操作不是原子的。
**正确姿势**：合并为单次锁内操作；无法合并时用 try/except 兜底。
**例证**：
- `CapacityLimiter`（`RawAsyncCapacityLimiter`）统一由单一把 `parking_lot::Mutex` 保护全部 token、借出与 waiter 状态；调用 `snapshot()` 原子上锁读出 `(total, avail, borrowed)`（`src/sync.rs`, `_sync.py`）。
- 通用教训（他处修复过）：`contains_key → getitem` 之间 key 可被并发删除
  → 拆开两步 + `try/except KeyError`。

### 模式 2：double-checked lock 的 lost-wakeup

**症状**：发送方成功入队后唤醒一个"已经不存在"的接收者，或接收方注册
future 时数据已经被取走，future 永远没人 resolve → 挂死。
**根因**：非阻塞快路径（`try_send`/`try_recv` 或 `is_set()`/`is_zero()`）与加锁的 waiter 注册是两步独立操作；当并发状态跃迁在快路径与注册之间胜出时，唤醒信号丢失。
**正确姿势**：快路径尝试后，**在 `parking_lot::Mutex` 锁内原子二次校验**后再注册 future；注册与
唤醒目标提取都在同一把 `parking_lot::Mutex` 下完成，并在释放锁后调用 `call_soon_threadsafe`。
**例证**：`RawAsyncChannel::send_or_register` / `register_getter_ticket`（`src/channel.rs`）、
`Channel.send` / `Channel.recv`（`primitives.py`）。

### 模式 3：取消路径的锁重获取（shield 缺失 → 死锁）

**症状**：`Condition.wait()` 被取消后，`finally`/异常路径里重获取锁时，
`await lock.acquire()` 立刻再抛 `CancelledError`，锁永远拿不回 → 任务
卡死在清理路径，锁泄漏。
**根因**：Python 的取消是"每次 await 都会重抛"的，清理代码里的 await
不能执行。
**正确姿势**：将异步清理路径与取消隔离——通用清理块使用 `CancelScope(shield=True)`（`_cancel.py`, `src/cancel.rs`），在任务 Rust `RawTaskCancelState` 栈上屏蔽外层 `CancelScope` 的截止时间与取消（保护 `sleep`、`wait_cancellable`、`Channel`、`Lock` 等清理 await），同时保持 CPython `task.cancelling()` 不受修改；或者采用吸收取消的重试循环——`asyncio.Condition.wait` 的官方模式（bpo-34094 家族），在 `while True` 循环内捕获 `asyncio.CancelledError` 重试直至 `await self._lock.acquire()` 成功拿回锁，再重新抛出 `CancelledError`。
**例证**：`Condition._reacquire_lock`（`_sync.py`，配合 `Condition.wait` 中的 `_raw.forward_notify()`）；`CancelScope(shield=True)`（`_cancel.py`）。

### 模式 4：通知与等待的锁不一致

**症状**：通知方需要持有某把锁才能 notify，而等待方在等通知前**已经释放
了那把锁** → 通知方永远拿不到锁 → 死锁。
**根因**：notify 依赖的锁与 wait 释放的锁不是同一把，或 notify 被错误地
要求持锁。
**正确姿势**：`notify()` 不要求底层锁；waiter 队列由独立的 Rust `RawAsyncCondition` 互斥锁保护，`notify` 只操作等待队列、不碰业务锁。
**例证**：`Condition.notify()`（`_sync.py`, `src/sync.rs`）——持锁 notify 还会
让生产者批量攒通知、饿死 waiter。

### 模式 5：轮次/代际错位与 ABA 指针复用（wrong-round removal）

**症状**：上一轮的取消/超时处理删掉了**下一轮**的 waiter 条目，或新分配的 `asyncio.Future` 复用了刚完成的 Future 内存地址（`fut.as_ptr()`）而被迟到的取消回调误删。
**根因**：waiter 条目或内存地址发生复用，基于裸指针或无代际校验的删除逻辑误删了新等待者。
**正确姿势**：每次 waiter 注册分配单调递增的 `WaiterId (u64)` 票据（`src/waiter.rs`），对于可循环轮次原语同时记录当时的 `generation`；取消处理严格按 `ticket_id`（循环屏障按 `(generation, ticket_id)`）比对后再决定删除或转发令牌。
**例证**：`RawAsyncBarrier::cancel_waiter`（`src/sync.rs`）、`RawAsyncWaitGroup::cancel_waiter_ticket`（`src/waitgroup.rs`）、
`RawAsyncSemaphore::cancel_waiter`（`src/sync.rs`）。

**Barrier Broken 注**：generation 守卫保护的是*下一轮*条目，且当轮次凑齐前有 party 被取消时，屏障自动转移至 Broken 状态并唤醒当前轮次所有等待者抛出异常，从根源杜绝死锁。

**已知限制**：`Channel`/`Lock`/`Semaphore`/`CapacityLimiter`/`Condition`/`RWMutex`/`WaitGroup`/`Once` 取消处理里按 `WaiterId` 扫描队列是每个取消 waiter O(n)，N 个挂起 waiter 的取消风暴总成本
O(n²)（而 `Barrier::cancel_waiter` 在首个取消到达时 O(n) 置破本轮并推进 `generation`，同轮后续取消凭 `generation` O(1) 提前返回）。真实等待者数量级下可接受；记录在案，防止后人"优化"成 wrong-round 或 ABA bug。

### 模式 6：注册-完成竞态（register-after-done）

**症状**：`wait()` 检查"计数已为 0"后正要注册，`done()` 恰好把计数减到 0
并清空 waiter 列表 → 新 waiter 永远不被唤醒。
**根因**：检查与注册之间的窗口。
**正确姿势**：注册函数在同一把互斥锁内原子检查完成状态，已完成时直接返回 `None` 而不入队等待
（Go `sync.WaitGroup.Wait` 同款语义）。
**例证**：`AsyncWaitGroup.wait()` + 返回 `Option<WaiterId>` 的 `RawAsyncWaitGroup::register_waiter_ticket()`
（`src/multiloop/primitives.py:752-763`、`src/waitgroup.rs:112-125`），内部受 `parking_lot::Mutex<WaitGroupInner>` 保护。注意：**所有正 delta 的 `add()` 必须 happens-before `register_waiter_ticket()`**，确保调用方不会在工作协程启动前误观察到计数器为 0 并提前放行。

### 模式 7：跨线程共享 asyncio 对象

**症状**：一个 loop 的 `asyncio.Event`/`Task` 被另一个线程直接 set/cancel
→ `RuntimeError` 或静默丢失。
**根因**：asyncio 对象不是线程安全的；`call_soon_threadsafe` 是唯一安全
的跨线程注入通道。
**正确姿势**：跨线程一律 `loop.call_soon_threadsafe(fut.set_result, ...)`
或 `call_soon_threadsafe(event.set)`；线程间通信用 `Channel`，不要
直接传 asyncio 原语。
**例证**：`wake_waiter_outside_lock` / `wake_pair_outside_lock`（`src/waiter.rs`）、
`RawAsyncChannel::dispatch_wake`（`src/channel.rs`）、`RawTaskCancelState::cancel_scope`（`src/cancel.rs`）、
`RawAsyncEvent::set` 与 `RawAsyncLock::release`（`src/sync.rs`）。

### 模式 8：死锁家族（锁顺序 / await 持锁 / 递归持锁）

**症状**：两个线程各自持有对方要的锁；或持锁时 await；或同一线程重复
获取非重入锁。
**正确姿势**：
- 多锁场景固定严格的加锁顺序（或如 `RawAsyncCapacityLimiter` 一样将复合状态收敛到单一把 Rust `parking_lot::Mutex` 下）；
- **绝不在持锁期间 await**（`threading.Lock` 无法跨 await 保持，等价于
  释放了锁还以为是持锁状态——`asyncio.Lock` 才有此语义）；
- 调用可能再取锁或触发 Python 回调的函数前先 `drop(guard)` 释放已持锁。
**例证**：`NativeWorkerPool::pop_work` 的跨 worker 缓冲窃取（`src/pool.rs:341-363`，先释放 `victim_buf` 再获取 `self.buffers[index]` 锁）以及所有锁外唤醒路径（`src/channel.rs`, `src/sync.rs`, `src/rwlock.rs`, `src/waitgroup.rs`, `src/waiter.rs`）均先 `drop(guard)` 释放互斥锁再获取下一把锁或触发 Python 回调。

### 模式 9：组件特定反模式与误用陷阱

**症状**：由于违反各组件特定的 API 契约或并发边界，导致运行时报错、死锁、饿死或限流失效。

**1. `AsyncWaitGroup.track` 未 await 协程泄漏与 `wrap` 延迟递增**：
- **陷阱**：`wg.track(coro)` 在调用点**立即**执行 `wg.add(1)`，并返回在 `try/finally`（或 `.close()`）中调用 `wg.done()` 的追踪协程包装器。若创建了 `wg.track(worker())` 却既不 `await`、也不传给 `tg.start_soon(...)` / `pool.submit(...)` 或手动 `.close()`，会导致计数器永久泄漏（GC 时发出 `RuntimeWarning`）。相反，`wg.wrap(fn)` 在子任务实际启动执行时才延迟调用 `wg.add(1)`，若子任务尚未步进就立即 `await wg.wait()` 则可能发生竞态。
- **错误示例**：
```python
async def worker():
    pass


wg = AsyncWaitGroup()
tracked = wg.track(worker())  # add(1) 已立即执行；丢弃 `tracked` 会导致计数器永久泄漏！
```
- **正确姿势**：直接将 `wg.track(worker())` 传给 `tg.start_soon(...)` 或 `pool.submit(...)`（两者均接受协程对象，且在未启动即取消时会自动 `.close()`），或直接 `await wg.track(worker())`：
```python
async with TaskGroup() as tg:
    wg = AsyncWaitGroup()
    tg.start_soon(wg.track(worker()))  # add(1) happens-before wait()；done() 在 finally 中执行
    await wg.wait()
```

**2. `select_channel` 线程局部轮询索引假设陷阱（无位置优先级与步调共振混叠）**：
- **陷阱**：Phase 1 通过线程局部单调计数器（`_next_select_index`）轮转 `try_recv()` 起始探测下标，因此参数顺序**不会**赋予靠前通道优先权；反之，同一线程内步调一致地交替调用 `N` 处 `N` 通道 `select_channel` 会触发 `counter % N` 同余混叠，使各调用点被固定锁死在同一起始下标。
- **错误示例**：
```python
while True:
    # 误以为 high_prio 总是优先于 low_prio 被排空（错误：Phase 1 实际按轮询轮转！）
    ch, val = await select_channel(high_prio, low_prio)
    process(val)
```
- **正确姿势**：需严格优先级时先显式 `try_recv()` 排空高优通道再回退至 `select_channel`（或在多调用点步调共振场景下打乱通道列表）。

**3. `Barrier` 单方超时与自动破损状态机**：
- **陷阱**：当某个等待方被取消或超时退出时，`Barrier` 会自动进入 Broken 破损状态并唤醒当前轮次的所有其他等待方抛出 `RuntimeError`。在此之后，后续的 `wait()` 会持续抛出异常，直到显式调用 `barrier.reset()`。
- **错误示例**：
```python
try:
    async with fail_after(1.0):
        await barrier.wait()
except TimeoutError:
    pass  # 屏障已破损；后续 wait() 调用将直接失败!
```
- **正确姿势**：
```python
try:
    async with fail_after(1.0):
        await barrier.wait()
except TimeoutError:
    barrier.reset()  # 重置屏障代际以允许后续轮次继续运行
    raise
```

**4. `CapacityLimiter` 匿名归还与浮点容量感知断层**：
- **陷阱**：`CapacityLimiter` 是匿名令牌模型（不校验借用者 Task 身份），且浮点容量（如 `2.5`）实际有效并发槽位为整型截断容量（`int(total_tokens) = 2`）。
- **错误示例**：
```python
if limiter.available_tokens > 0:  # 例如 0.5 > 0
    await limiter.acquire()  # 若整型容量已借满则必然阻塞!
```
- **正确姿势**：统一推荐 `async with limiter:` 上下文管理器；使用 `limiter.available_capacity >= 1`（或 `available_tokens >= 1.0`）确保可立即无阻借出。

**5. 在 `CancelScope` / `TaskGroup` / `AsyncContext` 中裸用 `asyncio.*` 阻塞原语**：
- **陷阱**：`CancelScope` 不修改 CPython `task.cancel()`，因此裸用 `asyncio.sleep(...)`、`asyncio.Event().wait()`、`asyncio.Queue().get()`、`asyncio.wait_for(...)` / `asyncio.gather(...)`（会创建游离于外层取消栈之外的子 `Task`），以及未经桥接的 `await pool.submit(...)`，都会无视作用域取消与截止时间。
- **错误示例**：
```python
async with fail_after(0.1):
    await asyncio.sleep(10)  # 会卡住 10s；CancelScope 不会唤醒 asyncio.sleep
```
- **正确姿势**：直接使用支持 `CancelScope` 订阅的 `multiloop` 原语（`multiloop.sleep(...)`、`fail_after(...)`、`TaskGroup`、`multiloop.Event`、`Channel`、`run_in_pool(...)`），遇到第三方或标准库 awaitable / 原始 `Future` 时用 `wait_cancellable(...)` 包装桥接。

---

## 3. 修改前必查清单（Pre-Change Checklist）

**改并发相关代码（通道/锁/信号量/事件/条件/屏障/waitgroup/取消/pool）
前，逐条核对；任何一条不满足，先想清楚再动手。**

- [ ] 改动是否新增/修改 **waiter 注册或唤醒路径**？→ 过模式 2/4：注册与
      唤醒是否在同一把锁下？是否有非阻塞快路径+锁内原子二次校验？
- [ ] 改动是否触及 **取消/超时路径**？→ 过模式 3/5：清理路径的 await
      是否在 shield 或吸收取消的重试循环内？waiter 删除是否带 generation/`WaiterId` 票据校验？
- [ ] 改动是否让 **asyncio 对象跨线程**？→ 过模式 7：跨线程只允许
      `call_soon_threadsafe`。
- [ ] 改动是否引入 **多个锁**？→ 过模式 8：锁顺序是否一致？是否在 await
      期间持锁？调用可能取锁的函数前是否先释放已持锁？
- [ ] 改动是否涉及 **计数/令牌的增减**？→ 过模式 1/6：check 与 act 是否
      原子？取消/失败时令牌是否转发或归还（不丢失、不凭空多出）？
- [ ] Rust 侧改动是否触碰 **poller 计数、batch pull 或原子操作**？→ 见
      第 4 节：RAII guard 是否保住计数？内存序是否够用？
- [ ] 新行为是否针对**目标测试文件/函数**用 `pytest-repeat --count=50` 压测
      （禁止全量套件盲目多倍压测）？测试是否加 `@pytest.mark.free_threading`（3.14t 专用压测）？
- [ ] 发现 bug 时：是否先写了**最小复现测试**（确定性数据、稳定 FAIL）
      才开始修？（见第 5 节强制流程）

---

## 4. Rust 核心注意事项（`src/*.rs`）

### 4.1 核心组件与各自的并发边界

| 组件 | 线程边界 | 注意 |
|---|---|---|
| `NativeWorkerPool` | 任意线程可 `push_global`/`push_local`；`pop_work` 仅 worker 线程 | 关池先丢 sender 再置 flag；无界局部队列 + 兄弟缓冲窃取（`src/pool.rs`） |
| `RawAsyncChannel` | 任意线程可 send/recv/register | `parking_lot::Mutex<ChannelState>` 防插队 FIFO、`WaiterId (u64)` 票据与锁外唤醒（`src/channel.rs`） |
| `RawAsyncWaitGroup` | 任意线程可 add/done/register | 单 `parking_lot::Mutex<WaitGroupInner>` 状态机与 `WaiterId` 票据；`done()` 锁内排空等待者并在锁外经 `wake_waiter_outside_lock` 唤醒（`src/waitgroup.rs`, `src/waiter.rs`, `primitives.py`） |
| `RawAsyncRWMutex` | 任意线程可获取/释放 reader/writer | 写者优先 `parking_lot::Mutex<RWMutexInner>` 状态机、`WaiterId` 票据与锁外唤醒（`src/rwlock.rs`） |
| `RawAsyncLock` / `Semaphore` / `Event` / `Condition` / `Barrier` / `CapacityLimiter` / `Once` | 任意线程可 acquire/release/notify/wait | 统一 Rust 状态机（`src/sync.rs`），基于 `parking_lot::Mutex`、`WaiterId (u64)` 票据与锁外唤醒（`src/waiter.rs`） |
| `RawTaskCancelState` | 任意线程可 `cancel_scope` / 查询；宿主任务压栈/弹栈/挂起 | 纯 Rust 任务取消栈，在 `parking_lot::Mutex<TaskCancelInner>` 下维护并配 `AtomicBool` / `AtomicU64` 快读缓存，在锁外唤醒挂起的 `WaiterEntry`（`src/cancel.rs`, `_cancel.py`） |
| `AtomicMetrics` | 任意线程可记录/读取 | 单 worker 独立 64 字节填充原子计数器，避免伪共享（`src/metrics.rs`） |

### 4.2 软 poller 门（`num_polling`）

- `fetch_add(1, Relaxed)` 后判断 `< max(num_workers/2, 1)`，**瞬时门**：
  抢到门只对本轮 batch pull 有效，释放即失效——没有硬角色分配，任何
  worker 随时可以偷全局队列（`src/pool.rs:275-285`）。
- **必须用 `PollerGuard`（RAII）递减**：panic 也能保证 `fetch_sub`
  执行（`src/pool.rs:25-31`）。手写 `fetch_add`/`fetch_sub` 配对时，任何
  `?`/panic 路径都会泄漏计数 → 永远用 guard。

### 4.3 `pop_work` 的四层消费

优先级：**private buffer → global batch pull → local channel → single-item fallback & cross-worker buffer work-stealing**（`src/pool.rs:256-391`）。
- batch_size = `min(global_len / num_workers + 1, 128)`（排空阶段为 `64`），摊薄 flume pop 成本（`src/pool.rs:287-290`）。
- 改优先级/批次公式时，同时检查：worker 是否会饿死全局队列（Python 侧
  `await asyncio.sleep(0)` 的防贪婪机制在 `pool.py` 的 `_worker_dispatcher`）。
- `push_local` 采用**无界**局部队列保障 `pin_to` 任务 100% 强亲和性，
  避免任务漂移至全局队列且不卡死提交方，关池排空阶段验证全局队列、全部 worker 缓冲及局部通道均完全排空（`src/pool.rs:58, 156-171, 223-254, 371-381`）。

### 4.4 原子操作与内存序

- `AtomicMetrics` 每个计数器是 `#[repr(align(64))] PaddedAtomic`——防
  false sharing。**加新计数器时保持 64 字节对齐**，否则多核下吞吐会
  莫名下降（`src/metrics.rs:6-7`）。
- 已确立的序约定，新代码沿用：
  - `is_closed` 与 `RawTaskCancelState` 快读缓存（`effectively_cancelled`、`effective_deadline_bits`、`parked_id`）：`Release` store / `Acquire` load（`src/pool.rs`, `src/cancel.rs`）
  - `idle_mask`：`Release` 位更新（`fetch_or`/`fetch_and`）/ `Acquire` load 与 `AcqRel` CAS（`src/pool.rs:78-114`）
  - `num_polling` 与 `AtomicMetrics` 各计数器：`Relaxed`（`src/pool.rs`, `src/metrics.rs`）
- flume 是 lock-free 但**不是零成本**：跨线程 send/recv 仍有原子操作与
  内存栅栏。性能敏感路径不要假设"无锁=免费"。

### 4.5 `register_waiter_ticket` 的顺序约束

`RawAsyncWaitGroup`（`src/waitgroup.rs:112-125`）由 `parking_lot::Mutex<WaitGroupInner>` 单互斥锁保护：**调用方必须保证所有正 delta 的 `add()` 在 `register_waiter_ticket()` 之前 happens-before**（Go WaitGroup 同款约定）。否则在计数为 0 时调用 `wait()` 会立即返回已就绪，从而漏过后续启动的并发任务。

---

## 5. 测试方法论

### 5.1 并发回归三件套

```bash
# 压测单个测试文件（race 检测）：重复 50 次
uv run pytest tests/test_pool.py -p no:cacheprovider --count=50

# 死锁必失败而非挂死：pytest-timeout 180s + thread 模式，
# 超时自动 dump 所有线程栈（pyproject.toml 已配置）
# 卡住时看输出里的 "Thread dump" 定位 await 点

# 只跑上次失败
uv run pytest --lf
```

### 5.2 marker 语义

| marker | 用途 | CI 行为 |
|---|---|---|
| `slow` | 性能测试（`tests/test_perf_regression.py`） | 默认跳过（`addopts = -m "not slow"`）；通过 `pytest -m slow` 运行 |
| `free_threading` | 3.14t 自由线程压测 | 仅在 free-threaded 构建的 job 跑 |
| `repeat(n)` | 声明需要重复跑 | 与 `--count` 配合 |

新写的并发测试：能标 `free_threading` 就标；能确定性的就用
`pytest.mark.repeat` 声明压测意图。

### 5.3 测试辅助设施（`tests/conftest.py`）

- 运行时必须编译加载 `_multiloop_core`：所有原语均由 Rust 支撑（不存在纯 Python 回退；运行测试前需先执行 `make develop`；`multiloop.testing.rust_available()` 可检查 `_multiloop_core` 是否已编译）。
- `yielder` fixture → `wait_all_tasks_blocked()`：等所有任务阻塞后再断言，
  消除"还没开始就断言"的时序假阳性。

### 5.4 本地资源约束（M1 8GB 实测）

- **逐函数跑，不跑全量**；每次命令加 timeout（`pytest-timeout` 已全局
  配置，单测用 `--timeout=60` 更紧）。
- 3.14t 自由线程本地压测 **≤ 6 线程**，池线程数保持 4-8。
- CI 压力测试原则：**不降负载、延长超时**——本地压不过不代表 CI 压不过，
  反之亦然。

### 5.5 发现 bug 的强制流程（用户规则，不可跳过）

1. **先构造最小复现测试**：确定性数据（固定 seed/固定并发数，不用随机
   时序），能稳定 FAIL。
2. 用 `--count=50` 确认复现率，把复现测试留在测试套件里（回归测试）。
3. 修复后同一测试必须稳定 PASS，再跑 `--count=50` 确认无并发回归。
4. **禁止**"边改边试"或"只修不测"——没有复现测试的修复一律视为未完成。

---

## 6. 相关文档

- [API_ZH.md](API_ZH.md) — 完整 API 参考
- [CHOOSING_ZH.md](CHOOSING_ZH.md) — 原语选择决策表（"该用哪个"）
- [AGENTS.md](../AGENTS.md) — AI 导航（七项非显而易见设计决策的速查）
