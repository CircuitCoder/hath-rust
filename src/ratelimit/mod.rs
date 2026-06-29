//! Generic serve scheduler + rate limiting for the file-serving (`/h/`) route.
//!
//! # Flow
//! 1. The handler determines the response size and, *once the data is ready to
//!    stream*, [`enqueue`]s the request.
//! 2. The request sits in a priority queue (smallest response first) until every
//!    rate limiter allows it. Only ready requests are ever enqueued, so every
//!    entry in the queue is immediately servable.
//! 3. A single owner task ([`run`]) dispatches the smallest request whenever all
//!    limiters allow, handing back a [`Permit`] that is held for the whole
//!    response body.
//! 4. If the handler's own timeout elapses first, it drops its [`Ticket`], which
//!    removes the request from the queue, and resets the connection.
//!
//! Concurrency is handled by single ownership: the queue and all limiter state
//! live inside the [`run`] task and are mutated only via messages, so no shared
//! locks are taken on the hot path.
//!
//! [`enqueue`]: SchedulerHandle::enqueue

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use axum::body::Body;
use bytes::Bytes;
use futures::future::select_all;
use http_body::Body as HttpBody;
use log::warn;
use pin_project_lite::pin_project;
use tokio::{
    sync::{
        mpsc::{self, Sender, UnboundedSender},
        oneshot,
    },
    time::Instant,
};

use crate::Command;

mod concurrency;
mod gcra;

use concurrency::ConcurrencyLimiter;
use gcra::GcraLimiter;

// --- Tunable configuration -----------------------------------------------------

/// Tunable limits for the scheduler and rate limiters, supplied from CLI args.
#[derive(Clone, Copy)]
pub struct SchedulerConfig {
    /// Maximum number of requests allowed to sit in the queue; pushing past it
    /// evicts the largest (lowest priority) waiter.
    pub queue_limit: usize,
    /// Request-rate limiter: sustained requests/second.
    pub request_rate: f64,
    /// Request-rate limiter: burst (in requests).
    pub request_burst: u32,
    /// Bandwidth limiter: sustained bytes/second.
    pub traffic_rate: f64,
    /// Bandwidth limiter: burst (in bytes).
    pub traffic_burst: u32,
    /// Hard cap on the number of requests being served concurrently.
    pub concurrency_limit: usize,
    /// Sustained rejected-requests-per-second tolerated before signalling overload.
    pub overload_rate: f64,
    /// Minimum gap between two overload notifications sent to the RPC server.
    pub overload_notify_interval: Duration,
}

// --- Public request description ------------------------------------------------

/// Information about a request that rate limiters may use for their decisions.
#[derive(Clone, Copy)]
pub struct ReqInfo {
    /// Number of bytes the response will actually transfer (0 for HEAD, the range
    /// length for range requests, otherwise the full file size). Used both as the
    /// scheduling priority (smallest first) and as the bandwidth limiter's cost.
    pub size: u32,
}

/// A future that resolves when a blocked limiter might allow progress.
type Wakeup = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Outcome of asking the limiters whether a request may start.
pub enum Decision {
    /// All limiters allow the request to start now.
    Ready,
    /// At least one limiter blocks; the future resolves when re-checking is worthwhile.
    Blocked(Wakeup),
}

// --- Rate limiter set ----------------------------------------------------------

/// The fixed set of rate limiters. Intentionally a concrete struct (not a
/// `Vec<dyn RateLimiter>`): the set is known at compile time and new limiters are
/// added as additional fields. Serving requires *every* limiter to allow.
struct RateLimiters {
    /// Limits request rate; each request costs 1.
    req_rate_limit: GcraLimiter,
    /// Limits bandwidth; each request costs its response size in bytes.
    traffic_rate_limit: GcraLimiter,
    /// Hard cap on concurrently-serving requests.
    concurrency: ConcurrencyLimiter,
}

impl RateLimiters {
    fn new(config: &SchedulerConfig) -> Self {
        Self {
            req_rate_limit: GcraLimiter::new(config.request_rate, config.request_burst, |_| 1),
            traffic_rate_limit: GcraLimiter::new(config.traffic_rate, config.traffic_burst, |info| info.size),
            concurrency: ConcurrencyLimiter::new(config.concurrency_limit),
        }
    }

    /// Read-only check across all limiters. Returns [`Decision::Ready`] only when
    /// every limiter allows; otherwise a single wakeup that fires as soon as *any*
    /// blocking limiter might allow progress (re-checking then converges).
    fn check(&self, now: Instant, info: &ReqInfo) -> Decision {
        let mut wakeups = Vec::new();
        for decision in [
            self.req_rate_limit.check(now, info),
            self.traffic_rate_limit.check(now, info),
            self.concurrency.check(now, info),
        ] {
            if let Decision::Blocked(wakeup) = decision {
                wakeups.push(wakeup);
            }
        }
        match wakeups.len() {
            0 => Decision::Ready,
            1 => Decision::Blocked(wakeups.pop().expect("len == 1")),
            _ => Decision::Blocked(Box::pin(async move {
                let _ = select_all(wakeups).await;
            })),
        }
    }

    /// Commit a grant on every limiter.
    fn on_start(&mut self, now: Instant, info: &ReqInfo) {
        self.req_rate_limit.on_start(now, info);
        self.traffic_rate_limit.on_start(now, info);
        self.concurrency.on_start(now, info);
    }

    /// Release a finished serve on every limiter.
    fn on_end(&mut self, info: &ReqInfo) {
        self.req_rate_limit.on_end(info);
        self.traffic_rate_limit.on_end(info);
        self.concurrency.on_end(info);
    }
}

// --- Scheduler messages & state ------------------------------------------------

/// Result delivered to a waiting handler once the scheduler reaches its request.
pub enum GrantResult {
    /// The request may proceed; hold the [`Permit`] for the whole response body.
    Granted(Permit),
    /// The request was evicted from the queue (capacity exceeded). The handler
    /// should behave as if its serve timeout elapsed.
    Evicted,
}

enum SchedMsg {
    Enqueue {
        seq: u64,
        info: ReqInfo,
        grant: oneshot::Sender<GrantResult>,
    },
    Remove {
        key: Key,
    },
    End {
        info: ReqInfo,
    },
}

struct Waiter {
    info: ReqInfo,
    grant: oneshot::Sender<GrantResult>,
}

/// Priority key: smallest response first, then arrival order for stability.
type Key = (u32, u64);

// --- Handles given out to request handlers -------------------------------------

/// Shared handle used by request handlers to talk to the scheduler task.
pub struct SchedulerHandle {
    tx: UnboundedSender<SchedMsg>,
    seq: AtomicU64,
}

impl SchedulerHandle {
    /// Spawn the scheduler task and return a shared handle to it. The `command`
    /// channel is used to notify the RPC server when the queue sheds too many
    /// requests (overload).
    pub fn spawn(config: SchedulerConfig, command: Sender<Command>) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = Arc::new(Self { tx: tx.clone(), seq: AtomicU64::new(0) });
        tokio::spawn(run(rx, tx, config, command));
        handle
    }

    /// Register a request that is ready to serve. The returned [`Ticket`] keeps the
    /// request in the queue until dropped; awaiting the receiver yields a
    /// [`GrantResult`] once the scheduler grants or evicts the request. Only
    /// enqueue a request once its data is available to stream — the queue holds
    /// ready requests exclusively.
    pub fn enqueue(&self, info: ReqInfo) -> (Ticket, oneshot::Receiver<GrantResult>) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let (grant_tx, grant_rx) = oneshot::channel();
        let _ = self.tx.send(SchedMsg::Enqueue { seq, info, grant: grant_tx });
        (
            Ticket {
                key: (info.size, seq),
                tx: self.tx.clone(),
            },
            grant_rx,
        )
    }
}

/// Keeps a request registered in the scheduler queue. Dropping it (on timeout,
/// early error, or after a grant) removes the request from the queue; the removal
/// is idempotent, so dropping after a grant is harmless.
pub struct Ticket {
    key: Key,
    tx: UnboundedSender<SchedMsg>,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let _ = self.tx.send(SchedMsg::Remove { key: self.key });
    }
}

/// Proof that a serve was granted by every rate limiter. Must be held for the
/// entire response body; dropping it releases the serve (semaphore-style limiters
/// react to this).
pub struct Permit {
    tx: UnboundedSender<SchedMsg>,
    info: ReqInfo,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let _ = self.tx.send(SchedMsg::End { info: self.info });
    }
}

impl Permit {
    /// Attach this permit to a response so it is released when the body is fully
    /// sent or dropped (e.g. client disconnect mid-stream).
    pub fn guard(self, body: Body) -> Body {
        Body::new(GuardBody { inner: body, _permit: self })
    }
}

// --- Cache-refill concurrency limiter ------------------------------------------

/// Caps the number of concurrent cache-miss refills. Acquisition is a load + CAS
/// (`fetch_update`): if at capacity, no slot is taken and the caller should behave
/// as if its serve timed out instead of starting a refill.
pub struct RefillLimiter {
    count: AtomicU64,
    limit: u64,
}

impl RefillLimiter {
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self { count: AtomicU64::new(0), limit })
    }

    /// Reserve a refill slot, returning a guard that releases it on drop. Returns
    /// `None` when at capacity (refill should be skipped).
    pub fn try_acquire(self: &Arc<Self>) -> Option<RefillPermit> {
        self.count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < self.limit).then_some(n + 1))
            .ok()
            .map(|_| RefillPermit { limiter: self.clone() })
    }
}

/// Holds a refill slot for the lifetime of a download worker; releases on drop.
pub struct RefillPermit {
    limiter: Arc<RefillLimiter>,
}

impl Drop for RefillPermit {
    fn drop(&mut self) {
        self.limiter.count.fetch_sub(1, Ordering::AcqRel);
    }
}

pin_project! {
    /// Response body wrapper that holds a [`Permit`] for as long as the body lives.
    struct GuardBody {
        #[pin]
        inner: Body,
        _permit: Permit,
    }
}

impl HttpBody for GuardBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.project().inner.poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

// --- Scheduler task ------------------------------------------------------------

async fn run(mut rx: mpsc::UnboundedReceiver<SchedMsg>, tx: UnboundedSender<SchedMsg>, config: SchedulerConfig, command: Sender<Command>) {
    let mut queue: BTreeMap<Key, Waiter> = BTreeMap::new();
    let mut limiters = RateLimiters::new(&config);
    let mut overload = OverloadTracker::new(config.overload_rate, config.overload_notify_interval, command);

    loop {
        // Dispatch as many requests as the limiters currently allow. Every queued
        // request is ready to serve, so the smallest-key entry (smallest response,
        // then arrival order) is always the next candidate — an O(log n) peek.
        let wakeup = loop {
            let now = Instant::now();
            let Some((_, waiter)) = queue.first_key_value() else {
                break None; // Queue empty.
            };
            let info = waiter.info;
            match limiters.check(now, &info) {
                Decision::Ready => {
                    let (_, waiter) = queue.pop_first().expect("queue just observed non-empty");
                    // Skip handlers that already gave up so we don't consume capacity
                    // for a serve that will never happen.
                    if waiter.grant.is_closed() {
                        continue;
                    }
                    // Commit before building the permit so that on_start and the
                    // permit's eventual End (on_end) are paired exactly 1:1 — even
                    // if `send` races a handler drop, the returned permit's End
                    // releases the capacity we just took.
                    limiters.on_start(now, &info);
                    let _ = waiter.grant.send(GrantResult::Granted(Permit { tx: tx.clone(), info }));
                }
                Decision::Blocked(wakeup) => break Some(wakeup),
            }
        };

        // Wait for a queue change or for a blocked limiter to free up.
        match wakeup {
            Some(wakeup) => {
                tokio::select! {
                    biased;
                    msg = rx.recv() => match msg {
                        Some(msg) => apply(msg, &mut queue, &mut limiters, &config, &mut overload),
                        None => break,
                    },
                    _ = wakeup => {}
                }
            }
            None => match rx.recv().await {
                Some(msg) => apply(msg, &mut queue, &mut limiters, &config, &mut overload),
                None => break,
            },
        }
    }
}

fn apply(
    msg: SchedMsg,
    queue: &mut BTreeMap<Key, Waiter>,
    limiters: &mut RateLimiters,
    config: &SchedulerConfig,
    overload: &mut OverloadTracker,
) {
    match msg {
        SchedMsg::Enqueue { seq, info, grant } => {
            queue.insert((info.size, seq), Waiter { info, grant });
            // Cap the queue: pushing past the limit evicts the largest (lowest
            // priority) waiter so smallest-first dispatch is preserved.
            if queue.len() > config.queue_limit
                && let Some((_, waiter)) = queue.pop_last()
            {
                let _ = waiter.grant.send(GrantResult::Evicted);
                overload.record_rejection();
            }
        }
        SchedMsg::Remove { key } => {
            queue.remove(&key);
        }
        SchedMsg::End { info } => limiters.on_end(&info),
    }
}

/// Tracks rejected (evicted) requests with a GCRA bucket sized to one minute of
/// the configured rate. When sustained rejections exhaust the bucket, the RPC
/// server is told we are overloaded — at most once per `notify_interval`.
struct OverloadTracker {
    gcra: GcraLimiter,
    command: Sender<Command>,
    notify_interval: Duration,
    last_notify: Option<Instant>,
}

impl OverloadTracker {
    fn new(rate: f64, notify_interval: Duration, command: Sender<Command>) -> Self {
        let burst = (rate * 60.0).ceil() as u32;
        Self {
            gcra: GcraLimiter::new(rate, burst, |_| 1),
            command,
            notify_interval,
            last_notify: None,
        }
    }

    fn record_rejection(&mut self) {
        let now = Instant::now();
        let info = ReqInfo { size: 1 };
        match self.gcra.check(now, &info) {
            Decision::Ready => self.gcra.on_start(now, &info),
            Decision::Blocked(_) => {
                if self.last_notify.is_none_or(|last| now.duration_since(last) >= self.notify_interval) {
                    self.last_notify = Some(now);
                    warn!("Server overloaded!");
                    let _ = self.command.try_send(Command::Overload);
                }
            }
        }
    }
}
