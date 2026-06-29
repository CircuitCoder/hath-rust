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
use pin_project_lite::pin_project;
use tokio::{
    sync::{
        mpsc::{self, UnboundedSender},
        oneshot,
    },
    time::Instant,
};

mod concurrency;
mod gcra;

use concurrency::ConcurrencyLimiter;
use gcra::GcraLimiter;

// --- Placeholder configuration -------------------------------------------------
// TODO: expose via CLI / settings. These are test values only.

/// How long a request may wait in the pool (measured from receipt) before it
/// gives up and the connection is reset.
pub const SERVE_TIMEOUT: Duration = Duration::from_secs(30);

/// Request-rate limiter: sustained requests/second and burst (in requests).
const REQUEST_RATE: f64 = 100.0;
const REQUEST_BURST: u32 = 200;

/// Bandwidth limiter: sustained bytes/second and burst (in bytes).
/// 125 MB/s == 1 Gbps; counted in bytes.
const TRAFFIC_RATE: f64 = 125_000_000.0;
const TRAFFIC_BURST: u32 = 250_000_000;

/// Hard cap on the number of requests being served concurrently.
const CONCURRENCY_LIMIT: usize = 200;

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
    fn new() -> Self {
        Self {
            req_rate_limit: GcraLimiter::new(REQUEST_RATE, REQUEST_BURST, |_| 1),
            traffic_rate_limit: GcraLimiter::new(TRAFFIC_RATE, TRAFFIC_BURST, |info| info.size),
            concurrency: ConcurrencyLimiter::new(CONCURRENCY_LIMIT),
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

enum SchedMsg {
    Enqueue {
        seq: u64,
        info: ReqInfo,
        grant: oneshot::Sender<Permit>,
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
    grant: oneshot::Sender<Permit>,
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
    /// Spawn the scheduler task and return a shared handle to it.
    pub fn spawn() -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = Arc::new(Self { tx: tx.clone(), seq: AtomicU64::new(0) });
        tokio::spawn(run(rx, tx));
        handle
    }

    /// Register a request that is ready to serve. The returned [`Ticket`] keeps the
    /// request in the queue until dropped; awaiting the receiver yields a [`Permit`]
    /// once the scheduler grants the request. Only enqueue a request once its data
    /// is available to stream — the queue holds ready requests exclusively.
    pub fn enqueue(&self, info: ReqInfo) -> (Ticket, oneshot::Receiver<Permit>) {
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

async fn run(mut rx: mpsc::UnboundedReceiver<SchedMsg>, tx: UnboundedSender<SchedMsg>) {
    let mut queue: BTreeMap<Key, Waiter> = BTreeMap::new();
    let mut limiters = RateLimiters::new();

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
                    let _ = waiter.grant.send(Permit { tx: tx.clone(), info });
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
                        Some(msg) => apply(msg, &mut queue, &mut limiters),
                        None => break,
                    },
                    _ = wakeup => {}
                }
            }
            None => match rx.recv().await {
                Some(msg) => apply(msg, &mut queue, &mut limiters),
                None => break,
            },
        }
    }
}

fn apply(msg: SchedMsg, queue: &mut BTreeMap<Key, Waiter>, limiters: &mut RateLimiters) {
    match msg {
        SchedMsg::Enqueue { seq, info, grant } => {
            queue.insert((info.size, seq), Waiter { info, grant });
        }
        SchedMsg::Remove { key } => {
            queue.remove(&key);
        }
        SchedMsg::End { info } => limiters.on_end(&info),
    }
}
