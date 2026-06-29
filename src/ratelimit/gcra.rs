use std::time::Duration;

use tokio::time::{Instant, sleep};

use super::{Decision, ReqInfo};

/// A GCRA (Generic Cell Rate Algorithm) rate limiter over an abstract resource.
///
/// Each request consumes some amount of the resource, given by [`cost`]. The two
/// concrete uses differ *only* in their parameters and cost function:
/// - request-rate limiting: `cost = 1` per request,
/// - bandwidth limiting: `cost = response size in bytes`.
///
/// State is a single theoretical arrival time (`tat`). Capacity is only ever
/// consumed on serve start ([`on_start`]); there is nothing to release on serve
/// end, so it wakes purely on the clock.
///
/// [`cost`]: GcraLimiter::cost
/// [`on_start`]: GcraLimiter::on_start
pub struct GcraLimiter {
    /// Emission interval per unit of resource, `T = 1 / rate`.
    per_unit: Duration,
    /// Burst tolerance `tau = burst * T` (the bucket's full drain time).
    tolerance: Duration,
    /// Resource consumed by a request (e.g. `1` for req-rate, size for bandwidth).
    cost: fn(&ReqInfo) -> u32,
    /// Theoretical arrival time.
    tat: Instant,
}

impl GcraLimiter {
    /// Build a limiter allowing a sustained `rate` units/second with up to
    /// `burst` units consumed back-to-back. `cost` maps a request to the number
    /// of units it consumes.
    pub fn new(rate: f64, burst: u32, cost: fn(&ReqInfo) -> u32) -> Self {
        let per_unit = Duration::from_secs_f64(1.0 / rate);
        let tolerance = per_unit.saturating_mul(burst);
        Self {
            per_unit,
            tolerance,
            cost,
            tat: Instant::now(),
        }
    }

    /// Time the theoretical arrival time advances if this request is served.
    fn increment(&self, info: &ReqInfo) -> Duration {
        self.per_unit.saturating_mul((self.cost)(info))
    }

    /// Read-only check: is a request allowed to start right now?
    ///
    /// Allowed when `tat + increment <= now + tolerance`. Otherwise the wakeup
    /// sleeps exactly until that becomes true.
    pub fn check(&self, now: Instant, info: &ReqInfo) -> Decision {
        let target = self.tat + self.increment(info);
        let threshold = now + self.tolerance;
        if target <= threshold {
            Decision::Ready
        } else {
            Decision::Blocked(Box::pin(sleep(target.saturating_duration_since(threshold))))
        }
    }

    /// Commit a grant: advance the theoretical arrival time by the request's cost.
    pub fn on_start(&mut self, now: Instant, info: &ReqInfo) {
        self.tat = self.tat.max(now) + self.increment(info);
    }

    /// GCRA has no per-serve state to release.
    pub fn on_end(&mut self, _info: &ReqInfo) {}
}
