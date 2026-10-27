//! Optional host-owned HTTP retirement and original-request drainage.
use crate::HttpError;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

/// An optional host boundary, checked immediately before every HTTP attempt.
///
/// Retirement is permanent. Already admitted requests retain their actual
/// response and remain tracked until response bytes have been read. This is not
/// message custody, recipient verification, or permission to retry a write.
pub struct HttpRequestFence {
    retired: AtomicBool,
    active: Mutex<usize>,
    changed: Notify,
    live: Box<dyn Fn() -> bool + Send + Sync>,
}
impl std::fmt::Debug for HttpRequestFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpRequestFence")
            .field("retired", &self.retired.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}
impl HttpRequestFence {
    /// The callback must be a synchronous, non-reentrant check of host authority.
    pub fn new(live: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        Self {
            retired: AtomicBool::new(false),
            active: Mutex::new(0),
            changed: Notify::new(),
            live: Box::new(live),
        }
    }
    /// Retire synchronously; no later admission can reactivate this fence.
    pub fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
    /// Admit one original HTTP operation, including an application's bounded raw
    /// HTTP request. Hold the permit through response-body completion.
    pub fn begin_request(self: &Arc<Self>) -> Result<HttpRequestPermit, HttpError> {
        if !(self.live)() {
            return Err(HttpError::RequestFenceClosed);
        }
        let mut active = self.active.lock().map_err(|_| HttpError::RequestFenceClosed)?;
        if self.retired.load(Ordering::Acquire) {
            return Err(HttpError::RequestFenceClosed);
        }
        *active = active.checked_add(1).ok_or(HttpError::RequestFenceClosed)?;
        Ok(HttpRequestPermit { fence: self.clone() })
    }
    /// Retire and join all already admitted HTTP operations. Dropping a waiter
    /// never revives authority or discards the original requests' permits.
    pub async fn retire_and_drain(&self) -> Result<(), HttpError> {
        self.retire();
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if *self.active.lock().map_err(|_| HttpError::RequestFenceClosed)? == 0 {
                return Ok(());
            }
            notified.await;
        }
    }
}
/// Original IO custody; intentionally neither Clone nor Serialize.
#[derive(Debug)]
pub struct HttpRequestPermit {
    fence: Arc<HttpRequestFence>,
}
impl Drop for HttpRequestPermit {
    fn drop(&mut self) {
        if let Ok(mut active) = self.fence.active.lock() {
            *active -= 1;
            if *active == 0 {
                self.fence.changed.notify_waiters();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn retirement_waits_original_permit_and_cannot_rearm() {
        let fence = Arc::new(HttpRequestFence::new(|| true));
        let original = fence.begin_request().unwrap();
        let mut drain = Box::pin(fence.retire_and_drain());
        assert!(futures_util::poll!(&mut drain).is_pending());
        assert!(matches!(fence.begin_request(), Err(HttpError::RequestFenceClosed)));
        drop(original);
        drain.await.unwrap();
        assert!(matches!(fence.begin_request(), Err(HttpError::RequestFenceClosed)));
    }
}
