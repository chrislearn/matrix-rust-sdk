// Copyright 2026 Hagency contributors
// SPDX-License-Identifier: Apache-2.0

//! Optional native ownership and admission for bounded buffered media uploads.
//! This does not bound backing-store retention, SQLite native allocations or
//! caller-owned inputs. Applications must share one instance across clients.
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use tokio::{
    runtime::Handle,
    sync::{oneshot, watch},
};

/// Limits on accepted original operations, not per-Room SDK tasks or outbox
/// rows.
#[derive(Clone, Copy, Debug)]
pub struct UploadLimits {
    /// Maximum bytes in an individual main file or thumbnail.
    pub max_file_bytes: usize,
    /// Accepted operations including the active operation and queued metadata.
    pub max_jobs: usize,
    /// Original incoming cache-write payloads, including active writes.
    pub max_pending_write_bytes: usize,
}
impl Default for UploadLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: 64 * 1024 * 1024,
            max_jobs: 16,
            max_pending_write_bytes: 128 * 1024 * 1024,
        }
    }
}
// These durable GenericApiError messages are a compatibility contract. Keep old
// strings recognizable when changing future presentation or policy versions.
const UNKNOWN_OUTCOME_MESSAGE: &str = "Attachment transport outcome is unknown; the original pending message is retained without automatic retry";
const UNKNOWN_WORKER_MESSAGE: &str =
    "Original attachment IO became unknown; restart before retrying";

/// Resource-policy errors never imply missing cached data or a completed send.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum UploadGovernorError {
    /// The accepted operation or incoming-payload quota is currently occupied.
    #[error("Attachment processing is busy; retry after current operations finish")]
    Capacity,
    /// Actual cached or incoming bytes exceed the configured file limit.
    #[error("Attachment exceeds the upload limit of {max_bytes} bytes")]
    TooLarge {
        /// Maximum accepted original file bytes.
        max_bytes: usize,
    },
    /// Transport admission happened before retirement; its outcome is unknown.
    #[error("{}", UNKNOWN_OUTCOME_MESSAGE)]
    OutcomeUnknown,
    /// A queued request has not started and its original queue is paused.
    #[error("Attachment upload is paused")]
    Paused,
    /// The original observer no longer wants a not-yet-published operation.
    #[error("Attachment operation was cancelled")]
    Cancelled,
    /// The configured store cannot enforce a bounded read.
    #[error("This media store does not provide bounded attachment reads")]
    UnsupportedStore,
    /// Admission was stopped for shutdown.
    #[error("Attachment processing is stopped")]
    Stopped,
    /// An original worker failed while physical IO might still exist.
    #[error("{}", UNKNOWN_WORKER_MESSAGE)]
    WorkerFailed,
    /// Limits would not admit a regular accepted file.
    #[error("Invalid attachment admission limits")]
    InvalidLimits,
}
/// Original transfer ownership; counters are not heap or RSS measurements.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UploadStats {
    /// All accepted operations, including queued and actual body owners.
    pub jobs: usize,
    /// Actual operation or serialized body still owns the active slot (at
    /// most1).
    pub active: usize,
    /// Metadata awaiting the original worker.
    pub queued: usize,
    /// Cache-write input bytes charged through actual original completion.
    pub pending_write_bytes: usize,
    /// Original worker failures whose physical completion is unknown.
    pub unknown: usize,
    /// New admission has stopped.
    pub stopped: bool,
}
/// The original worker joined, or its actual work remains owned and unknown.
#[derive(Clone, Copy, Debug)]
pub enum UploadShutdown {
    /// No original jobs or bodies remain and the worker thread was joined.
    Joined(UploadStats),
    /// A deadline or failure prevented confirming actual completion.
    Pending(UploadStats),
}
struct State {
    queue: VecDeque<Box<dyn RunJob>>,
    jobs: usize,
    active: usize,
    payload: usize,
    stopped: bool,
    failed: bool,
    unknown: Vec<Arc<UploadCustody>>,
}
struct Inner {
    limits: UploadLimits,
    state: Mutex<State>,
    wake: Condvar,
    changed: watch::Sender<u64>,
}
/// A disabled-by-default native governor. No worker starts until a real job.
///
/// A single owned worker awaits each original store/crypto/HTTP operation.
/// Cancelling its observing future cannot release the worker or actual body
/// quota. Share the same `Arc` for all clients of a native application.
pub struct UploadGovernor {
    inner: Arc<Inner>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
impl std::fmt::Debug for UploadGovernor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadGovernor")
            .field("limits", &self.inner.limits)
            .field("stats", &self.stats())
            .finish()
    }
}
impl UploadGovernor {
    /// Construct without IO, threads, authentication changes or store access.
    pub fn new(limits: UploadLimits) -> Result<Self, UploadGovernorError> {
        if limits.max_file_bytes == 0
            || limits.max_jobs == 0
            || limits.max_pending_write_bytes < limits.max_file_bytes
            || limits.max_file_bytes.checked_add(128).is_none()
        {
            return Err(UploadGovernorError::InvalidLimits);
        }
        let (changed, _) = watch::channel(0);
        Ok(Self {
            inner: Arc::new(Inner {
                limits,
                state: Mutex::new(State {
                    queue: VecDeque::new(),
                    jobs: 0,
                    active: 0,
                    payload: 0,
                    stopped: false,
                    failed: false,
                    unknown: Vec::new(),
                }),
                wake: Condvar::new(),
                changed,
            }),
            worker: Mutex::new(None),
        })
    }
    /// Limits enforced on actual incoming/cache bytes.
    pub fn limits(&self) -> UploadLimits {
        self.inner.limits
    }
    /// Read-only observation. It never starts the worker or accesses a store.
    pub fn stats(&self) -> UploadStats {
        let s = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        UploadStats {
            jobs: s.jobs,
            active: s.active,
            queued: s.queue.len(),
            pending_write_bytes: s.payload,
            unknown: s.unknown.len(),
            stopped: s.stopped,
        }
    }
    pub(crate) fn capacity(&self) -> watch::Receiver<u64> {
        self.inner.changed.subscribe()
    }
    fn ensure_worker(&self) -> Result<(), UploadGovernorError> {
        let mut worker = self.worker.lock().map_err(|_| UploadGovernorError::WorkerFailed)?;
        {
            let state = self.inner.state.lock().map_err(|_| UploadGovernorError::WorkerFailed)?;
            if state.failed {
                return Err(UploadGovernorError::WorkerFailed);
            }
            if state.stopped {
                return Err(UploadGovernorError::Stopped);
            }
        }
        if worker.is_none() {
            let inner = self.inner.clone();
            *worker = Some(
                std::thread::Builder::new()
                    .name("matrix-upload-io".into())
                    .spawn(move || run_worker(inner))
                    .map_err(|_| UploadGovernorError::WorkerFailed)?,
            );
        }
        Ok(())
    }
    pub(crate) fn submit<T, E, F, Fut>(
        &self,
        payload: usize,
        operation: F,
    ) -> Result<UploadOperation<T, E>, UploadGovernorError>
    where
        T: Send + 'static,
        E: From<UploadGovernorError> + Send + 'static,
        F: FnOnce(Arc<UploadCustody>, Arc<AtomicBool>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let runtime = Handle::try_current().map_err(|_| UploadGovernorError::Stopped)?;
        self.ensure_worker()?;
        let mut state = self.inner.state.lock().map_err(|_| UploadGovernorError::WorkerFailed)?;
        if state.failed {
            return Err(UploadGovernorError::WorkerFailed);
        }
        if state.stopped {
            return Err(UploadGovernorError::Stopped);
        }
        if state.jobs >= self.inner.limits.max_jobs
            || payload > self.inner.limits.max_pending_write_bytes.saturating_sub(state.payload)
        {
            return Err(UploadGovernorError::Capacity);
        }
        state.jobs += 1;
        state.payload += payload;
        let custody = Arc::new(UploadCustody {
            inner: Arc::downgrade(&self.inner),
            payload,
            active: AtomicBool::new(false),
            client: OnceLock::new(),
        });
        let live = Arc::new(AtomicBool::new(true));
        let (done, receiver) = oneshot::channel();
        state.queue.push_back(Box::new(Job {
            operation: Some(operation),
            done: Some(done),
            custody,
            live: live.clone(),
            runtime,
        }));
        self.inner.wake.notify_one();
        Ok(UploadOperation { receiver, live, completed: false })
    }
    /// Stop new admission and return queued work without dropping original IO.
    pub fn stop(&self) {
        let queued = {
            let mut s = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            s.stopped = true;
            let queued = s.queue.drain(..).collect::<Vec<_>>();
            self.inner.wake.notify_all();
            queued
        };
        for job in queued {
            job.cancel(UploadGovernorError::Stopped);
        }
        self.inner.changed.send_modify(|generation| *generation = generation.wrapping_add(1));
    }
    /// Bounded original-thread join. Call only from a shutdown worker/boundary,
    /// never an interactive UI handler. A Pending result retains the real
    /// handle.
    pub fn shutdown_until(&self, deadline: Instant) -> UploadShutdown {
        self.stop();
        let mut s = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        while s.jobs != 0 && !s.failed {
            let now = Instant::now();
            if now >= deadline {
                return UploadShutdown::Pending(self.stats_locked(&s));
            }
            let (next, timeout) = self
                .inner
                .wake
                .wait_timeout(s, deadline.saturating_duration_since(now))
                .unwrap_or_else(|e| e.into_inner());
            s = next;
            if timeout.timed_out() && s.jobs != 0 {
                return UploadShutdown::Pending(self.stats_locked(&s));
            }
        }
        if s.failed {
            return UploadShutdown::Pending(self.stats_locked(&s));
        }
        let stats = self.stats_locked(&s);
        drop(s);
        let mut worker = self.worker.lock().unwrap_or_else(|e| e.into_inner());
        while worker.as_ref().is_some_and(|join| !join.is_finished()) {
            if Instant::now() >= deadline {
                return UploadShutdown::Pending(self.stats());
            }
            drop(worker);
            std::thread::sleep(Duration::from_millis(1));
            worker = self.worker.lock().unwrap_or_else(|e| e.into_inner());
        }
        if let Some(join) = worker.take()
            && join.join().is_err()
        {
            return UploadShutdown::Pending(self.stats());
        }
        UploadShutdown::Joined(stats)
    }
    fn stats_locked(&self, s: &State) -> UploadStats {
        UploadStats {
            jobs: s.jobs,
            active: s.active,
            queued: s.queue.len(),
            pending_write_bytes: s.payload,
            unknown: s.unknown.len(),
            stopped: s.stopped,
        }
    }
}
impl Drop for UploadGovernor {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Native physical payload ownership, retained by actual serialized Bytes too.
pub(crate) struct UploadCustody {
    inner: Weak<Inner>,
    payload: usize,
    active: AtomicBool,
    client: OnceLock<crate::Client>,
}
impl UploadCustody {
    pub(crate) fn retain_client(&self, client: &crate::Client) {
        let _ = self.client.set(client.clone());
    }
}
impl std::fmt::Debug for UploadCustody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OriginalUploadCustody")
    }
}
impl Drop for UploadCustody {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            {
                let mut s = inner.state.lock().unwrap_or_else(|e| e.into_inner());
                s.jobs -= 1;
                s.payload -= self.payload;
                if self.active.load(Ordering::Acquire) {
                    s.active -= 1;
                }
                inner.wake.notify_all();
            }
            inner.changed.send_modify(|generation| *generation = generation.wrapping_add(1));
        }
    }
}
trait RunJob: Send {
    fn custody(&self) -> &Arc<UploadCustody>;
    fn run(self: Box<Self>);
    fn cancel(self: Box<Self>, reason: UploadGovernorError);
}
struct Job<T, E: From<UploadGovernorError>, F> {
    operation: Option<F>,
    done: Option<oneshot::Sender<Result<T, E>>>,
    custody: Arc<UploadCustody>,
    live: Arc<AtomicBool>,
    runtime: Handle,
}
impl<T, E, F, Fut> RunJob for Job<T, E, F>
where
    T: Send + 'static,
    E: From<UploadGovernorError> + Send + 'static,
    F: FnOnce(Arc<UploadCustody>, Arc<AtomicBool>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, E>> + Send + 'static,
{
    fn custody(&self) -> &Arc<UploadCustody> {
        &self.custody
    }
    fn run(mut self: Box<Self>) {
        let operation = self.operation.take().expect("original job is run once");
        let result = if self.live.load(Ordering::Acquire) {
            self.runtime.block_on(operation(self.custody.clone(), self.live.clone()))
        } else {
            Err(UploadGovernorError::Cancelled.into())
        };
        if let Some(done) = self.done.take() {
            let _ = done.send(result);
        }
    }
    fn cancel(mut self: Box<Self>, reason: UploadGovernorError) {
        if let Some(done) = self.done.take() {
            let _ = done.send(Err(reason.into()));
        }
    }
}
impl<T, E: From<UploadGovernorError>, F> Drop for Job<T, E, F> {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(Err(UploadGovernorError::WorkerFailed.into()));
        }
    }
}
fn run_worker(inner: Arc<Inner>) {
    loop {
        let job = {
            let mut s = inner.state.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if s.stopped && s.jobs == 0 {
                    return;
                }
                if s.active == 0
                    && let Some(job) = s.queue.pop_front()
                {
                    s.active += 1;
                    job.custody().active.store(true, Ordering::Release);
                    break job;
                }
                s = inner.wake.wait(s).unwrap_or_else(|e| e.into_inner());
            }
        };
        let custody = job.custody().clone();
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.run())).is_err() {
            let queued = {
                let mut s = inner.state.lock().unwrap_or_else(|e| e.into_inner());
                s.failed = true;
                s.stopped = true;
                s.unknown.push(custody);
                inner.wake.notify_all();
                s.queue.drain(..).collect::<Vec<_>>()
            };
            for job in queued {
                job.cancel(UploadGovernorError::WorkerFailed);
            }
            inner.changed.send_modify(|generation| *generation = generation.wrapping_add(1));
            return;
        }
        drop(custody);
    }
}
pub(crate) struct UploadOperation<T, E> {
    receiver: oneshot::Receiver<Result<T, E>>,
    live: Arc<AtomicBool>,
    completed: bool,
}
impl<T, E> Unpin for UploadOperation<T, E> {}
impl<T, E: From<UploadGovernorError>> Future for UploadOperation<T, E> {
    type Output = Result<T, E>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(result) => {
                self.completed = true;
                Poll::Ready(
                    result.unwrap_or_else(|_| Err(UploadGovernorError::WorkerFailed.into())),
                )
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl<T, E> Drop for UploadOperation<T, E> {
    fn drop(&mut self) {
        if !self.completed {
            self.live.store(false, Ordering::Release);
        }
    }
}

pub(crate) async fn bounded_content(
    client: &crate::Client,
    request: &matrix_sdk_base::media::MediaRequestParameters,
    limit: usize,
) -> Result<Option<Vec<u8>>, crate::Error> {
    use matrix_sdk_base::media::store::BoundedMediaStoreError;
    let store = client.media_store().lock().await?;
    match store.get_media_content_bounded(request, limit).await {
        Ok(content) => Ok(content),
        Err(BoundedMediaStoreError::Exceeded { .. }) => {
            Err(UploadGovernorError::TooLarge { max_bytes: limit }.into())
        }
        Err(BoundedMediaStoreError::Unsupported) => {
            Err(UploadGovernorError::UnsupportedStore.into())
        }
        Err(BoundedMediaStoreError::Store(error)) => Err(error.into()),
        Err(_) => Err(UploadGovernorError::UnsupportedStore.into()),
    }
}
impl UploadGovernor {
    pub(crate) async fn cache_length(
        &self,
        weak: crate::WeakClient,
        request: matrix_sdk_base::media::MediaRequestParameters,
    ) -> Result<Option<usize>, crate::Error> {
        let limit = self.limits().max_file_bytes;
        self.submit(0, move |custody, _| async move {
            let client = weak.get().ok_or(UploadGovernorError::Paused)?;
            custody.retain_client(&client);
            Ok(bounded_content(&client, &request, limit).await?.map(|bytes| bytes.len()))
        })?
        .await
    }
}

// Runs only on the original owned OS worker, including synchronous crypto.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn upload_original(
    room: &crate::Room,
    request: matrix_sdk_base::media::MediaRequestParameters,
    content_type: String,
    progress: Option<eyeball::SharedObservable<crate::TransmissionProgress>>,
    limit: usize,
    custody: Arc<UploadCustody>,
    live: Arc<AtomicBool>,
) -> Result<ruma::events::room::MediaSource, crate::Error> {
    use std::{str::FromStr, sync::atomic::AtomicU64};

    use matrix_sdk_base::store::QueueWedgeError;
    use ruma::events::room::MediaSource;

    use crate::{Media, config::RequestConfig};
    let client = room.client();
    let admissions = Arc::new(AtomicU64::new(0));
    custody.retain_client(&client);
    let data = bounded_content(&client, &request, limit)
        .await?
        .ok_or(crate::Error::SendQueueWedgeError(Box::new(QueueWedgeError::MissingMediaContent)))?;
    let mime = mime::Mime::from_str(&content_type).map_err(|_| {
        crate::Error::SendQueueWedgeError(Box::new(QueueWedgeError::InvalidMimeType {
            mime_type: content_type,
        }))
    })?;
    if !live.load(Ordering::Acquire) {
        return Err(UploadGovernorError::Cancelled.into());
    }
    #[cfg(feature = "e2e-encryption")]
    let (data, keys) = if room
        .latest_encryption_state()
        .await
        .map_err(|error| classify_closed_http(error, admissions.load(Ordering::Acquire)))?
        .is_encrypted()
    {
        use std::io::Read;
        let mut encrypted = Vec::with_capacity(data.len());
        let mut cursor = std::io::Cursor::new(data);
        let mut encryptor = matrix_sdk_base::crypto::AttachmentEncryptor::new(&mut cursor);
        let mut chunk = [0u8; 64 * 1024];
        loop {
            if !live.load(Ordering::Acquire) {
                return Err(UploadGovernorError::Cancelled.into());
            }
            let count = encryptor.read(&mut chunk)?;
            if count == 0 {
                break;
            }
            if count > limit.saturating_sub(encrypted.len()) {
                return Err(UploadGovernorError::TooLarge { max_bytes: limit }.into());
            }
            encrypted.extend_from_slice(&chunk[..count]);
        }
        // finish consumes the encryptor and its original plaintext before HTTP.
        let keys = encryptor.finish();
        drop(cursor);
        (encrypted, Some(keys))
    } else {
        (data, None)
    };
    if !live.load(Ordering::Acquire) {
        return Err(UploadGovernorError::Cancelled.into());
    }
    let owner = crate::http_client::BodyOwner {
        // Actual serialized request Bytes retain BOTH the budget and the original
        // client/credential lease, even after the observer or HTTP future drops.
        _custody: Arc::new((custody, client.clone())),
        admissions: admissions.clone(),
    };
    #[cfg(feature = "e2e-encryption")]
    let mime = if keys.is_some() { mime::APPLICATION_OCTET_STREAM } else { mime };
    let config = RequestConfig::short_retry().timeout(Media::reasonable_upload_timeout(&data));
    let mut upload = client.media().upload(&mime, data, Some(config)).with_upload_custody(owner);
    if let Some(progress) = progress {
        upload = upload.with_send_progress_observable(progress);
    }
    let response = upload
        .await
        .map_err(|error| classify_closed_http(error, admissions.load(Ordering::Acquire)))?;
    #[cfg(feature = "e2e-encryption")]
    if let Some(keys) = keys {
        return Ok(MediaSource::Encrypted(Box::new(ruma::events::room::EncryptedFile::new(
            response.content_uri,
            keys.encryption_info,
            keys.hashes,
        ))));
    }
    Ok(MediaSource::Plain(response.content_uri))
}

// Match typed original errors, including the SDK's automatic Matrix refresh
// wrapper. Ordinary final 401/refresh401 failures retain their SDK semantics.
fn classify_closed_http(error: crate::Error, admissions: u64) -> crate::Error {
    fn closed(error: &crate::HttpError) -> bool {
        match error {
            crate::HttpError::RequestFenceClosed => true,
            crate::HttpError::RefreshToken(crate::error::RefreshTokenError::MatrixAuth(error)) => {
                closed(error)
            }
            _ => false,
        }
    }
    if matches!(&error, crate::Error::Http(error) if matches!(error.as_ref(), crate::HttpError::UploadResponseTooLarge { .. }))
    {
        // A POST was admitted but its bounded response supplies no trusted
        // upload receipt. Persist unknown rather than automatically resending.
        return UploadGovernorError::OutcomeUnknown.into();
    }
    if matches!(&error, crate::Error::Http(error) if closed(error)) {
        if admissions == 0 {
            UploadGovernorError::Paused.into()
        } else {
            UploadGovernorError::OutcomeUnknown.into()
        }
    } else {
        error
    }
}

impl UploadGovernorError {
    pub(crate) fn requires_explicit_retry(&self) -> bool {
        matches!(self, Self::OutcomeUnknown | Self::WorkerFailed)
    }
}
// A fresh receipt distinguishes separate unknown attempts of the same outbox
// transaction. The safe message and receipt are persisted together using the
// existing GenericApiError representation; no payload or credential is encoded.
pub(crate) fn persist_unknown(error: crate::Error) -> crate::Error {
    let message = match &error {
        crate::Error::UploadGovernor(UploadGovernorError::OutcomeUnknown) => {
            UNKNOWN_OUTCOME_MESSAGE
        }
        crate::Error::UploadGovernor(UploadGovernorError::WorkerFailed) => UNKNOWN_WORKER_MESSAGE,
        _ => return error,
    };
    let receipt = ruma::TransactionId::new();
    crate::Error::SendQueueWedgeError(Box::new(
        matrix_sdk_base::store::QueueWedgeError::GenericApiError {
            msg: format!("{message} [hagency-upload-receipt-v1:{receipt}]"),
        },
    ))
}

pub(crate) fn unknown_wedge_receipt(
    error: &matrix_sdk_base::store::QueueWedgeError,
) -> Option<&str> {
    let matrix_sdk_base::store::QueueWedgeError::GenericApiError { msg } = error else {
        return None;
    };
    if msg == UNKNOWN_OUTCOME_MESSAGE || msg == UNKNOWN_WORKER_MESSAGE {
        // Compatibility with unknown rows produced before per-attempt receipts.
        return Some(msg);
    }
    let (message, receipt) = msg.split_once(" [hagency-upload-receipt-v1:")?;
    if message != UNKNOWN_OUTCOME_MESSAGE && message != UNKNOWN_WORKER_MESSAGE {
        return None;
    }
    let receipt = receipt.strip_suffix(']')?;
    // The pinned Ruma generator uses a 32-digit simple UUID. This is a narrow
    // versioned marker, never a substring match on arbitrary server errors.
    (receipt.len() == 32 && receipt.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then_some(msg.as_str())
}

/// Opaque confirmation receipt for one persisted unknown upload failure.
/// Live send-queue errors and their restored durable representation carry the
/// same receipt. Retain it with the original SendHandle and use
/// [`crate::send_queue::SendHandle::unwedge_if_current_error`] to reject a
/// later unknown failure under the original queue lock. Direct,
/// not-yet-persisted transport errors have no receipt. Legacy unknown rows
/// remain recognizable.
pub fn unknown_upload_receipt(error: &crate::Error) -> Option<&str> {
    match error {
        crate::Error::SendQueueWedgeError(error) => unknown_wedge_receipt(error),
        _ => None,
    }
}

/// Recognize an opt-in unknown original upload outcome, including its existing
/// durable GenericApiError representation after restoration. A host must ask
/// for explicit confirmation before conditionally unwedging this request;
/// enabling a queue alone must not retry it. No authentication failure is
/// matched.
pub fn is_unknown_upload(error: &crate::Error) -> bool {
    match error {
        crate::Error::UploadGovernor(policy) => policy.requires_explicit_retry(),
        _ => unknown_upload_receipt(error).is_some(),
    }
}

#[cfg(test)]
mod tests;
