// Copyright 2026 Hagency contributors
// SPDX-License-Identifier: Apache-2.0

//! Exact opt-in upload-preflight policy. No body owner or authentication scope
//! is invented for a GET; unlisted endpoints retain their original collector.
use bytes::Bytes;
use http::{Method, Request};
use ruma::api::{
    OutgoingRequest,
    client::{
        authenticated_media, discovery::get_supported_versions, media,
        state::get_state_event_for_key,
    },
    path_builder::PathBuilder as _,
};

use crate::HttpError;

#[derive(Clone, Copy)]
enum Kind {
    Upload,
    Metadata(&'static str),
}
#[derive(Clone, Copy)]
pub(super) struct ResponseBodyLimit {
    pub(super) max_bytes: usize,
    kind: Kind,
}
impl ResponseBodyLimit {
    pub(super) fn upload() -> Self {
        Self { max_bytes: 64 * 1024, kind: Kind::Upload }
    }
    fn metadata(endpoint: &'static str, max_bytes: usize) -> Self {
        Self { max_bytes, kind: Kind::Metadata(endpoint) }
    }
    pub(super) fn exceeded(self) -> HttpError {
        match self.kind {
            Kind::Upload => HttpError::UploadResponseTooLarge { max_bytes: self.max_bytes },
            Kind::Metadata(endpoint) => {
                HttpError::MetadataResponseTooLarge { endpoint, max_bytes: self.max_bytes }
            }
        }
    }
}
fn endpoint_is<R: OutgoingRequest, E: OutgoingRequest>() -> bool {
    R::METHOD == E::METHOD && R::PATH_BUILDER.all_paths().eq(E::PATH_BUILDER.all_paths())
}
/// Typed endpoint metadata prevents a coincidental URL suffix from limiting
/// other requests. Empty encryption state keys retain a final slash in Ruma's
/// serializer; Room IDs are percent-encoded as one path component. The suffix
/// additionally distinguishes this specific state from arbitrary room events.
pub(super) fn preflight_limit<R: OutgoingRequest>(
    request: &Request<Bytes>,
) -> Option<ResponseBodyLimit> {
    if request.method() != Method::GET {
        return None;
    }
    if endpoint_is::<R, get_supported_versions::Request>() {
        return Some(ResponseBodyLimit::metadata("versions", 256 * 1024));
    }
    #[allow(deprecated)] // The SDK still supports pre-authenticated-media homeservers.
    let media_config = endpoint_is::<R, authenticated_media::get_media_config::v1::Request>()
        || endpoint_is::<R, media::get_media_config::v3::Request>();
    if media_config {
        return Some(ResponseBodyLimit::metadata("media_config", 64 * 1024));
    }
    if endpoint_is::<R, get_state_event_for_key::v3::Request>()
        && request.uri().path().ends_with("/state/m.room.encryption/")
    {
        return Some(ResponseBodyLimit::metadata("encryption_state", 64 * 1024));
    }
    None
}
