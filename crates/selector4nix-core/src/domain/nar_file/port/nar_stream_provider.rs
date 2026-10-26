use std::pin::Pin;

use anyhow::Result as AnyhowResult;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

use crate::domain::common::passthrough_headers::PassthroughHeaders;
use crate::domain::common::url::Url;
use crate::domain::nar_file::model::NarFileLocation;

#[async_trait]
pub trait NarStreamProvider: Send + Sync {
    async fn stream_nar(
        &self,
        locations: &[NarFileLocation],
        headers: &PassthroughHeaders,
    ) -> (
        AnyhowResult<Option<NarStreamData>>,
        Vec<NarStreamOpenAttempt>,
    );

    /// Like `stream_nar`, but the returned stream starts at byte `offset` of the object
    /// instead of byte 0 (0 behaves the same as `stream_nar`). Candidates race in parallel.
    /// A winning response is accepted only when it actually starts at `offset`; servers
    /// that ignore the `Range` header and answer with a full body are reported through
    /// `NarStreamOpenAttempt::RangeUnsupported` instead of being returned.
    async fn stream_nar_from_offset(
        &self,
        locations: &[NarFileLocation],
        offset: u64,
        headers: &PassthroughHeaders,
    ) -> (
        AnyhowResult<Option<NarStreamData>>,
        Vec<NarStreamOpenAttempt>,
    );
}

pub struct NarStreamData {
    pub headers: NarStreamHeaders,
    pub inner: Pin<Box<dyn Stream<Item = AnyhowResult<Bytes>> + Send>>,
    pub source_url: Url,
}

impl NarStreamData {
    pub fn new(
        headers: NarStreamHeaders,
        inner: Pin<Box<dyn Stream<Item = AnyhowResult<Bytes>> + Send>>,
        source_url: Url,
    ) -> Self {
        Self {
            headers,
            inner,
            source_url,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NarStreamHeaders {
    pub content_length: Option<u64>,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NarStreamOpenAttempt {
    Successful {
        source_url: Url,
    },
    Offline {
        source_url: Url,
    },
    ServiceError {
        source_url: Url,
    },
    /// The substituter answered the range request with a response that does not start at the
    /// requested offset (typically a full 200 body because it ignores `Range` headers), so it
    /// cannot be used for resuming a partial download.
    RangeUnsupported {
        source_url: Url,
    },
}

impl NarStreamOpenAttempt {
    pub fn source_url(&self) -> &Url {
        match self {
            Self::Successful { source_url } => source_url,
            Self::Offline { source_url } => source_url,
            Self::ServiceError { source_url } => source_url,
            Self::RangeUnsupported { source_url } => source_url,
        }
    }
}
