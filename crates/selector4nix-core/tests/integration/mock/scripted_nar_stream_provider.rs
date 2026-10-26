//! Scripted NAR stream provider for the nar-disk-cache integration tests.
//!
//! Each substituter source URL is bound to a [`ScriptedBody`]; every ranged/plain open is
//! recorded as one [`CallRecord`] (candidate URLs in order + requested offset) so tests can
//! assert which substituters were consulted and from which watermark.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result as AnyhowResult;
use async_trait::async_trait;
use bytes::Bytes;
use selector4nix_core::domain::common::passthrough_headers::PassthroughHeaders;
use selector4nix_core::domain::common::url::Url;
use selector4nix_core::domain::nar_file::model::NarFileLocation;
use selector4nix_core::domain::nar_file::port::{
    NarStreamData, NarStreamHeaders, NarStreamOpenAttempt, NarStreamProvider,
};

/// What one source URL answers.
#[derive(Clone)]
pub enum ScriptedBody {
    /// Serve these bytes; ranged requests receive `bytes[offset..]`.
    Bytes(Arc<Vec<u8>>),
    /// Never yield a chunk nor end (an upstream that stalls).
    Hang,
    /// Answer range requests with a full 200 body (ignores `Range`).
    RangeIgnored,
}

/// One recorded provider call: the candidate source URLs in the order they were passed in,
/// plus the byte offset the fill started from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallRecord {
    pub candidates: Vec<Url>,
    pub offset: u64,
}

pub struct ScriptedNarStreamProvider {
    scripts: Mutex<HashMap<Url, ScriptedBody>>,
    calls: Mutex<Vec<CallRecord>>,
}

impl ScriptedNarStreamProvider {
    /// An empty provider; scripts are bound afterwards via `set_script` (the exact source
    /// URLs are only known once the test env has built the NAR identity).
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            scripts: Mutex::new(HashMap::new()),
            calls: Mutex::new(Vec::new()),
        })
    }

    /// Bind or rebind a source URL's script after construction (test envs learn the exact
    /// source URLs only after building the NAR identity).
    pub fn set_script(&self, url: Url, body: ScriptedBody) {
        self.scripts.lock().unwrap().insert(url, body);
    }

    pub fn calls(&self) -> Vec<CallRecord> {
        self.calls.lock().unwrap().clone()
    }
}

impl ScriptedNarStreamProvider {
    fn open(
        &self,
        locations: &[NarFileLocation],
        offset: u64,
    ) -> (
        AnyhowResult<Option<NarStreamData>>,
        Vec<NarStreamOpenAttempt>,
    ) {
        self.calls.lock().unwrap().push(CallRecord {
            candidates: locations
                .iter()
                .map(|loc| loc.source_url().clone())
                .collect(),
            offset,
        });

        let empty_headers = NarStreamHeaders {
            content_length: None,
            content_type: None,
            content_encoding: None,
        };
        let mut attempts = Vec::new();
        let scripts = self.scripts.lock().unwrap();
        for loc in locations {
            let Some(body) = scripts.get(loc.source_url()) else {
                continue;
            };
            match body {
                ScriptedBody::Bytes(bytes) => {
                    attempts.push(NarStreamOpenAttempt::Successful {
                        source_url: loc.source_url().clone(),
                    });
                    let from = (offset as usize).min(bytes.len());
                    let chunk = Bytes::from(bytes[from..].to_vec());
                    let headers = NarStreamHeaders {
                        content_length: Some((bytes.len() - from) as u64),
                        content_type: None,
                        content_encoding: None,
                    };
                    return (
                        Ok(Some(NarStreamData::new(
                            headers,
                            Box::pin(futures::stream::iter(vec![Ok(chunk)])),
                            loc.source_url().clone(),
                        ))),
                        attempts,
                    );
                }
                ScriptedBody::Hang => {
                    attempts.push(NarStreamOpenAttempt::Successful {
                        source_url: loc.source_url().clone(),
                    });
                    return (
                        Ok(Some(NarStreamData::new(
                            empty_headers,
                            Box::pin(futures::stream::pending()),
                            loc.source_url().clone(),
                        ))),
                        attempts,
                    );
                }
                ScriptedBody::RangeIgnored => {
                    attempts.push(NarStreamOpenAttempt::RangeUnsupported {
                        source_url: loc.source_url().clone(),
                    });
                }
            }
        }
        (Ok(None), attempts)
    }
}

#[async_trait]
impl NarStreamProvider for ScriptedNarStreamProvider {
    async fn stream_nar(
        &self,
        locations: &[NarFileLocation],
        _headers: &PassthroughHeaders,
    ) -> (
        AnyhowResult<Option<NarStreamData>>,
        Vec<NarStreamOpenAttempt>,
    ) {
        self.open(locations, 0)
    }

    async fn stream_nar_from_offset(
        &self,
        locations: &[NarFileLocation],
        offset: u64,
        _headers: &PassthroughHeaders,
    ) -> (
        AnyhowResult<Option<NarStreamData>>,
        Vec<NarStreamOpenAttempt>,
    ) {
        self.open(locations, offset)
    }
}

/// Builds a repeatable `OpenUpstream` closure handing out scripted chunk streams. Each call
/// produces an independent stream of `chunks` (the miss path may open several rounds).
pub fn chunked_upstream(
    chunks: Vec<Vec<u8>>,
    source_url: Url,
) -> selector4nix_core::infrastructure::nar_cache::OpenUpstream {
    let headers = NarStreamHeaders {
        content_length: None,
        content_type: None,
        content_encoding: None,
    };
    Arc::new(move || {
        let chunks = chunks.clone();
        let source_url = source_url.clone();
        let headers = headers.clone();
        Box::pin(async move {
            Ok(NarStreamData::new(
                headers,
                Box::pin(futures::stream::iter(
                    chunks.into_iter().map(|c| Ok(Bytes::from(c))),
                )),
                source_url,
            ))
        })
    })
}
