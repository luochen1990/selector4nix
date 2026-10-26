use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result as AnyhowResult;
use bytes::Bytes;
use futures::Stream;

use crate::application::actor::nar_file::{NarFileActorRegistry, NarFileRequest};
use crate::application::actor::substituter::{SubstituterActorRegistry, SubstituterRequest};
use crate::domain::common::passthrough_headers::PassthroughHeaders;
use crate::domain::common::query_parameters::QueryParameters;
use crate::domain::nar_file::StreamNarFileEvent;
use crate::domain::nar_file::model::NarFileKey;
use crate::domain::nar_file::port::NarStreamData;
use crate::domain::nar_info::NarInfoRepository;
use crate::domain::nar_info::model::StorePathHash;
use crate::infrastructure::metric::{NarTransferHandle, NarTransferMeta, NarTransferMetric};
use crate::infrastructure::nar_cache::{NarDiskCache, OpenUpstream};
use crate::{AppError, AppResultExt};

pub struct StreamNarFileUseCase {
    substituter_registry: Arc<SubstituterActorRegistry>,
    nar_file_registry: Arc<NarFileActorRegistry>,
    nar_info_repository: Arc<dyn NarInfoRepository>,
    nar_transfer_metric: Arc<NarTransferMetric>,
    nar_disk_cache: Arc<NarDiskCache>,
}

impl StreamNarFileUseCase {
    pub fn new(
        substituter_registry: Arc<SubstituterActorRegistry>,
        nar_file_registry: Arc<NarFileActorRegistry>,
        nar_info_repository: Arc<dyn NarInfoRepository>,
        nar_transfer_metric: Arc<NarTransferMetric>,
        nar_disk_cache: Arc<NarDiskCache>,
    ) -> Self {
        Self {
            substituter_registry,
            nar_file_registry,
            nar_info_repository,
            nar_transfer_metric,
            nar_disk_cache,
        }
    }

    pub async fn run(
        &self,
        key: NarFileKey,
        upstream_query: Option<QueryParameters>,
        headers: PassthroughHeaders,
    ) -> Result<NarStreamData, AppError> {
        tracing::info!(nar_file = %key.to_file_name().value(), "acquiring nar stream from substituter");

        // With the disk cache disabled the flow below is byte-for-byte the original
        // streaming path (spec §3 "disabled" branch).
        if self.nar_disk_cache.is_enabled() {
            return self.run_with_cache(key, upstream_query, headers).await;
        }

        let address = self.nar_file_registry.get(&key).await;

        let response = address
            .ask(|reply_to| NarFileRequest::StreamNarFile {
                reply_to,
                upstream_query,
                headers,
            })
            .await
            .throw_catastrophic("`NarFileActor` terminated unexpectedly")?;

        Self::exec_events(&self.substituter_registry, response.events).await;

        let result = response
            .result
            .inspect(|result| tracing::info!(nar_file = %key.to_file_name().value(), source_url = %result.stream.source_url, substituter = %result.substituter.url(), "streamed nar from substituter"))
            .inspect_err(|err| tracing::warn!(nar_file = %key.to_file_name().value(), %err, "failed to stream nar"))?;

        Ok(instrument_stream(
            &self.nar_transfer_metric,
            NarTransferMeta {
                nar_file_name: key.to_file_name(),
                store_path: self.query_store_path(result.store_path_hash.as_ref()).await,
                substituter_url: result.substituter.url().clone(),
                source_url: result.stream.source_url.clone(),
                content_length: result.stream.headers.content_length,
            },
            result.stream,
        ))
    }

    /// Cache-enabled route (spec §3): the facade decides between bypass / miss / partial
    /// hit / complete and hands back whichever stream it produced.
    async fn run_with_cache(
        &self,
        key: NarFileKey,
        upstream_query: Option<QueryParameters>,
        headers: PassthroughHeaders,
    ) -> Result<NarStreamData, AppError> {
        let open_upstream =
            self.open_upstream(key.clone(), upstream_query.clone(), headers.clone());
        let served = self
            .nar_disk_cache
            .serve(key.clone(), upstream_query, headers, open_upstream)
            .await?;

        // Transfer metrics for cache-served NARs report the synthetic cache URL (or the
        // real upstream URL on bypass/miss) as both source and substituter label — the
        // concrete substituter is internal to the facade.
        Ok(instrument_stream(
            &self.nar_transfer_metric,
            NarTransferMeta {
                nar_file_name: key.to_file_name(),
                store_path: self.query_store_path(served.store_path_hash.as_ref()).await,
                substituter_url: served.data.source_url.clone(),
                source_url: served.data.source_url.clone(),
                content_length: served.data.headers.content_length,
            },
            served.data,
        ))
    }

    /// The repeatable real-time opener consumed by the cache facade: one actor round per
    /// call, exactly like the uncached path (query, headers and events included).
    fn open_upstream(
        &self,
        key: NarFileKey,
        upstream_query: Option<QueryParameters>,
        headers: PassthroughHeaders,
    ) -> OpenUpstream {
        let nar_file_registry = Arc::clone(&self.nar_file_registry);
        let substituter_registry = Arc::clone(&self.substituter_registry);
        Arc::new(move || {
            let nar_file_registry = Arc::clone(&nar_file_registry);
            let substituter_registry = Arc::clone(&substituter_registry);
            let key = key.clone();
            let upstream_query = upstream_query.clone();
            let headers = headers.clone();
            Box::pin(async move {
                let address = nar_file_registry.get(&key).await;
                let response = address
                    .ask(|reply_to| NarFileRequest::StreamNarFile {
                        reply_to,
                        upstream_query,
                        headers,
                    })
                    .await
                    .throw_catastrophic("`NarFileActor` terminated unexpectedly")?;
                Self::exec_events(&substituter_registry, response.events).await;
                response.result.map(|result| result.stream)
            })
        })
    }

    async fn query_store_path(&self, hash: Option<&StorePathHash>) -> Option<String> {
        let nar_info = self.nar_info_repository.get(hash?).await.ok().flatten()?;
        nar_info
            .nar_info()
            .and_then(|data| data.store_path().map(ToString::to_string))
    }

    async fn exec_events(
        registry: &Arc<SubstituterActorRegistry>,
        events: Vec<StreamNarFileEvent>,
    ) {
        for event in events {
            Self::exec_event(registry, event).await;
        }
    }

    async fn exec_event(registry: &Arc<SubstituterActorRegistry>, event: StreamNarFileEvent) {
        match event {
            StreamNarFileEvent::SubstituterSucceeded(url) => {
                let sender = registry.get(&url).await;
                let _ = sender.tell(SubstituterRequest::ServiceSuccessful).await;
            }
            StreamNarFileEvent::SubstituterOffline(url) => {
                let sender = registry.get(&url).await;
                let _ = sender.tell(SubstituterRequest::ServiceOffline).await;
            }
            StreamNarFileEvent::SubstituterError(url) => {
                let sender = registry.get(&url).await;
                let _ = sender.tell(SubstituterRequest::ServiceError).await;
            }
        }
    }
}

fn instrument_stream(
    metric: &Arc<NarTransferMetric>,
    meta: NarTransferMeta,
    data: NarStreamData,
) -> NarStreamData {
    let stream = Box::pin(InstrumentedNarStream {
        inner: data.inner,
        handle: metric.begin(meta),
    });
    NarStreamData::new(data.headers, stream, data.source_url)
}

struct InstrumentedNarStream {
    inner: Pin<Box<dyn Stream<Item = AnyhowResult<Bytes>> + Send>>,
    handle: NarTransferHandle,
}

impl Stream for InstrumentedNarStream {
    type Item = AnyhowResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                self.handle.record_bytes(bytes.len() as u64);
                Poll::Ready(Some(Ok(bytes)))
            }
            other => other,
        }
    }
}
