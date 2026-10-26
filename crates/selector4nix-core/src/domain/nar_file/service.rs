use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::domain::common::expire_at::ExpireAt;
use crate::domain::common::passthrough_headers::PassthroughHeaders;
use crate::domain::common::query_parameters::QueryParameters;
use crate::domain::common::url::Url;
use crate::domain::nar_file::model::{NarFile, NarFileLocation};
use crate::domain::nar_file::port::{NarStreamData, NarStreamOpenAttempt, NarStreamProvider};
use crate::domain::nar_info::model::NarFileName;
use crate::domain::substituter::{SubstituterCandidate, SubstituterRepository};
use crate::{AppError, AppResultExt};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StreamNarFileEvent {
    SubstituterSucceeded(Url),
    SubstituterOffline(Url),
    SubstituterError(Url),
}

pub struct NarFileService {
    nar_stream_provider: Arc<dyn NarStreamProvider>,
    substituter_repository: Arc<dyn SubstituterRepository>,
    nar_file_ttl: Duration,
}

/// Build one candidate location per substituter for the given NAR file name.
///
/// Since the NAR file name is concatenated onto each substituter's storage URL manually, the
/// original query parameters (if any) are lost, so they are appended back. These parameters
/// may only make sense to a portion of substituters, but it should have no impact on other
/// substituters, so they are appended unconditionally.
pub fn build_nar_file_locations(
    substituters: &[SubstituterCandidate],
    nar_file_name: &NarFileName,
    upstream_query: Option<&QueryParameters>,
) -> Vec<NarFileLocation> {
    substituters
        .iter()
        .map(|sub| {
            let source_url = nar_file_name
                .with_storage_prefix(sub.meta().storage_url())
                .with_query_params(upstream_query);
            let timeout = sub.meta().nar_timeout();
            NarFileLocation::new(source_url, sub.meta().clone(), timeout)
        })
        .collect()
}

impl NarFileService {
    pub fn new(
        nar_stream_provider: Arc<dyn NarStreamProvider>,
        substituter_repository: Arc<dyn SubstituterRepository>,
        nar_file_ttl: Duration,
    ) -> Self {
        Self {
            nar_stream_provider,
            substituter_repository,
            nar_file_ttl,
        }
    }

    pub async fn stream(
        &self,
        nar_file: NarFile,
        upstream_query: Option<QueryParameters>,
        headers: PassthroughHeaders,
        now: SystemTime,
    ) -> (
        NarFile,
        Result<NarStreamData, AppError>,
        Vec<StreamNarFileEvent>,
    ) {
        let nar_file_name = nar_file.key().to_file_name();

        if let Some(location) = nar_file.location() {
            // Don't send requests to selected substituter that is unavailable and do fallback
            // early. Always avoid short-circuit if `{url}/nar != storage_url`, because we only
            // probe the health information of host of `url` and can't determine `storage_url`'s
            // status.
            if location.substituter().has_custom_storage_url()
                || self
                    .substituter_repository
                    .exists_available(location.substituter().url())
                    .await
            {
                tracing::trace!(nar_file = %nar_file_name.value(), source_url = %location.source_url(), "use cached nar file location");

                // `upstream_query` is unused here because `source_url` in `location` already
                // contains those query parameters.
                let locations = [location.clone()];

                let (outcome, attempts) = self
                    .nar_stream_provider
                    .stream_nar(&locations, &headers)
                    .await;

                if let Ok(Some(data)) = outcome {
                    let events = Self::map_attempts_to_events(&locations, &attempts);
                    return (nar_file, Ok(data), events);
                }
            }

            tracing::trace!(nar_file = %nar_file_name.value(), "fallback to query all substituters for nar file location");
        } else {
            tracing::trace!(nar_file = %nar_file_name.value(), "query all substituters for nar file location");
        }

        let candidates = self
            .build_candidates_from_all(&nar_file_name, upstream_query)
            .await;
        self.stream_from_all(nar_file, headers, candidates, now)
            .await
    }

    async fn stream_from_all(
        &self,
        nar_file: NarFile,
        headers: PassthroughHeaders,
        candidates: Vec<NarFileLocation>,
        now: SystemTime,
    ) -> (
        NarFile,
        Result<NarStreamData, AppError>,
        Vec<StreamNarFileEvent>,
    ) {
        let (outcome, attempts) = self
            .nar_stream_provider
            .stream_nar(&candidates, &headers)
            .await;
        let events = Self::map_attempts_to_events(&candidates, &attempts);

        match outcome {
            Ok(Some(data)) => {
                let location = candidates
                    .into_iter()
                    .find(|loc| loc.source_url() == &data.source_url)
                    .expect("returned `source_url` should match a candidate");
                let nar_file = match nar_file.location() {
                    Some(_) => nar_file.on_relocated(location),
                    None => {
                        let expire_at = ExpireAt::since(now, self.nar_file_ttl);
                        nar_file.on_located(location, expire_at, None)
                    }
                };
                (nar_file, Ok(data), events)
            }
            Ok(None) => (
                nar_file,
                Err(AppError::not_found(
                    "failed to acquire stream for non-existent nar file",
                )),
                events,
            ),
            Err(err) => (
                nar_file,
                Err(err).chain_infrastructure("failed to acquire nar stream from substituters"),
                events,
            ),
        }
    }

    async fn build_candidates_from_all(
        &self,
        nar_file_name: &NarFileName,
        upstream_query: Option<QueryParameters>,
    ) -> Vec<NarFileLocation> {
        let substituters = self.substituter_repository.query_all_available().await;
        build_nar_file_locations(&substituters, nar_file_name, upstream_query.as_ref())
    }

    fn map_attempts_to_events(
        locations: &[NarFileLocation],
        attempts: &[NarStreamOpenAttempt],
    ) -> Vec<StreamNarFileEvent> {
        locations
            .iter()
            .filter_map(|location| {
                attempts
                    .iter()
                    .find(|attempt| attempt.source_url() == location.source_url())
                    .and_then(|attempt| {
                        let substituter_url = location.substituter().url().clone();
                        match attempt {
                            NarStreamOpenAttempt::Successful { .. } => {
                                Some(StreamNarFileEvent::SubstituterSucceeded(substituter_url))
                            }
                            NarStreamOpenAttempt::Offline { .. } => {
                                Some(StreamNarFileEvent::SubstituterOffline(substituter_url))
                            }
                            NarStreamOpenAttempt::ServiceError { .. } => {
                                Some(StreamNarFileEvent::SubstituterError(substituter_url))
                            }
                            // A range-ignoring server is healthy; its answer just cannot be used
                            // for resuming, so it produces no substituter health event.
                            NarStreamOpenAttempt::RangeUnsupported { .. } => None,
                        }
                    })
            })
            .collect()
    }
}
