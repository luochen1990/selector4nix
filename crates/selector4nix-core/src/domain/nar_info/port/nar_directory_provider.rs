use anyhow::Result as AnyhowResult;
use async_trait::async_trait;

use crate::domain::common::passthrough_headers::PassthroughHeaders;
use crate::domain::common::url::Url;
use crate::domain::nar_info::model::StorePathHash;
use crate::domain::substituter::model::SubstituterMeta;

#[async_trait]
pub trait NarDirectoryProvider: Send + Sync {
    async fn list(
        &self,
        substituters: &[SubstituterMeta],
        store_path_hash: &StorePathHash,
        headers: &PassthroughHeaders,
    ) -> (
        AnyhowResult<Option<ListDirectoryData>>,
        Vec<ListDirectoryAttempt>,
    );
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListDirectoryData {
    pub content: String,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ListDirectoryAttempt {
    Successful { substituter_url: Url },
    Offline { substituter_url: Url },
    ServiceError { substituter_url: Url },
}
