use std::path::PathBuf;
use std::sync::Arc;

use futures::Future;
use parking_lot::Mutex;

use super::Collection;
use crate::operations::types::CollectionResult;
use crate::shards::replica_set::ReplicaState;
use crate::shards::resharding::tasks_pool::{ReshardTaskItem, ReshardTaskProgress};
use crate::shards::resharding::{self, ReshardKey, ReshardState};
use crate::shards::transfer::ShardTransferConsensus;

impl Collection {
    pub async fn resharding_state(&self) -> Option<ReshardState> {
        self.shards_holder
            .read()
            .await
            .resharding_state
            .read()
            .clone()
    }

    pub async fn start_resharding<T, F>(
        &self,
        reshard_key: ReshardKey,
        consensus: Box<dyn ShardTransferConsensus>,
        temp_dir: PathBuf,
        on_finish: T,
        on_error: F,
    ) -> CollectionResult<()>
    where
        T: Future<Output = ()> + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        let mut shard_holder = self.shards_holder.write().await;

        shard_holder.check_start_resharding(&reshard_key)?;

        let replica_set = self
            .create_replica_set(
                reshard_key.shard_id,
                &[reshard_key.peer_id],
                Some(ReplicaState::Resharding),
            )
            .await?;

        shard_holder.start_resharding_unchecked(reshard_key.clone(), replica_set)?;

        Ok(())
    }

    pub async fn commit_read_hashring(&self, resharding_key: ReshardKey) -> CollectionResult<()> {
        self.shards_holder
            .write()
            .await
            .commit_read_hashring(resharding_key)
    }

    pub async fn commit_write_hashring(&self, resharding_key: ReshardKey) -> CollectionResult<()> {
        self.shards_holder
            .write()
            .await
            .commit_write_hashring(resharding_key)
    }

    pub async fn finish_resharding(&self, resharding_key: ReshardKey) -> CollectionResult<()> {
        self.shards_holder
            .write()
            .await
            .finish_resharding(resharding_key)
    }

    pub async fn abort_resharding(&self, reshard_key: ReshardKey) -> CollectionResult<()> {
        self.shards_holder
            .write()
            .await
            .abort_resharding(reshard_key)
            .await
    }
}
