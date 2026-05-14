//! Leases containing consumer heads for necessary buckets and other bookkeeping
//! information.

use crate::matchmaking::scheduling::request_matrix::{LeaseError, RequestBucket, RequestMatrix};
use crate::matchmaking::utils::OffsetIndexedSlice;
use crate::prelude::MatchmakingRequest;

pub struct Lease {
    buckets: OffsetIndexedSlice<RequestBucket>,
}

impl Lease {
    pub fn try_acquire(
        request_matrix: &mut RequestMatrix,
        anchor_index: usize,
        delta: usize,
    ) -> Result<Lease, LeaseError> {
        let guards = request_matrix.try_lease_window(anchor_index, delta)?;

        Ok(Lease { buckets: guards })
    }

    pub fn try_peek(&self, offset: i32) -> Option<&MatchmakingRequest> {
        self.buckets.get_offset(offset)?.try_peek()
    }

    pub fn try_pop(&mut self, offset: i32) -> Option<MatchmakingRequest> {
        self.buckets.get_offset_mut(offset)?.try_pop()
    }

    pub fn into_buckets(self) -> OffsetIndexedSlice<RequestBucket> {
        self.buckets
    }
}
