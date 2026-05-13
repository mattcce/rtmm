//! Leases containing consumer heads for necessary buckets and other bookkeeping
//! information.

use parking_lot::MutexGuard;

use crate::matchmaking::scheduling::request_matrix::{LeaseError, RequestBucket, RequestMatrix};
use crate::matchmaking::utils::OffsetIndexedSlice;
use crate::prelude::MatchmakingRequest;

pub struct Lease<'b> {
    guards: OffsetIndexedSlice<MutexGuard<'b, RequestBucket>>,
}

impl Lease<'_> {
    pub fn try_acquire<'a>(
        request_matrix: &'a RequestMatrix,
        anchor_index: usize,
        delta: usize,
    ) -> Result<Lease<'a>, LeaseError> {
        let guards = request_matrix.try_lease_window(anchor_index, delta)?;

        Ok(Lease { guards })
    }

    pub fn try_peek(&self, offset: i32) -> Option<&MatchmakingRequest> {
        self.guards.get_offset(offset)?.try_peek()
    }

    pub fn try_pop(&mut self, offset: i32) -> Option<MatchmakingRequest> {
        self.guards.get_offset_mut(offset)?.try_pop()
    }
}
