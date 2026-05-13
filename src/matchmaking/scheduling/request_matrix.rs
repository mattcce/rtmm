//! Efficient concurrent request bucket queue for use by the matchmaker.

use std::error::Error;
use std::fmt::Display;

use parking_lot::{Mutex, MutexGuard};
use ringbuf::traits::{Consumer, Producer, Split};
use ringbuf::{HeapCons, HeapProd, HeapRb};

use crate::matchmaking::utils::OffsetIndexedSlice;
use crate::prelude::MatchmakingRequest;

/// Request matrix bucketing requests into separate bins.
/// Designed to expose an interface that never allows blocking except for tasks
/// that predictably will not be contended for.
pub struct RequestMatrix {
    buckets: Vec<Mutex<RequestBucket>>,
}

impl RequestMatrix {
    pub fn new(bucket_count: usize, bucket_capacity: usize) -> (RequestMatrix, Postbox) {
        let mut postbox = Vec::with_capacity(bucket_count);
        let mut buckets = Vec::with_capacity(bucket_count);

        for _ in 0..bucket_count {
            let heap_rb: HeapRb<MatchmakingRequest> = HeapRb::new(bucket_capacity);
            let (producer, consumer) = heap_rb.split();
            postbox.push(producer);
            buckets.push(Mutex::new(RequestBucket(consumer)));
        }

        (RequestMatrix { buckets }, Postbox(postbox))
    }

    /// Tries to lease a contiguous section of request buckets.
    /// All or nothing: either the entire range is leased or nothing is leased
    /// at all.
    ///
    /// Implicit invariant: lease admission is serialized by the scheduler
    /// thread. This method therefore only guarantees that failed range
    /// acquisition leaves no prefix bucket leased on return; it is not intended
    /// to provide general multi-caller atomic range locking semantics.
    pub fn try_lease_window<'a>(
        &'a self,
        anchor_index: usize,
        delta: usize,
    ) -> Result<OffsetIndexedSlice<MutexGuard<'a, RequestBucket>>, LeaseError> {
        match OffsetIndexedSlice::try_from_slice_map(
            &self.buckets,
            anchor_index,
            delta,
            |bucket: &'_ Mutex<RequestBucket>| bucket.try_lock(),
        ) {
            Ok(guards) => Ok(guards),
            Err(index) => Err(LeaseError::ContentionError(index)),
        }
    }
}

#[derive(Clone, Debug)]
pub enum LeaseError {
    ContentionError(usize),
}

impl Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ContentionError(indices) => {
                write!(f, "Failed to acquire lock for buckets {:?}.", indices)
            }
        }
    }
}

impl Error for LeaseError {}

#[derive(Clone, Copy, Debug)]
pub enum PostError {
    BucketFull(usize, MatchmakingRequest),
    InvalidIndex(usize),
}

impl Display for PostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BucketFull(i, request) => write!(
                f,
                "Bucket {:?} is full, failed to post request {:?}.",
                i, request
            ),
            Self::InvalidIndex(i) => write!(f, "Bucket at index {:?} does not exist.", i),
        }
    }
}

impl Error for PostError {}

#[repr(transparent)]
pub struct Postbox(Vec<HeapProd<MatchmakingRequest>>);

impl Postbox {
    pub fn try_post(&mut self, index: usize, request: MatchmakingRequest) -> Result<(), PostError> {
        let Some(bucket) = self.0.get_mut(index) else {
            return Err(PostError::InvalidIndex(index));
        };

        if let Err(request) = bucket.try_push(request) {
            return Err(PostError::BucketFull(index, request));
        }

        Ok(())
    }
}

#[repr(transparent)]
pub struct RequestBucket(HeapCons<MatchmakingRequest>);

impl RequestBucket {
    pub fn try_peek(&self) -> Option<&MatchmakingRequest> {
        self.0.try_peek()
    }

    pub fn try_pop(&mut self) -> Option<MatchmakingRequest> {
        self.0.try_pop()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::{LeaseError, PostError, RequestMatrix};
    use crate::prelude::MatchmakingRequest;

    fn request(request_id: u32, skill_rating: u32) -> MatchmakingRequest {
        MatchmakingRequest {
            request_id,
            skill_rating,
            submission_timestamp: UNIX_EPOCH + Duration::from_secs(request_id as u64),
        }
    }

    #[test]
    fn posting_and_popping_preserves_fifo_order() {
        let (request_matrix, mut postbox) = RequestMatrix::new(2, 2);

        postbox.try_post(1, request(1, 100)).unwrap();
        postbox.try_post(1, request(2, 100)).unwrap();

        let mut lease = request_matrix.try_lease_window(1, 0).unwrap();
        let bucket = lease.get_offset_mut(0).unwrap();

        assert_eq!(bucket.try_peek().unwrap().request_id, 1);
        assert_eq!(bucket.try_pop().unwrap().request_id, 1);
        assert_eq!(bucket.try_pop().unwrap().request_id, 2);
        assert!(bucket.try_pop().is_none());
    }

    #[test]
    fn leasing_window_clips_at_queue_edges() {
        let (request_matrix, _) = RequestMatrix::new(3, 1);

        let lease = request_matrix.try_lease_window(0, 2).unwrap();

        assert!(lease.get_offset(-1).is_none());
        assert!(lease.get_offset(0).is_some());
        assert!(lease.get_offset(1).is_some());
        assert!(lease.get_offset(2).is_some());
        assert!(lease.get_offset(3).is_none());
    }

    #[test]
    fn lease_reports_the_blocking_bucket() {
        let (request_matrix, _) = RequestMatrix::new(3, 1);

        let _held = request_matrix.try_lease_window(1, 0).unwrap();

        assert!(matches!(
            request_matrix.try_lease_window(0, 1),
            Err(LeaseError::ContentionError(1))
        ));
    }

    #[test]
    fn postbox_rejects_full_and_invalid_buckets() {
        let (_, mut postbox) = RequestMatrix::new(1, 1);

        postbox.try_post(0, request(1, 100)).unwrap();

        assert!(matches!(
            postbox.try_post(0, request(2, 100)),
            Err(PostError::BucketFull(0, _))
        ));
        assert!(matches!(
            postbox.try_post(2, request(3, 100)),
            Err(PostError::InvalidIndex(2))
        ));
    }
}
