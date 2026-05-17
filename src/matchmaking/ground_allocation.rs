//! Ground allocation algorithm.

use std::cmp::min;
use std::time::Duration;

use crate::matchmaking::scheduling::{Assigned, Completed};
use crate::matchmaking::utils::{OffsetIndexedSlice, now};
use crate::validator::validator::MatchmakingAssignment;

/// Ground allocator.
/// Corresponds to a single ground allocation session.
pub struct GroundAllocator {
    ticket: Assigned,
}

impl GroundAllocator {
    pub fn new(ticket: Assigned) -> GroundAllocator {
        GroundAllocator { ticket }
    }

    /// Runs the ground allocation algorithm.
    pub fn run_allocation(mut self) -> (Vec<MatchmakingAssignment>, Completed) {
        let Assigned {
            ticket,
            ground_allocation_control_signals,
            lease,
        } = &mut self.ticket;

        let mut successful_matchings = 0;
        let mut furthest_bucket_distance = 0;

        let mut matchings = Vec::new();
        let mut drained_counts = ground_allocation_control_signals
            .window_eligible_request_counts
            .map(|_| 0);

        // allocate for anchor bin first
        // compute number of possible complete matchings for anchor bin
        let anchor_local_allocations = min(
            ground_allocation_control_signals.maximum_matchings,
            ground_allocation_control_signals
                .get_eligible_request_count(0)
                .unwrap()
                / ground_allocation_control_signals.requests_per_matching,
        );

        // batch allocate from anchor bin
        for _ in 0..anchor_local_allocations {
            let mut matching =
                Vec::with_capacity(ground_allocation_control_signals.requests_per_matching);
            for _ in 0..ground_allocation_control_signals.requests_per_matching {
                matching.push(lease.try_pop(0).unwrap());
            }
            matchings.push(MatchmakingAssignment {
                grouped_requests: matching,
            });
        }

        // update bookkeeping
        successful_matchings += anchor_local_allocations;
        *drained_counts.get_offset_mut(0).unwrap() +=
            anchor_local_allocations * ground_allocation_control_signals.requests_per_matching;
        *ground_allocation_control_signals
            .window_eligible_request_counts
            .get_offset_mut(0)
            .unwrap() -= drained_counts.get_offset(0).unwrap();

        // allocate using full window, starting from anchor
        // verify that there are sufficient requests and room for precisely one more
        // matching
        if successful_matchings < ground_allocation_control_signals.maximum_matchings
            && ground_allocation_control_signals
                .window_eligible_request_counts
                .iter()
                .sum::<usize>()
                >= ground_allocation_control_signals.requests_per_matching
        {
            // alternate between positive and negative edge of expanding window
            let mut direction = 1;
            let mut current_absolute_offset = 0;
            let mut matching =
                Vec::with_capacity(ground_allocation_control_signals.requests_per_matching);
            let mut failed_at_current_offset = false;

            while matching.len() < ground_allocation_control_signals.requests_per_matching
                && current_absolute_offset <= ticket.delta
            {
                let offset = current_absolute_offset as i32 * direction;
                match lease.try_pop(offset) {
                    Some(request) => {
                        matching.push(request);
                        *drained_counts.get_offset_mut(offset).unwrap() += 1;
                        direction *= -1;
                        failed_at_current_offset = false;
                    }
                    None => {
                        if !failed_at_current_offset {
                            direction *= -1;
                            failed_at_current_offset = true;
                        } else {
                            current_absolute_offset += 1;
                            direction = 1;
                            failed_at_current_offset = false;
                        }
                    }
                }
            }

            // allocate
            matchings.push(MatchmakingAssignment {
                grouped_requests: matching,
            });
            successful_matchings += 1;
            furthest_bucket_distance = current_absolute_offset;
        }

        let summary = GroundAllocationSessionSummary {
            successful_matchings,
            furthest_bucket_distance,
            completion_timestamp: now(),
            drained_counts,
        };

        (matchings, self.ticket.complete(summary))
    }
}

/// Ground allocation control signals, as determined by the scheduler.
pub struct GroundAllocationControlSignals {
    pub maximum_matchings: usize,
    pub requests_per_matching: usize,
    pub window_eligible_request_counts: OffsetIndexedSlice<usize>,
}

impl GroundAllocationControlSignals {
    pub fn get_eligible_request_count(&self, offset: i32) -> Option<usize> {
        self.window_eligible_request_counts
            .get_offset(offset)
            .copied()
    }
}

/// Ground allocation session summary statistics.
#[derive(Debug)]
pub struct GroundAllocationSessionSummary {
    pub successful_matchings: usize,
    pub furthest_bucket_distance: usize,
    pub completion_timestamp: Duration,
    pub drained_counts: OffsetIndexedSlice<usize>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::matchmaking::scheduling::leases::Lease;
    use crate::matchmaking::scheduling::request_matrix::{Postbox, RequestMatrix};
    use crate::matchmaking::scheduling::tickets::Ticket;
    use crate::prelude::MatchmakingRequest;

    fn request(request_id: u32, skill_rating: u32) -> MatchmakingRequest {
        MatchmakingRequest {
            request_id,
            skill_rating,
            submission_timestamp: Duration::from_secs(request_id as u64),
        }
    }

    fn populate_bucket(postbox: &mut Postbox, bucket: usize, count: usize) {
        for i in 0..count {
            postbox
                .try_post(bucket, request(i as u32 + 1, 100))
                .unwrap();
        }
    }

    fn make_assigned(
        matrix: &mut RequestMatrix,
        anchor: usize,
        delta: usize,
        eligible: &[usize],
    ) -> Assigned {
        let lease = Lease::try_acquire(matrix, anchor, delta).unwrap();
        let window_eligible_request_counts =
            OffsetIndexedSlice::from_slice(eligible, anchor, delta).unwrap();
        let mut ticket = Ticket::new(anchor);
        ticket.delta = delta;
        Assigned {
            ticket,
            lease,
            ground_allocation_control_signals: GroundAllocationControlSignals {
                maximum_matchings: 1,
                requests_per_matching: 10,
                window_eligible_request_counts,
            },
        }
    }

    #[test]
    fn anchor_local_allocation_produces_full_matches() {
        let (mut matrix, mut postbox) = RequestMatrix::new(3, 20);
        populate_bucket(&mut postbox, 1, 12);

        let assigned = make_assigned(&mut matrix, 1, 0, &[0, 12, 0]);
        let allocator = GroundAllocator::new(assigned);
        let (matchings, completed) = allocator.run_allocation();

        assert_eq!(matchings.len(), 1);
        assert_eq!(matchings[0].grouped_requests.len(), 10);
        assert_eq!(completed.local_feedback.successful_matchings, 1);
        assert_eq!(completed.local_feedback.furthest_bucket_distance, 0);
        assert_eq!(
            *completed
                .local_feedback
                .drained_counts
                .get_offset(0)
                .unwrap(),
            10
        );
    }

    #[test]
    fn anchor_local_allocation_uses_only_anchor_bucket() {
        let (mut matrix, mut postbox) = RequestMatrix::new(5, 30);
        populate_bucket(&mut postbox, 2, 20);
        populate_bucket(&mut postbox, 1, 20);
        populate_bucket(&mut postbox, 3, 20);

        let assigned = make_assigned(&mut matrix, 2, 1, &[0, 20, 20, 20, 0]);
        let allocator = GroundAllocator::new(assigned);
        let (matchings, completed) = allocator.run_allocation();

        assert_eq!(matchings.len(), 1);
        assert_eq!(matchings[0].grouped_requests.len(), 10);
        assert_eq!(completed.local_feedback.successful_matchings, 1);
        assert_eq!(completed.local_feedback.furthest_bucket_distance, 0);
        assert_eq!(
            *completed
                .local_feedback
                .drained_counts
                .get_offset(0)
                .unwrap(),
            10
        );
        assert_eq!(
            *completed
                .local_feedback
                .drained_counts
                .get_offset(-1)
                .unwrap(),
            0
        );
        assert_eq!(
            *completed
                .local_feedback
                .drained_counts
                .get_offset(1)
                .unwrap(),
            0
        );
    }

    #[test]
    fn widening_when_anchor_insufficient_for_full_match() {
        let (mut matrix, mut postbox) = RequestMatrix::new(5, 20);
        populate_bucket(&mut postbox, 2, 4);
        populate_bucket(&mut postbox, 1, 4);
        populate_bucket(&mut postbox, 3, 4);

        let assigned = make_assigned(&mut matrix, 2, 2, &[0, 4, 4, 4, 0]);
        let allocator = GroundAllocator::new(assigned);
        let (matchings, completed) = allocator.run_allocation();

        assert_eq!(matchings.len(), 1);
        assert_eq!(matchings[0].grouped_requests.len(), 10);
        assert_eq!(completed.local_feedback.successful_matchings, 1);
        assert!(completed.local_feedback.furthest_bucket_distance >= 1);
    }

    #[test]
    fn no_widening_when_budget_exhausted_on_anchor_locally() {
        let (mut matrix, mut postbox) = RequestMatrix::new(3, 30);
        populate_bucket(&mut postbox, 1, 25);

        let mut assigned = make_assigned(&mut matrix, 1, 0, &[0, 25, 0]);
        assigned.ground_allocation_control_signals.maximum_matchings = 2;
        let allocator = GroundAllocator::new(assigned);
        let (matchings, completed) = allocator.run_allocation();

        assert_eq!(matchings.len(), 2);
        assert_eq!(completed.local_feedback.successful_matchings, 2);
        assert_eq!(completed.local_feedback.furthest_bucket_distance, 0);
    }

    #[test]
    fn insufficient_requests_anywhere_produces_no_match() {
        let (mut matrix, mut postbox) = RequestMatrix::new(3, 10);
        populate_bucket(&mut postbox, 1, 5);

        let assigned = make_assigned(&mut matrix, 1, 1, &[0, 5, 0]);
        let allocator = GroundAllocator::new(assigned);
        let (matchings, completed) = allocator.run_allocation();

        assert!(matchings.is_empty());
        assert_eq!(completed.local_feedback.successful_matchings, 0);
        assert_eq!(completed.local_feedback.furthest_bucket_distance, 0);
    }

    #[test]
    fn furthest_bucket_distance_reported_after_widening() {
        let (mut matrix, mut postbox) = RequestMatrix::new(7, 20);
        populate_bucket(&mut postbox, 3, 4);
        populate_bucket(&mut postbox, 5, 8);

        let assigned = make_assigned(&mut matrix, 3, 2, &[0, 0, 0, 4, 0, 8, 0]);
        let allocator = GroundAllocator::new(assigned);
        let (_matchings, completed) = allocator.run_allocation();

        assert_eq!(completed.local_feedback.successful_matchings, 1);
        assert_eq!(completed.local_feedback.furthest_bucket_distance, 2);
    }

    #[test]
    fn drained_counts_accumulate_across_anchor_and_widening() {
        let (mut matrix, mut postbox) = RequestMatrix::new(5, 20);
        populate_bucket(&mut postbox, 2, 4);
        populate_bucket(&mut postbox, 1, 8);

        let assigned = make_assigned(&mut matrix, 2, 1, &[0, 8, 4, 0, 0]);
        let allocator = GroundAllocator::new(assigned);
        let (_matchings, completed) = allocator.run_allocation();

        assert_eq!(completed.local_feedback.successful_matchings, 1);
        let anchor_drained = *completed
            .local_feedback
            .drained_counts
            .get_offset(0)
            .unwrap();
        let widened_drained = *completed
            .local_feedback
            .drained_counts
            .get_offset(-1)
            .unwrap();
        assert_eq!(anchor_drained + widened_drained, 10);
    }

    #[test]
    fn allocation_returns_ticket_with_remaining_data() {
        let (mut matrix, mut postbox) = RequestMatrix::new(3, 20);
        populate_bucket(&mut postbox, 1, 20);

        let assigned = make_assigned(&mut matrix, 1, 0, &[0, 20, 0]);
        let allocator = GroundAllocator::new(assigned);
        let (_matchings, completed) = allocator.run_allocation();

        let remaining = completed.lease.as_ref().unwrap().try_peek(0).unwrap();
        assert_eq!(remaining.request_id, 11);
    }

    #[test]
    fn completion_timestamp_is_set_after_allocation() {
        let (mut matrix, mut postbox) = RequestMatrix::new(1, 20);
        populate_bucket(&mut postbox, 0, 10);

        let assigned = make_assigned(&mut matrix, 0, 0, &[10]);
        let allocator = GroundAllocator::new(assigned);
        let (_matchings, completed) = allocator.run_allocation();

        assert!(completed.local_feedback.completion_timestamp > Duration::ZERO);
    }

    #[test]
    fn multi_match_run_produces_only_one_cross_bucket_match() {
        let (mut matrix, mut postbox) = RequestMatrix::new(3, 30);
        populate_bucket(&mut postbox, 0, 25);
        populate_bucket(&mut postbox, 1, 10);

        let mut assigned = make_assigned(&mut matrix, 0, 1, &[25, 10, 0]);
        assigned.ground_allocation_control_signals.maximum_matchings = 3;

        let allocator = GroundAllocator::new(assigned);
        let (matchings, completed) = allocator.run_allocation();

        // 2 anchor-local + 1 cross-bucket = 3
        assert_eq!(matchings.len(), 3);
        assert_eq!(completed.local_feedback.successful_matchings, 3);
        assert!(completed.local_feedback.furthest_bucket_distance > 0);
    }
}
