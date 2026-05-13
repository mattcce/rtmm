//! Ground allocation algorithm.

use std::cmp::min;
use std::time::SystemTime;

use crate::matchmaking::scheduling::Assigned;
use crate::matchmaking::utils::OffsetIndexedSlice;
use crate::validator::validator::MatchmakingAssignment;

/// Ground allocator.
/// Corresponds to a single ground allocation session.
pub struct GroundAllocator<'a> {
    ticket: Assigned<'a>,
    control_signals: GroundAllocationControlSignals,
}

impl<'a> GroundAllocator<'a> {
    pub fn new(
        ticket: Assigned<'a>,
        control_signals: GroundAllocationControlSignals,
    ) -> GroundAllocator<'a> {
        GroundAllocator {
            ticket,
            control_signals,
        }
    }

    /// Runs the ground allocation algorithm.
    pub fn run_allocation(
        mut self,
    ) -> (
        Vec<MatchmakingAssignment>,
        Assigned<'a>,
        GroundAllocationSessionSummary,
    ) {
        let mut successful_matchings = 0;
        let mut furthest_bucket_distance = 0;

        let mut matchings = Vec::new();
        let mut drained_counts = self
            .control_signals
            .window_eligible_request_counts
            .map(|_| 0);

        // allocate for anchor bin first
        let anchor_local_allocations = min(
            self.control_signals.maximum_matchings,
            self.control_signals.get_eligible_request_count(0).unwrap()
                / self.control_signals.requests_per_matching,
        );
        for _ in 0..anchor_local_allocations {
            let mut matching = Vec::with_capacity(self.control_signals.requests_per_matching);
            for _ in 0..self.control_signals.requests_per_matching {
                matching.push(self.ticket.lease.try_pop(0).unwrap());
            }
            matchings.push(MatchmakingAssignment {
                grouped_requests: matching,
            });
        }
        successful_matchings += anchor_local_allocations;
        *drained_counts.get_offset_mut(0).unwrap() +=
            anchor_local_allocations * self.control_signals.requests_per_matching;
        *self
            .control_signals
            .window_eligible_request_counts
            .get_offset_mut(0)
            .unwrap() -= drained_counts.get_offset(0).unwrap();

        // allocate using window, starting from anchor
        // verify that there are sufficient requests and room for one more matching
        if successful_matchings < self.control_signals.maximum_matchings
            && self
                .control_signals
                .window_eligible_request_counts
                .iter()
                .sum::<usize>()
                >= self.control_signals.requests_per_matching
        {
            let mut direction = 1;
            let mut current_absolute_offset = 0;
            let mut matching = Vec::with_capacity(self.control_signals.requests_per_matching);
            let mut last_seen_was_failure = false;

            while matching.len() < self.control_signals.requests_per_matching
                && current_absolute_offset <= self.ticket.delta
            {
                let offset = current_absolute_offset as i32 * direction;
                match self.ticket.lease.try_pop(offset) {
                    Some(request) => {
                        matching.push(request);
                        *drained_counts.get_offset_mut(offset).unwrap() += 1;
                        direction *= -1;
                    }
                    None => {
                        if last_seen_was_failure {
                            last_seen_was_failure = true;
                        } else {
                            current_absolute_offset += 1;
                            direction = 1;
                            last_seen_was_failure = false;
                        }
                    }
                }
            }

            matchings.push(MatchmakingAssignment {
                grouped_requests: matching,
            });
            successful_matchings += 1;
            furthest_bucket_distance = current_absolute_offset;
        }

        let summary = GroundAllocationSessionSummary {
            successful_matchings,
            furthest_bucket_distance,
            completion_timestamp: SystemTime::now(),
            drained_counts,
        };

        (matchings, self.ticket, summary)
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
    pub completion_timestamp: SystemTime,
    pub drained_counts: OffsetIndexedSlice<usize>,
}
