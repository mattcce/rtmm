//! Global allocation scheduler.

use std::collections::BinaryHeap;

use statrs::distribution::{ContinuousCDF, Normal as NormalDistribution};

use crate::matchmaking::config::MatchmakingQueueParameters;
use crate::matchmaking::ground_allocation::{
    GroundAllocationControlSignals, GroundAllocationSessionSummary,
};
use crate::matchmaking::scheduling::leases::Lease;
use crate::matchmaking::scheduling::policies::{
    SchedulerGlobalFeedback, compute_backoff_duration, compute_target_delta,
};
use crate::matchmaking::scheduling::request_matrix::*;
use crate::matchmaking::scheduling::states::*;
use crate::matchmaking::scheduling::substructures::contention::Contention;
use crate::matchmaking::scheduling::substructures::empty_hold::EmptyHold;
use crate::matchmaking::scheduling::tickets::Ticket;
use crate::matchmaking::utils::{OffsetIndexedSlice, now};
use crate::prelude::MatchmakingRequest;

/// Core scheduling table for a single matchmaking queue.
pub struct SchedulingTable {
    request_matrix: RequestMatrix,
    postbox: Postbox,

    contention_lane: Contention,
    ready_queue: BinaryHeap<ReadyPriority>,
    waiting_lane: BinaryHeap<Waiting>,
    empty_hold: EmptyHold,

    bookkeeping: SchedulerBookkeeping, // local/meso statistics
    queue_parameters: MatchmakingQueueParameters,
    global_feedback: SchedulerGlobalFeedback, // batch-level statistics
}

/// Basic scheduler functionality and utilities.
impl SchedulingTable {
    const RESIDENCY_LIMIT: usize = 300;

    pub fn new(queue_parameters: MatchmakingQueueParameters) -> SchedulingTable {
        let bucket_count = queue_parameters.bucket_count();

        let (request_matrix, postbox) =
            RequestMatrix::new(bucket_count, queue_parameters.bucket_capacity());
        let bucket_request_counts = vec![0; bucket_count].into_boxed_slice();

        let empty_hold = EmptyHold::new_with_issue(bucket_count);

        let bookkeeping = SchedulerBookkeeping {
            bucket_request_counts,
            proportion_table: SchedulingTable::generate_proportion_table(
                queue_parameters.skill_rating_range(),
                queue_parameters.bucket_width(),
                queue_parameters.distribution(),
            ),
            assigned_ticket_count: 0,
        };

        // preallocate maximum sizes for all scheduler-related queues: bounded by total
        // number of tickets
        SchedulingTable {
            request_matrix,
            postbox,

            contention_lane: Contention::new(bucket_count),
            ready_queue: BinaryHeap::with_capacity(bucket_count),
            waiting_lane: BinaryHeap::with_capacity(bucket_count),
            empty_hold,

            bookkeeping,
            global_feedback: SchedulerGlobalFeedback::new(),
            queue_parameters,
        }
    }

    /// Posts a single request. Should not be called independently as it does
    /// not trigger required batch updates.
    fn post_request_raw(&mut self, request: MatchmakingRequest) -> Result<(), PostError> {
        let anchor_bucket_index = self.resolve_anchor_bucket_index(&request);

        self.postbox.try_post(anchor_bucket_index, request)?;
        self.bookkeeping.bucket_request_counts[anchor_bucket_index] += 1;

        // if ticket is empty, update ticket time and move it to ready
        if let Some(mut ticket) = self.empty_hold.take(anchor_bucket_index) {
            ticket.oldest_request_timestamp = Some(request.submission_timestamp);
            self.ready_ticket(ReadyPriority::Normal(ticket.ready()));
        }

        // in all other cases, there is no bookkeeping to be done
        // coherency is logically maintained
        Ok(())
    }

    /// Posts an entire batch of incoming requests. Attempts to post all, and
    /// will return the ones that failed to post.
    pub fn post_request_batch(
        &mut self,
        requests: Vec<MatchmakingRequest>,
    ) -> Result<(), Vec<PostError>> {
        let full_batch_size = requests.len();
        let batch_arrival_timestamp = now();

        let mut errors = Vec::new();

        for request in requests {
            match self.post_request_raw(request) {
                Ok(_) => continue,
                Err(post_error) => errors.push(post_error),
            }
        }

        self.global_feedback
            .record_new_request_batch(batch_arrival_timestamp, full_batch_size);

        self.flush_waiting_lane();

        match errors.len() {
            0 => Ok(()),
            1.. => Err(errors),
        }
    }

    /// Resolves the index of the anchor bucket that the input request should be
    /// posted to.
    fn resolve_anchor_bucket_index(&self, request: &MatchmakingRequest) -> usize {
        (request.skill_rating / self.queue_parameters.bucket_width()) as usize
    }

    /// Assigns the next ticket, if available.
    pub fn next_assignment(&mut self) -> Option<Assigned> {
        while let Some(ticket) = SchedulingTable::next_ticket(&mut self.ready_queue) {
            let result =
                SchedulingTable::try_acquire_lease(&mut self.request_matrix, ticket.peek());
            match result {
                Ok(lease) => {
                    let anchor_bucket_index = ticket.peek().anchor_bucket_index();
                    let delta = ticket.peek().delta;
                    let window_eligible_request_counts = OffsetIndexedSlice::from_slice(
                        &self.bookkeeping.bucket_request_counts,
                        anchor_bucket_index,
                        delta,
                    )
                    .unwrap();

                    let ground_allocation_control_signals = GroundAllocationControlSignals {
                        maximum_matchings: SchedulingTable::RESIDENCY_LIMIT,
                        requests_per_matching: self.queue_parameters.requests_per_matching(),
                        window_eligible_request_counts,
                    };

                    self.bookkeeping.assigned_ticket_count += 1;

                    return Some(ticket.assign(lease, ground_allocation_control_signals));
                }
                Err(error) => match error {
                    LeaseError::ContentionError(contended_bucket_index) => {
                        SchedulingTable::contend_ticket(
                            &mut self.contention_lane,
                            ticket.contend(),
                            contended_bucket_index,
                        );
                    }
                },
            }
        }

        None
    }

    /// Readmits a previously assigned ticket back into the scheduler.
    pub fn readmit_completed(&mut self, mut ticket: Completed) {
        // update bookkeeping
        self.update_assignment_batch_bookkeeping(&ticket, &ticket.local_feedback);
        SchedulingTable::update_oldest_request_timestamp(&mut ticket);

        // release lease
        self.request_matrix.release(
            ticket.lease.take().unwrap().into_buckets(),
            ticket.ticket.anchor_bucket_index(),
            ticket.ticket.delta,
        );

        // flush contenders
        self.flush_contenders(ticket.anchor_bucket_index(), ticket.delta);

        // apply resize policy
        let current_delta = ticket.delta as i32;
        let target_delta = compute_target_delta(
            &ticket,
            &self.bookkeeping,
            &self.queue_parameters,
            &ticket.local_feedback,
            &self.global_feedback,
        );
        let new_delta = (current_delta
            + match &ticket.local_feedback.furthest_bucket_distance {
                edge_bucket if ticket.delta < target_delta && *edge_bucket == ticket.delta => 1,
                edge_bucket if ticket.delta > target_delta && *edge_bucket < ticket.delta => -1,
                _ => 0,
            }) as usize;
        ticket.delta = new_delta;

        // apply retry policy
        let backoff = match &ticket.local_feedback.successful_matchings {
            0 => Some(compute_backoff_duration(
                &ticket,
                &self.bookkeeping,
                &self.queue_parameters,
                &ticket.local_feedback,
                &self.global_feedback,
            )),
            1.. => None,
        };

        // check if the anchor bucket is empty
        match (backoff, ticket.oldest_request_timestamp) {
            (Some(backoff_duration), _) => {
                let completion_timestamp = ticket.local_feedback.completion_timestamp;
                let ticket = ticket.wait(completion_timestamp + backoff_duration);
                self.wait_ticket(ticket);
            }
            (None, Some(_)) => {
                let ticket = ReadyPriority::Normal(ticket.ready());
                self.ready_ticket(ticket);
            }
            (None, None) => {
                let ticket = ticket.empty();
                self.empty_ticket(ticket);
            }
        }

        // lease is dropped, releasing buckets
    }

    /// Places a ticket into contention for a bucket.
    fn contend_ticket(
        contention_lane: &mut Contention,
        ticket: Contended,
        contended_bucket_index: usize,
    ) {
        contention_lane.contend_ticket(ticket, contended_bucket_index);
    }

    fn generate_proportion_table(
        skill_rating_range: u32,
        bucket_width: u32,
        rating_distribution: &NormalDistribution,
    ) -> Box<[f64]> {
        let bucket_count = (skill_rating_range / bucket_width) as usize;

        let mut proportion_table = Vec::with_capacity(bucket_count);

        for i in 0..bucket_count {
            let proportion = {
                let lower_limit = (bucket_width * i as u32) as f64;
                let upper_limit = (bucket_width * (i + 1) as u32) as f64;

                rating_distribution.cdf(upper_limit) - rating_distribution.cdf(lower_limit)
            };
            proportion_table.push(proportion);
        }

        proportion_table.into_boxed_slice()
    }
}

/// Admission lanes for simple states.
///
/// All methods here should take a state and wrap it in the necessary state
/// wrapper, before admitting it into the right subsystem.
impl SchedulingTable {
    /// Moves a ticket into the ready queue.
    fn ready_ticket(&mut self, ticket: ReadyPriority) {
        self.ready_queue.push(ticket);
    }

    /// Holds a ticket in waiting lane.
    fn wait_ticket(&mut self, ticket: Waiting) {
        self.waiting_lane.push(ticket);
    }

    /// Holds a ticket in empty hold.
    fn empty_ticket(&mut self, ticket: Empty) {
        self.empty_hold.place(ticket);
    }
}

/// Scheduler utilities and ticket state transitions. Nothing here should borrow
/// the scheduler itself, only parts of it.
impl SchedulingTable {
    /// Flushes all tickets that have their timeouts fully elapsed from the
    /// waiting lane into the ready queue. Corresponding anchor bucket must
    /// not be empty.
    fn flush_waiting_lane(&mut self) {
        let now = now();

        while let Some(ticket) = self.waiting_lane.peek() {
            if ticket.next_readmission_timestamp() <= now {
                let ticket = self.waiting_lane.pop().unwrap();
                self.ready_queue.push(ReadyPriority::Normal(ticket.ready()))
            } else {
                break;
            }
        }
    }

    /// Returns the next ticket to be assigned, if available.
    fn next_ticket(ready_queue: &mut BinaryHeap<ReadyPriority>) -> Option<ReadyPriority> {
        ready_queue.pop()
    }

    /// Acquires lease for a given ticket.
    fn try_acquire_lease(
        request_matrix: &mut RequestMatrix,
        ticket: &Ticket,
    ) -> Result<Lease, LeaseError> {
        Lease::try_acquire(request_matrix, ticket.anchor_bucket_index(), ticket.delta)
    }

    /// Flushes all contenders for a window.
    fn flush_contenders(&mut self, anchor: usize, delta: usize) {
        for contended in self.contention_lane.flush_contenders(anchor, delta) {
            self.ready_queue.push(ReadyPriority::Contended(contended))
        }
    }

    /// Updates eligible request counts after a ground allocation session.
    fn update_assignment_batch_bookkeeping(
        &mut self,
        ticket: &Ticket,
        local_feedback: &GroundAllocationSessionSummary,
    ) {
        let GroundAllocationSessionSummary {
            successful_matchings,
            drained_counts,
            ..
        } = local_feedback;

        self.bookkeeping.assigned_ticket_count -= 1;

        let mut bucket_request_counts = OffsetIndexedSlice::new_mut_view(
            &mut self.bookkeeping.bucket_request_counts,
            ticket.anchor_bucket_index(),
            ticket.delta,
        );
        for offset in -(ticket.delta as i32)..=(ticket.delta as i32) {
            if let Some(dc) = drained_counts.get_offset(offset) {
                **bucket_request_counts.get_offset_mut(offset).unwrap() -= dc;
            }
        }

        self.global_feedback.record_new_assignment_batch(
            *successful_matchings * self.queue_parameters.requests_per_matching(),
        );
    }

    /// Updates new oldest request timing.
    fn update_oldest_request_timestamp(ticket: &mut Completed) {
        ticket.oldest_request_timestamp = ticket
            .lease
            .as_ref()
            .unwrap()
            .try_peek(0)
            .map(|request| request.submission_timestamp)
    }

    pub fn diagnostics(&self) -> SchedulerDiagnostics {
        SchedulerDiagnostics {
            total_request_count: self.global_feedback.total_requests_received(),
            total_filled_request_count: self.global_feedback.total_requests_assigned(),
            per_bucket_request_count: self.bookkeeping.bucket_request_counts.clone(),

            ready_count: self.ready_queue.len(),
            assigned_count: self.bookkeeping.assigned_ticket_count,
            waiting_count: self.waiting_lane.len(),
            contended_count: self.contention_lane.count(),
            empty_count: self.empty_hold.count(),

            contention_lane_view: self.contention_lane.scan(),
        }
    }
}

/// Thin operational wrapper for ready tickets.
#[derive(PartialEq, PartialOrd, Eq, Ord)]
pub enum ReadyPriority {
    Normal(Normal),
    Contended(Contended),
}

impl ReadyPriority {
    pub fn peek(&self) -> &Ticket {
        match self {
            ReadyPriority::Normal(ticket) => ticket,
            ReadyPriority::Contended(ticket) => ticket,
        }
    }

    pub fn assign(
        self,
        lease: Lease,
        ground_allocation_control_signals: GroundAllocationControlSignals,
    ) -> Assigned {
        match self {
            ReadyPriority::Normal(ticket) => {
                ticket.assigned(lease, ground_allocation_control_signals)
            }
            ReadyPriority::Contended(ticket) => {
                ticket.assigned(lease, ground_allocation_control_signals)
            }
        }
    }

    pub fn contend(self) -> Contended {
        match self {
            ReadyPriority::Normal(ticket) => ticket.contended(),
            ReadyPriority::Contended(ticket) => ticket,
        }
    }
}

pub struct SchedulerBookkeeping {
    pub bucket_request_counts: Box<[usize]>,
    pub proportion_table: Box<[f64]>,
    pub assigned_ticket_count: usize,
}

pub struct SchedulerDiagnostics {
    pub total_request_count: usize,
    pub total_filled_request_count: usize,
    pub per_bucket_request_count: Box<[usize]>,

    pub ready_count: usize,
    pub assigned_count: usize,
    pub waiting_count: usize,
    pub contended_count: usize,
    pub empty_count: usize,

    pub contention_lane_view: Box<[Vec<usize>]>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::SchedulingTable;
    use crate::matchmaking::config::MatchmakingQueueParameters;
    use crate::matchmaking::ground_allocation::GroundAllocator;
    use crate::prelude::MatchmakingRequest;

    fn request(request_id: u32, skill_rating: u32) -> MatchmakingRequest {
        MatchmakingRequest {
            request_id,
            skill_rating,
            submission_timestamp: Duration::from_secs(request_id as u64),
        }
    }

    fn test_queue_parameters() -> MatchmakingQueueParameters {
        MatchmakingQueueParameters::builder()
            .skill_rating_range(300)
            .bucket_width(100)
            .delta_ceiling(2)
            .build()
            .unwrap()
    }

    #[test]
    fn new_scheduler_has_no_immediately_assignable_work() {
        let mut table = SchedulingTable::new(test_queue_parameters());

        assert!(table.next_assignment().is_none());
    }

    #[test]
    fn posting_request_wakes_empty_ticket_and_snapshots_counts() {
        let mut table = SchedulingTable::new(test_queue_parameters());

        table.post_request_batch(vec![request(1, 150)]).unwrap();

        let assigned = table.next_assignment().unwrap();

        assert_eq!(assigned.anchor_bucket_index(), 1);
        assert_eq!(assigned.delta, 0);
        assert_eq!(
            assigned
                .ground_allocation_control_signals
                .window_eligible_request_counts
                .get_offset(0),
            Some(&1)
        );
        assert_eq!(assigned.lease.try_peek(0).unwrap().request_id, 1);
    }

    #[test]
    fn ready_queue_prefers_oldest_anchor_request() {
        let mut table = SchedulingTable::new(test_queue_parameters());

        table
            .post_request_batch(vec![request(2, 150), request(1, 50)])
            .unwrap();

        let assigned = table.next_assignment().unwrap();

        assert_eq!(assigned.anchor_bucket_index(), 0);
        assert_eq!(assigned.lease.try_peek(0).unwrap().request_id, 1);
    }

    #[test]
    fn successful_readmission_requeues_anchor_with_refreshed_head() {
        let mut table = SchedulingTable::new(test_queue_parameters());
        let requests = (1..=20).map(|request_id| request(request_id, 50)).collect();

        table.post_request_batch(requests).unwrap();

        let completed = {
            let mut assigned = table.next_assignment().unwrap();
            assigned.ground_allocation_control_signals.maximum_matchings = 1;
            let allocator = GroundAllocator::new(assigned);
            let (_matchings, completed) = allocator.run_allocation();
            completed
        };

        table.readmit_completed(completed);

        let reassigned = table.next_assignment().unwrap();

        assert_eq!(reassigned.anchor_bucket_index(), 0);
        assert_eq!(reassigned.lease.try_peek(0).unwrap().request_id, 11);
    }

    #[test]
    fn failed_readmission_waits_ticket_instead_of_immediate_reassignment() {
        let mut table = SchedulingTable::new(test_queue_parameters());

        table.post_request_batch(vec![request(1, 50)]).unwrap();

        let completed = {
            let assigned = table.next_assignment().unwrap();
            let allocator = GroundAllocator::new(assigned);
            let (_matchings, completed) = allocator.run_allocation();
            completed
        };

        assert_eq!(completed.local_feedback.successful_matchings, 0);

        table.readmit_completed(completed);

        assert!(table.next_assignment().is_none());
    }

    #[test]
    fn fully_drained_anchor_stays_empty_until_new_request_arrives() {
        let mut table = SchedulingTable::new(test_queue_parameters());
        let requests = (1..=10).map(|request_id| request(request_id, 50)).collect();

        table.post_request_batch(requests).unwrap();

        let completed = {
            let mut assigned = table.next_assignment().unwrap();
            assigned.ground_allocation_control_signals.maximum_matchings = 1;
            let allocator = GroundAllocator::new(assigned);
            let (_matchings, completed) = allocator.run_allocation();
            completed
        };

        table.readmit_completed(completed);

        assert!(table.next_assignment().is_none());

        table.post_request_batch(vec![request(11, 50)]).unwrap();

        let reassigned = table.next_assignment().unwrap();
        assert_eq!(reassigned.anchor_bucket_index(), 0);
        assert_eq!(reassigned.lease.try_peek(0).unwrap().request_id, 11);
    }
}
