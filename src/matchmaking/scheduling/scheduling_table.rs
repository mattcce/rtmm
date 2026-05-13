//! Global allocation scheduler.

use std::collections::BinaryHeap;
use std::time::SystemTime;

use statrs::distribution::{ContinuousCDF, Normal as NormalDistribution};

use crate::matchmaking::config::MatchmakingQueueParameters;
use crate::matchmaking::ground_allocation::GroundAllocationSessionSummary;
use crate::matchmaking::scheduling::leases::Lease;
use crate::matchmaking::scheduling::policies::{
    SchedulerGlobalFeedback, compute_backoff_duration, compute_target_delta,
};
use crate::matchmaking::scheduling::request_matrix::*;
use crate::matchmaking::scheduling::states::*;
use crate::matchmaking::scheduling::substructures::contention::Contention;
use crate::matchmaking::scheduling::substructures::empty_hold::EmptyHold;
use crate::matchmaking::scheduling::tickets::Ticket;
use crate::matchmaking::utils::OffsetIndexedSlice;
use crate::prelude::MatchmakingRequest;

/// Core scheduling table for a single matchmaking queue.
pub struct SchedulingTable {
    request_matrix: RequestMatrix,
    postbox: Postbox,

    contention_lane: Contention,
    ready_queue: BinaryHeap<ReadyPriority>,
    waiting_lane: BinaryHeap<Waiting>,
    empty_hold: EmptyHold,

    bookkeeping: SchedulerBookkeeping,
    queue_parameters: MatchmakingQueueParameters,
    global_feedback: SchedulerGlobalFeedback,
}

/// Basic scheduler functionality and utilities.
impl SchedulingTable {
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
            (*ticket).oldest_request_timestamp = Some(request.submission_timestamp);
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
        let batch_arrival_timestamp = SystemTime::now();

        let mut errors = Vec::new();

        for request in requests {
            match self.post_request_raw(request) {
                Ok(_) => continue,
                Err(post_error) => errors.push(post_error),
            }
        }

        self.global_feedback
            .record_new_batch(batch_arrival_timestamp, full_batch_size);

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
    pub fn next_assignment(&mut self) -> Option<Assigned<'_>> {
        while let Some(ticket) = SchedulingTable::next_ticket(&mut self.ready_queue) {
            let result = SchedulingTable::try_acquire_lease(&self.request_matrix, &ticket.peek());
            match result {
                Ok(lease) => {
                    let anchor_bucket_index = ticket.peek().anchor_bucket_index();
                    let delta = ticket.peek().delta;
                    let eligible_request_counts = OffsetIndexedSlice::from_slice(
                        &self.bookkeeping.bucket_request_counts,
                        anchor_bucket_index,
                        delta,
                    )
                    .unwrap();
                    return Some(ticket.assign(lease, eligible_request_counts));
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
    pub fn readmit_assigned(
        &mut self,
        mut ticket: Assigned,
        lease: Lease,
        local_feedback: GroundAllocationSessionSummary,
    ) {
        // update bookkeeping
        self.update_drained_counts(&ticket, &local_feedback);
        SchedulingTable::update_oldest_request_timestamp(&lease, &mut ticket);

        // flush contenders
        self.flush_contenders(ticket.anchor_bucket_index(), ticket.delta);

        // apply resize policy
        let current_delta = ticket.delta as i32;
        let target_delta = compute_target_delta(
            &ticket,
            &self.bookkeeping,
            &self.queue_parameters,
            &local_feedback,
            &self.global_feedback,
        );
        let new_delta = (current_delta
            + match local_feedback.furthest_bucket_distance {
                edge_bucket if ticket.delta < target_delta && edge_bucket == ticket.delta => 1,
                edge_bucket if ticket.delta > target_delta && edge_bucket < ticket.delta => -1,
                _ => 0,
            }) as usize;
        ticket.delta = new_delta;

        // apply retry policy
        let backoff = match local_feedback.successful_matchings {
            0 => Some(compute_backoff_duration(
                &ticket,
                &self.bookkeeping,
                &self.queue_parameters,
                &local_feedback,
                &self.global_feedback,
            )),
            1.. => None,
        };

        // check if the anchor bucket is empty
        match (backoff, ticket.oldest_request_timestamp) {
            (Some(backoff_duration), _) => self
                .wait_ticket(ticket.wait(local_feedback.completion_timestamp + backoff_duration)),
            (None, Some(_)) => self.ready_ticket(ReadyPriority::Normal(ticket.ready())),
            (None, None) => self.empty_ticket(ticket.empty()),
        }
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
                let proportion =
                    rating_distribution.cdf(upper_limit) - rating_distribution.cdf(lower_limit);
                proportion
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
        let now = SystemTime::now();

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
    fn try_acquire_lease<'a, 'b>(
        request_matrix: &'a RequestMatrix,
        ticket: &'b Ticket,
    ) -> Result<Lease<'a>, LeaseError> {
        Lease::try_acquire(request_matrix, ticket.anchor_bucket_index(), ticket.delta)
    }

    /// Flushes all contenders for a window.
    fn flush_contenders(&mut self, anchor: usize, delta: usize) {
        for contended in self.contention_lane.flush_contenders(anchor, delta) {
            self.ready_queue.push(ReadyPriority::Contended(contended))
        }
    }

    /// Updates eligible request counts after a ground allocation session.
    fn update_drained_counts(
        &mut self,
        ticket: &Ticket,
        local_feedback: &GroundAllocationSessionSummary,
    ) {
        let GroundAllocationSessionSummary { drained_counts, .. } = local_feedback;

        let mut bucket_request_counts = OffsetIndexedSlice::from_slice(
            &mut self.bookkeeping.bucket_request_counts,
            ticket.anchor_bucket_index(),
            ticket.delta,
        )
        .unwrap();
        for offset in -(ticket.delta as i32)..=(ticket.delta as i32) {
            if let Some(dc) = drained_counts.get_offset(offset) {
                *bucket_request_counts.get_offset_mut(offset).unwrap() -= dc;
            }
        }
    }

    /// Updates new oldest request timing.
    fn update_oldest_request_timestamp(lease: &Lease, ticket: &mut Ticket) {
        ticket.oldest_request_timestamp = match lease.try_peek(0) {
            Some(request) => Some(request.submission_timestamp),
            None => None,
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

    pub fn assign<'a>(
        self,
        lease: Lease<'a>,
        eligible_request_counts: OffsetIndexedSlice<usize>,
    ) -> Assigned<'a> {
        match self {
            ReadyPriority::Normal(ticket) => ticket.assigned(lease, eligible_request_counts),
            ReadyPriority::Contended(ticket) => ticket.assigned(lease, eligible_request_counts),
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
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::SchedulingTable;
    use crate::matchmaking::config::MatchmakingQueueParameters;
    use crate::prelude::MatchmakingRequest;

    fn request(request_id: u32, skill_rating: u32) -> MatchmakingRequest {
        MatchmakingRequest {
            request_id,
            skill_rating,
            submission_timestamp: UNIX_EPOCH + Duration::from_secs(request_id as u64),
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
        assert_eq!(assigned.eligible_request_counts.get_offset(0), Some(&1));
        assert_eq!(assigned.lease.try_peek(0).unwrap().request_id, 1);
    }
}
