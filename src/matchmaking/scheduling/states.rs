//! Thin operational wrappers for ticket states.
//!
//! Except to prevent nonconsumption of a state, a state should transparently
//! provide access to its tickets (possibly mutably) as it only wraps around
//! them with additional state.

use std::ops::{Deref, DerefMut};
use std::time::Duration;

use crate::matchmaking::ground_allocation::{
    GroundAllocationControlSignals, GroundAllocationSessionSummary,
};
use crate::matchmaking::scheduling::leases::Lease;
use crate::matchmaking::scheduling::tickets::Ticket;
use crate::matchmaking::utils::now;

/// Thin operational wrapper for waiting tickets.
pub struct Waiting {
    ticket: Ticket,
    next_readmission_timestamp: Duration,
}

impl Waiting {
    pub fn new(ticket: Ticket, next_readmission_timestamp: Duration) -> Waiting {
        Waiting {
            ticket,
            next_readmission_timestamp,
        }
    }

    pub fn ready(self) -> Normal {
        Normal::new(self.ticket)
    }

    #[inline]
    pub fn next_readmission_timestamp(&self) -> Duration {
        self.next_readmission_timestamp
    }
}

impl Deref for Waiting {
    type Target = Ticket;

    fn deref(&self) -> &Self::Target {
        &self.ticket
    }
}

impl DerefMut for Waiting {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ticket
    }
}

impl Ord for Waiting {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .next_readmission_timestamp
            .cmp(&self.next_readmission_timestamp)
    }
}

impl PartialOrd for Waiting {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Waiting {
    fn eq(&self, other: &Self) -> bool {
        self.next_readmission_timestamp == other.next_readmission_timestamp
    }
}

impl Eq for Waiting {}

/// Thin operational wrapper for normal ready tickets.
pub struct Normal {
    ticket: Ticket,
}

impl Normal {
    pub fn new(ticket: Ticket) -> Normal {
        Normal { ticket }
    }

    pub fn contended(self) -> Contended {
        Contended {
            ticket: self.ticket,
            contention_start_timestamp: now(),
        }
    }

    pub fn assigned(
        self,
        lease: Lease,
        ground_allocation_control_signals: GroundAllocationControlSignals,
    ) -> Assigned {
        Assigned {
            ticket: self.ticket,
            lease,
            ground_allocation_control_signals,
        }
    }
}

impl Deref for Normal {
    type Target = Ticket;

    fn deref(&self) -> &Self::Target {
        &self.ticket
    }
}

impl DerefMut for Normal {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ticket
    }
}

impl Ord for Normal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .ticket
            .oldest_request_timestamp
            .cmp(&self.ticket.oldest_request_timestamp)
    }
}

impl PartialOrd for Normal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Normal {
    fn eq(&self, other: &Self) -> bool {
        self.ticket.oldest_request_timestamp == other.ticket.oldest_request_timestamp
    }
}

impl Eq for Normal {}

/// Thin operational wrapper for assigned tickets.
pub struct Assigned {
    pub ticket: Ticket,
    pub lease: Lease,
    pub ground_allocation_control_signals: GroundAllocationControlSignals,
}

impl Assigned {
    pub fn complete(self, local_feedback: GroundAllocationSessionSummary) -> Completed {
        Completed {
            ticket: self.ticket,
            lease: Some(self.lease),
            local_feedback,
        }
    }
}

impl Deref for Assigned {
    type Target = Ticket;

    fn deref(&self) -> &Self::Target {
        &self.ticket
    }
}

impl DerefMut for Assigned {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ticket
    }
}

/// Thin operational wrapper for contended tickets.
pub struct Contended {
    ticket: Ticket,
    contention_start_timestamp: Duration,
}

impl Contended {
    pub fn assigned(
        self,
        lease: Lease,
        ground_allocation_control_signals: GroundAllocationControlSignals,
    ) -> Assigned {
        Assigned {
            ticket: self.ticket,
            lease,
            ground_allocation_control_signals,
        }
    }
}

impl Deref for Contended {
    type Target = Ticket;

    fn deref(&self) -> &Self::Target {
        &self.ticket
    }
}

impl DerefMut for Contended {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ticket
    }
}
impl Ord for Contended {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .contention_start_timestamp
            .cmp(&self.contention_start_timestamp)
            .then(
                other
                    .ticket
                    .oldest_request_timestamp
                    .cmp(&self.ticket.oldest_request_timestamp),
            )
    }
}

impl PartialOrd for Contended {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Contended {
    fn eq(&self, other: &Self) -> bool {
        (self.contention_start_timestamp == other.contention_start_timestamp)
            && (self.ticket.oldest_request_timestamp == other.ticket.oldest_request_timestamp)
    }
}

impl Eq for Contended {}

/// Thin operational wrapper for empty tickets.
#[derive(Debug)]
pub struct Empty {
    ticket: Ticket,
}

impl Empty {
    pub fn new(ticket: Ticket) -> Empty {
        Empty { ticket }
    }

    pub fn ready(self) -> Normal {
        Normal::new(self.ticket)
    }
}

impl Deref for Empty {
    type Target = Ticket;

    fn deref(&self) -> &Self::Target {
        &self.ticket
    }
}

impl DerefMut for Empty {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ticket
    }
}

/// Thin operational wrapper for completed tickets.
/// The lease must be extracted and freed manually, or the state transition will
/// force panic.
pub struct Completed {
    pub ticket: Ticket,
    pub lease: Option<Lease>,
    pub local_feedback: GroundAllocationSessionSummary,
}

impl Completed {
    pub fn wait(self, until: Duration) -> Waiting {
        assert!(self.lease.is_none());
        Waiting::new(self.ticket, until)
    }

    pub fn ready(self) -> Normal {
        assert!(self.lease.is_none());
        Normal::new(self.ticket)
    }

    pub fn empty(self) -> Empty {
        assert!(self.lease.is_none());
        Empty::new(self.ticket)
    }
}

impl Deref for Completed {
    type Target = Ticket;

    fn deref(&self) -> &Self::Target {
        &self.ticket
    }
}

impl DerefMut for Completed {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ticket
    }
}

pub fn new_empty_ticket(anchor_index: usize) -> Empty {
    Empty::new(Ticket::new(anchor_index))
}

#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;
    use std::time::Duration;

    use super::{Contended, Normal, Waiting, new_empty_ticket};
    use crate::matchmaking::ground_allocation::{
        GroundAllocationControlSignals, GroundAllocationSessionSummary,
    };
    use crate::matchmaking::scheduling::leases::Lease;
    use crate::matchmaking::scheduling::request_matrix::RequestMatrix;
    use crate::matchmaking::scheduling::scheduling_table::ReadyPriority;
    use crate::matchmaking::scheduling::tickets::Ticket;
    use crate::matchmaking::utils::OffsetIndexedSlice;

    #[test]
    fn empty_ready_contended_transition_preserves_ticket_identity() {
        let contended = new_empty_ticket(2).ready().contended();

        assert_eq!(contended.anchor_bucket_index(), 2);
        assert_eq!(contended.delta, 0);
        assert_eq!(contended.oldest_request_timestamp, None);
    }

    #[test]
    fn assigned_wait_transition_preserves_ticket_identity() {
        let (mut request_matrix, _) = RequestMatrix::new(1, 1);
        let lease = Lease::try_acquire(&mut request_matrix, 0, 0).unwrap();
        let window_eligible_request_counts =
            OffsetIndexedSlice::from_slice(&[1usize], 0, 0).unwrap();
        let signals = GroundAllocationControlSignals {
            maximum_matchings: 1,
            requests_per_matching: 10,
            window_eligible_request_counts,
        };
        let summary = GroundAllocationSessionSummary {
            successful_matchings: 0,
            furthest_bucket_distance: 0,
            completion_timestamp: Duration::ZERO,
            drained_counts: OffsetIndexedSlice::from_slice(&[0usize], 0, 0).unwrap(),
        };

        let mut completed = new_empty_ticket(0)
            .ready()
            .assigned(lease, signals)
            .complete(summary);

        let lease = completed.lease.take().unwrap();
        request_matrix.release(
            lease.into_buckets(),
            completed.anchor_bucket_index(),
            completed.delta,
        );

        let waiting = completed.wait(Duration::from_secs(5));

        assert_eq!(waiting.anchor_bucket_index(), 0);
        assert_eq!(waiting.next_readmission_timestamp(), Duration::from_secs(5));
    }

    #[test]
    fn assigned_ready_releases_lease_and_preserves_ticket() {
        let (mut request_matrix, _) = RequestMatrix::new(1, 1);
        let lease = Lease::try_acquire(&mut request_matrix, 0, 0).unwrap();
        let window_eligible_request_counts =
            OffsetIndexedSlice::from_slice(&[1usize], 0, 0).unwrap();
        let signals = GroundAllocationControlSignals {
            maximum_matchings: 1,
            requests_per_matching: 10,
            window_eligible_request_counts,
        };
        let summary = GroundAllocationSessionSummary {
            successful_matchings: 0,
            furthest_bucket_distance: 0,
            completion_timestamp: Duration::ZERO,
            drained_counts: OffsetIndexedSlice::from_slice(&[0usize], 0, 0).unwrap(),
        };

        let mut completed = new_empty_ticket(0)
            .ready()
            .assigned(lease, signals)
            .complete(summary);

        let lease = completed.lease.take().unwrap();
        request_matrix.release(
            lease.into_buckets(),
            completed.anchor_bucket_index(),
            completed.delta,
        );

        let normal = completed.ready();

        assert_eq!(normal.anchor_bucket_index(), 0);
        assert!(request_matrix.try_lease_window(0, 0).is_ok());
    }

    #[test]
    fn assigned_empty_releases_lease_and_preserves_ticket() {
        let (mut request_matrix, _) = RequestMatrix::new(1, 1);
        let lease = Lease::try_acquire(&mut request_matrix, 0, 0).unwrap();
        let window_eligible_request_counts =
            OffsetIndexedSlice::from_slice(&[0usize], 0, 0).unwrap();
        let signals = GroundAllocationControlSignals {
            maximum_matchings: 1,
            requests_per_matching: 10,
            window_eligible_request_counts,
        };
        let summary = GroundAllocationSessionSummary {
            successful_matchings: 0,
            furthest_bucket_distance: 0,
            completion_timestamp: Duration::ZERO,
            drained_counts: OffsetIndexedSlice::from_slice(&[0usize], 0, 0).unwrap(),
        };

        let mut completed = new_empty_ticket(0)
            .ready()
            .assigned(lease, signals)
            .complete(summary);

        let lease = completed.lease.take().unwrap();
        request_matrix.release(
            lease.into_buckets(),
            completed.anchor_bucket_index(),
            completed.delta,
        );

        let empty = completed.empty();

        assert_eq!(empty.anchor_bucket_index(), 0);
        assert!(request_matrix.try_lease_window(0, 0).is_ok());
    }

    #[test]
    fn waiting_orders_earliest_timestamp_first() {
        let mut heap = BinaryHeap::new();

        heap.push(Waiting {
            ticket: Ticket::new(0),
            next_readmission_timestamp: Duration::from_secs(10),
        });
        heap.push(Waiting {
            ticket: Ticket::new(1),
            next_readmission_timestamp: Duration::from_secs(5),
        });

        assert_eq!(heap.pop().unwrap().anchor_bucket_index(), 1);
    }

    #[test]
    fn normal_orders_oldest_anchor_request_first() {
        let mut heap = BinaryHeap::new();

        let mut newer = Ticket::new(0);
        newer.oldest_request_timestamp = Some(Duration::from_secs(10));
        let mut older = Ticket::new(1);
        older.oldest_request_timestamp = Some(Duration::from_secs(5));

        heap.push(Normal { ticket: newer });
        heap.push(Normal { ticket: older });

        assert_eq!(heap.pop().unwrap().anchor_bucket_index(), 1);
    }

    #[test]
    fn contended_orders_longest_wait_first() {
        let mut heap = BinaryHeap::new();

        let mut shorter_wait = Ticket::new(0);
        shorter_wait.oldest_request_timestamp = Some(Duration::from_secs(3));
        let mut longer_wait = Ticket::new(1);
        longer_wait.oldest_request_timestamp = Some(Duration::from_secs(4));

        heap.push(Contended {
            ticket: shorter_wait,
            contention_start_timestamp: Duration::from_secs(10),
        });
        heap.push(Contended {
            ticket: longer_wait,
            contention_start_timestamp: Duration::from_secs(5),
        });

        assert_eq!(heap.pop().unwrap().anchor_bucket_index(), 1);
    }

    #[test]
    fn contended_to_completed_to_ready_cycle() {
        let (mut request_matrix, _) = RequestMatrix::new(1, 1);
        let lease = Lease::try_acquire(&mut request_matrix, 0, 0).unwrap();
        let window_eligible_request_counts =
            OffsetIndexedSlice::from_slice(&[1usize], 0, 0).unwrap();
        let signals = GroundAllocationControlSignals {
            maximum_matchings: 1,
            requests_per_matching: 10,
            window_eligible_request_counts,
        };

        let contended = new_empty_ticket(0).ready().contended();
        assert_eq!(contended.anchor_bucket_index(), 0);

        let mut completed =
            contended
                .assigned(lease, signals)
                .complete(GroundAllocationSessionSummary {
                    successful_matchings: 0,
                    furthest_bucket_distance: 0,
                    completion_timestamp: Duration::ZERO,
                    drained_counts: OffsetIndexedSlice::from_slice(&[0usize], 0, 0).unwrap(),
                });

        let lease = completed.lease.take().unwrap();
        request_matrix.release(
            lease.into_buckets(),
            completed.anchor_bucket_index(),
            completed.delta,
        );

        let ready = completed.ready();
        assert_eq!(ready.anchor_bucket_index(), 0);
        assert!(request_matrix.try_lease_window(0, 0).is_ok());
    }

    #[test]
    fn mixed_ready_priority_contended_before_normal() {
        let mut heap = BinaryHeap::new();

        let mut late_ticket = Ticket::new(0);
        late_ticket.oldest_request_timestamp = Some(Duration::from_secs(1));

        let mut contended_ticket = Ticket::new(1);
        contended_ticket.oldest_request_timestamp = Some(Duration::from_secs(100));

        heap.push(ReadyPriority::Normal(Normal {
            ticket: late_ticket,
        }));
        heap.push(ReadyPriority::Contended(Contended {
            ticket: contended_ticket,
            contention_start_timestamp: Duration::from_secs(100),
        }));

        assert_eq!(heap.pop().unwrap().peek().anchor_bucket_index(), 1);
    }

    #[test]
    fn normal_same_timestamp_is_stable_in_heap() {
        let mut heap = BinaryHeap::new();

        let mut ticket_a = Ticket::new(2);
        ticket_a.oldest_request_timestamp = Some(Duration::from_secs(10));
        let mut ticket_b = Ticket::new(5);
        ticket_b.oldest_request_timestamp = Some(Duration::from_secs(10));

        heap.push(Normal { ticket: ticket_a });
        heap.push(Normal { ticket: ticket_b });

        // same timestamp — both should pop without panic
        let first = heap.pop().unwrap();
        let second = heap.pop().unwrap();
        assert!(heap.is_empty());

        // both present; ordering between equal-timestamp Normals is not guaranteed
        let anchors: Vec<_> = [first.anchor_bucket_index(), second.anchor_bucket_index()]
            .into_iter()
            .collect();
        assert!(anchors.contains(&2));
        assert!(anchors.contains(&5));
    }

    #[test]
    #[should_panic]
    fn completed_wait_panics_without_releasing_lease() {
        let (mut request_matrix, _) = RequestMatrix::new(1, 1);
        let lease = Lease::try_acquire(&mut request_matrix, 0, 0).unwrap();
        let summary = GroundAllocationSessionSummary {
            successful_matchings: 0,
            furthest_bucket_distance: 0,
            completion_timestamp: Duration::ZERO,
            drained_counts: OffsetIndexedSlice::from_slice(&[0usize], 0, 0).unwrap(),
        };

        let completed = new_empty_ticket(0)
            .ready()
            .assigned(lease, GroundAllocationControlSignals {
                maximum_matchings: 1,
                requests_per_matching: 10,
                window_eligible_request_counts: OffsetIndexedSlice::from_slice(&[1usize], 0, 0).unwrap(),
            })
            .complete(summary);

        let _ = completed.wait(Duration::from_secs(5));
    }
}
