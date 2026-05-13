//! Thin operational wrappers for ticket states.
//!
//! Except to prevent nonconsumption of a state, a state should transparently
//! provide access to its tickets (possibly mutably) as it only wraps around
//! them with additional state.

use std::ops::{Deref, DerefMut};
use std::time::SystemTime;

use crate::matchmaking::scheduling::leases::Lease;
use crate::matchmaking::scheduling::tickets::Ticket;
use crate::matchmaking::utils::OffsetIndexedSlice;

/// Thin operational wrapper for waiting tickets.
pub struct Waiting {
    ticket: Ticket,
    next_readmission_timestamp: SystemTime,
}

impl Waiting {
    pub fn ready(self) -> Normal {
        Normal {
            ticket: self.ticket,
        }
    }

    #[inline]
    pub fn next_readmission_timestamp(&self) -> SystemTime {
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
        Some(self.cmp(&other))
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
    pub fn contended(self) -> Contended {
        Contended {
            ticket: self.ticket,
            contention_start_timestamp: SystemTime::now(),
        }
    }

    pub fn assigned(
        self,
        lease: Lease,
        eligible_request_counts: OffsetIndexedSlice<usize>,
    ) -> Assigned {
        Assigned {
            ticket: self.ticket,
            lease,
            eligible_request_counts,
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
        Some(self.cmp(&other))
    }
}

impl PartialEq for Normal {
    fn eq(&self, other: &Self) -> bool {
        self.ticket.oldest_request_timestamp == other.ticket.oldest_request_timestamp
    }
}

impl Eq for Normal {}

/// Thin operational wrapper for assigned tickets.
pub struct Assigned<'a> {
    pub ticket: Ticket,
    pub lease: Lease<'a>,
    pub eligible_request_counts: OffsetIndexedSlice<usize>,
}

impl Assigned<'_> {
    pub fn wait(self, until: SystemTime) -> Waiting {
        Waiting {
            ticket: self.ticket,
            next_readmission_timestamp: until,
        }
    }

    pub fn ready(self) -> Normal {
        Normal {
            ticket: self.ticket,
        }
    }

    pub fn empty(self) -> Empty {
        Empty {
            ticket: self.ticket,
        }
    }
}

impl Deref for Assigned<'_> {
    type Target = Ticket;

    fn deref(&self) -> &Self::Target {
        &self.ticket
    }
}

impl DerefMut for Assigned<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ticket
    }
}

/// Thin operational wrapper for contended tickets.
pub struct Contended {
    ticket: Ticket,
    contention_start_timestamp: SystemTime,
}

impl Contended {
    pub fn assigned(
        self,
        lease: Lease,
        eligible_request_counts: OffsetIndexedSlice<usize>,
    ) -> Assigned {
        Assigned {
            ticket: self.ticket,
            lease,
            eligible_request_counts,
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
        Some(self.cmp(&other))
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
pub struct Empty {
    ticket: Ticket,
}

impl Empty {
    pub fn ready(self) -> Normal {
        Normal {
            ticket: self.ticket,
        }
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

pub fn new_empty_ticket(anchor_index: usize) -> Empty {
    Empty {
        ticket: Ticket::new(anchor_index),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;
    use std::time::{Duration, UNIX_EPOCH};

    use super::{Contended, Normal, Waiting, new_empty_ticket};
    use crate::matchmaking::scheduling::leases::Lease;
    use crate::matchmaking::scheduling::request_matrix::RequestMatrix;
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
        let (request_matrix, _) = RequestMatrix::new(1, 1);
        let lease = Lease::try_acquire(&request_matrix, 0, 0).unwrap();
        let eligible_request_counts = OffsetIndexedSlice::from_slice(&[1usize], 0, 0).unwrap();

        let waiting = new_empty_ticket(0)
            .ready()
            .assigned(lease, eligible_request_counts)
            .wait(UNIX_EPOCH + Duration::from_secs(5));

        assert_eq!(waiting.anchor_bucket_index(), 0);
        assert_eq!(
            waiting.next_readmission_timestamp(),
            UNIX_EPOCH + Duration::from_secs(5)
        );
    }

    #[test]
    fn waiting_orders_earliest_timestamp_first() {
        let mut heap = BinaryHeap::new();

        heap.push(Waiting {
            ticket: Ticket::new(0),
            next_readmission_timestamp: UNIX_EPOCH + Duration::from_secs(10),
        });
        heap.push(Waiting {
            ticket: Ticket::new(1),
            next_readmission_timestamp: UNIX_EPOCH + Duration::from_secs(5),
        });

        assert_eq!(heap.pop().unwrap().anchor_bucket_index(), 1);
    }

    #[test]
    fn normal_orders_oldest_anchor_request_first() {
        let mut heap = BinaryHeap::new();

        let mut newer = Ticket::new(0);
        newer.oldest_request_timestamp = Some(UNIX_EPOCH + Duration::from_secs(10));
        let mut older = Ticket::new(1);
        older.oldest_request_timestamp = Some(UNIX_EPOCH + Duration::from_secs(5));

        heap.push(Normal { ticket: newer });
        heap.push(Normal { ticket: older });

        assert_eq!(heap.pop().unwrap().anchor_bucket_index(), 1);
    }

    #[test]
    fn contended_orders_longest_wait_first() {
        let mut heap = BinaryHeap::new();

        let mut shorter_wait = Ticket::new(0);
        shorter_wait.oldest_request_timestamp = Some(UNIX_EPOCH + Duration::from_secs(3));
        let mut longer_wait = Ticket::new(1);
        longer_wait.oldest_request_timestamp = Some(UNIX_EPOCH + Duration::from_secs(4));

        heap.push(Contended {
            ticket: shorter_wait,
            contention_start_timestamp: UNIX_EPOCH + Duration::from_secs(10),
        });
        heap.push(Contended {
            ticket: longer_wait,
            contention_start_timestamp: UNIX_EPOCH + Duration::from_secs(5),
        });

        assert_eq!(heap.pop().unwrap().anchor_bucket_index(), 1);
    }
}
