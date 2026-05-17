//! Ticket job system.

use std::fmt::Display;
use std::time::Duration;

use log::error;

use crate::matchmaking::scheduling::substructures::empty_hold::EmptyHold;

#[derive(Debug)]
pub struct Ticket {
    anchor_bucket_index: usize,
    pub delta: usize,
    pub oldest_request_timestamp: Option<Duration>,
}

impl Display for Ticket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Ticket {
    pub fn new(anchor_bucket_index: usize) -> Ticket {
        Ticket {
            anchor_bucket_index,
            delta: 0,
            oldest_request_timestamp: None,
        }
    }

    #[inline]
    pub fn anchor_bucket_index(&self) -> usize {
        self.anchor_bucket_index
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        error!("Ticket {self} dropped.")
    }
}

#[cfg(test)]
mod tests {
    use super::Ticket;

    #[test]
    fn new_ticket_starts_with_delta_zero_and_no_timestamp() {
        let ticket = Ticket::new(5);

        assert_eq!(ticket.anchor_bucket_index(), 5);
        assert_eq!(ticket.delta, 0);
        assert_eq!(ticket.oldest_request_timestamp, None);
    }

    #[test]
    fn ticket_delta_can_be_set() {
        let mut ticket = Ticket::new(3);
        ticket.delta = 4;

        assert_eq!(ticket.delta, 4);
    }
}

pub fn issue_tickets(count: usize) -> EmptyHold {
    EmptyHold::new_with_issue(count)
}
