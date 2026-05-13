//! Ticket job system.

use std::time::SystemTime;

use crate::matchmaking::scheduling::substructures::empty_hold::EmptyHold;

#[derive(Debug)]
pub struct Ticket {
    anchor_bucket_index: usize,
    pub delta: usize,
    pub oldest_request_timestamp: Option<SystemTime>,
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

pub fn issue_tickets(count: usize) -> EmptyHold {
    EmptyHold::new_with_issue(count)
}
