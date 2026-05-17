//! Hold for presently empty tickets.

use crate::matchmaking::scheduling::states::{Empty, new_empty_ticket};

#[derive(Debug)]
pub struct EmptyHold {
    holds: Box<[Option<Empty>]>,
    ticket_count: usize,
}

impl EmptyHold {
    pub fn new_with_issue(bucket_count: usize) -> EmptyHold {
        EmptyHold {
            holds: (0..bucket_count)
                .map(|index| Some(new_empty_ticket(index)))
                .collect(),
            ticket_count: bucket_count,
        }
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.ticket_count
    }

    pub fn place(&mut self, ticket: Empty) {
        if self.holds[ticket.anchor_bucket_index()].is_some() {
            panic!();
        }

        self.holds[ticket.anchor_bucket_index()].replace(ticket);
        self.ticket_count += 1;
    }

    pub fn take(&mut self, anchor_bucket_index: usize) -> Option<Empty> {
        if self.holds[anchor_bucket_index].is_some() {
            self.ticket_count -= 1;
        }

        self.holds[anchor_bucket_index].take()
    }
}

#[cfg(test)]
mod tests {
    use super::EmptyHold;
    use crate::matchmaking::scheduling::states::new_empty_ticket;

    #[test]
    fn issues_one_empty_ticket_per_anchor() {
        let mut hold = EmptyHold::new_with_issue(3);

        for anchor_bucket_index in 0..3 {
            let ticket = hold.take(anchor_bucket_index).unwrap();
            assert_eq!(ticket.anchor_bucket_index(), anchor_bucket_index);
        }

        assert!(hold.take(0).is_none());
    }

    #[test]
    fn place_reinserts_ticket_at_its_anchor() {
        let mut hold = EmptyHold::new_with_issue(1);
        let ticket = hold.take(0).unwrap();

        assert!(hold.take(0).is_none());

        hold.place(ticket);

        assert_eq!(hold.take(0).unwrap().anchor_bucket_index(), 0);
    }

    #[test]
    #[should_panic]
    fn place_panics_on_occupied_slot() {
        let mut hold = EmptyHold::new_with_issue(1);
        let _taken = hold.take(0).unwrap();

        hold.place(new_empty_ticket(0));
        hold.place(new_empty_ticket(0));
    }
}
