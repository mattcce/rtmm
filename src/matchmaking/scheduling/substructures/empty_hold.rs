//! Hold for presently empty tickets.

use crate::matchmaking::scheduling::states::{Empty, new_empty_ticket};

pub struct EmptyHold {
    holds: Box<[Option<Empty>]>,
}

impl EmptyHold {
    pub fn new_with_issue(bucket_count: usize) -> EmptyHold {
        EmptyHold {
            holds: (0..bucket_count)
                .map(|index| Some(new_empty_ticket(index)))
                .collect(),
        }
    }

    pub fn place(&mut self, ticket: Empty) {
        self.holds[ticket.anchor_bucket_index()].replace(ticket);
    }

    pub fn take(&mut self, anchor_bucket_index: usize) -> Option<Empty> {
        self.holds[anchor_bucket_index].take()
    }
}

#[cfg(test)]
mod tests {
    use super::EmptyHold;

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
}
