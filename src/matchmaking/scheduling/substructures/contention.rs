//! Substructure for scheduler contention mechanics.

use crate::matchmaking::scheduling::states::Contended;
use crate::matchmaking::utils::OffsetIndexedSlice;

pub struct Contention {
    contenders: Box<[Vec<Contended>]>,
}

impl Contention {
    pub fn new(bucket_count: usize) -> Contention {
        let contenders = (0..bucket_count).map(|_| Vec::new()).collect();

        Contention { contenders }
    }

    pub fn contend_ticket(&mut self, contended: Contended, contended_bucket_index: usize) {
        self.contenders[contended_bucket_index].push(contended);
    }

    pub fn flush_contenders<'a>(&'a mut self, base: usize, limit: usize) -> Vec<Contended> {
        let mut flushed = Vec::new();

        OffsetIndexedSlice::new_mut_view(&mut self.contenders, base, limit)
            .map_into(|contenders| contenders.drain(..).collect())
            .map_in_place(|v| flushed.append(v));

        flushed
    }
}

#[cfg(test)]
mod tests {
    use super::Contention;
    use crate::matchmaking::scheduling::states::new_empty_ticket;

    #[test]
    fn flushes_all_contenders_in_the_released_window() {
        let mut contention = Contention::new(5);

        contention.contend_ticket(new_empty_ticket(0).ready().contended(), 0);
        contention.contend_ticket(new_empty_ticket(1).ready().contended(), 1);
        contention.contend_ticket(new_empty_ticket(4).ready().contended(), 4);

        let flushed = contention.flush_contenders(1, 1);
        let anchors: Vec<_> = flushed
            .into_iter()
            .map(|ticket| ticket.anchor_bucket_index())
            .collect();

        assert_eq!(anchors.len(), 2);
        assert!(anchors.contains(&0));
        assert!(anchors.contains(&1));
        assert!(contention.flush_contenders(1, 1).is_empty());
        assert_eq!(contention.flush_contenders(4, 0).len(), 1);
    }
}
