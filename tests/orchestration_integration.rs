use std::time::Duration;

use rt_matchmaking::matchmaking::ground_allocation::GroundAllocationControlSignals;
use rt_matchmaking::matchmaking::scheduling::leases::Lease;
use rt_matchmaking::matchmaking::scheduling::request_matrix::{Postbox, RequestMatrix};
use rt_matchmaking::matchmaking::scheduling::tickets::Ticket;
use rt_matchmaking::matchmaking::scheduling::{Assigned, Completed};
use rt_matchmaking::matchmaking::utils::OffsetIndexedSlice;
use rt_matchmaking::orchestration::events::{GroundAllocationEvent, SchedulerEvent};
use rt_matchmaking::orchestration::workers::{self, ChannelPair, GroundAllocatorInitialisation};
use rt_matchmaking::prelude::MatchmakingRequest;
use rt_matchmaking::validator::validator::MatchmakingAssignment;

fn request(request_id: u32, skill_rating: u32) -> MatchmakingRequest {
    MatchmakingRequest {
        request_id,
        skill_rating,
        submission_timestamp: Duration::from_secs(request_id as u64),
    }
}

fn populate_bucket(postbox: &mut Postbox, bucket: usize, count: usize) {
    for i in 0..count {
        postbox
            .try_post(bucket, request(i as u32 + 1, 100))
            .unwrap();
    }
}

fn make_assigned(
    matrix: &mut RequestMatrix,
    anchor: usize,
    delta: usize,
    eligible: &[usize],
) -> Assigned {
    let lease = Lease::try_acquire(matrix, anchor, delta).unwrap();
    let window_eligible_request_counts =
        OffsetIndexedSlice::from_slice(eligible, anchor, delta).unwrap();
    let mut ticket = Ticket::new(anchor);
    ticket.delta = delta;
    Assigned {
        ticket,
        lease,
        ground_allocation_control_signals: GroundAllocationControlSignals {
            maximum_matchings: 1,
            requests_per_matching: 10,
            window_eligible_request_counts,
        },
    }
}

struct GaHarness {
    tx_to_ga: crossbeam_channel::Sender<SchedulerEvent>,
    rx_from_ga: crossbeam_channel::Receiver<GroundAllocationEvent>,
    handle: std::thread::JoinHandle<()>,
    _egress_rx: crossbeam_channel::Receiver<Vec<MatchmakingAssignment>>,
}

impl GaHarness {
    fn spawn(id: usize) -> Self {
        let (tx_to_ga, rx_from_scheduler) = crossbeam_channel::bounded::<SchedulerEvent>(1);
        let (tx_to_scheduler, rx_from_ga) = crossbeam_channel::bounded::<GroundAllocationEvent>(1);
        let (egress_tx, egress_rx) = crossbeam_channel::unbounded();

        let init = GroundAllocatorInitialisation {
            id,
            channel_pair: ChannelPair {
                tx: tx_to_scheduler,
                rx: rx_from_scheduler,
            },
            egress_sender: egress_tx,
        };

        let handle = std::thread::spawn(move || {
            workers::ground_allocator_loop(init);
        });

        GaHarness {
            tx_to_ga,
            rx_from_ga,
            handle,
            _egress_rx: egress_rx,
        }
    }

    fn expect_ready(&self) {
        match self.rx_from_ga.recv().unwrap() {
            GroundAllocationEvent::Ready => {}
            _ => panic!("expected Ready event"),
        }
    }

    fn send_assignment(&self, assigned: Assigned) {
        self.tx_to_ga
            .send(SchedulerEvent::Assignment(assigned))
            .unwrap();
    }

    fn expect_complete(&self) -> Completed {
        match self.rx_from_ga.recv().unwrap() {
            GroundAllocationEvent::Complete(completed) => completed,
            _ => panic!("expected Complete event"),
        }
    }

    fn join(self) {
        drop(self.tx_to_ga);
        drop(self.rx_from_ga);
        self.handle.join().unwrap();
    }
}

#[test]
fn ga_loop_receives_assignment_and_returns_complete() {
    let (mut matrix, mut postbox) = RequestMatrix::new(1, 20);
    populate_bucket(&mut postbox, 0, 10);

    let assigned = make_assigned(&mut matrix, 0, 0, &[10]);

    let ga = GaHarness::spawn(0);
    ga.expect_ready();
    ga.send_assignment(assigned);
    let completed = ga.expect_complete();

    assert_eq!(completed.local_feedback.successful_matchings, 1);
    assert_eq!(completed.local_feedback.furthest_bucket_distance, 0);
    assert_eq!(
        *completed
            .local_feedback
            .drained_counts
            .get_offset(0)
            .unwrap(),
        10
    );

    ga.join();
}

#[test]
fn ga_loop_handles_two_consecutive_assignments() {
    let (mut matrix, mut postbox) = RequestMatrix::new(2, 20);
    populate_bucket(&mut postbox, 0, 10);
    populate_bucket(&mut postbox, 1, 10);

    let ga = GaHarness::spawn(0);

    ga.expect_ready();
    let assigned_0 = make_assigned(&mut matrix, 0, 0, &[10, 0]);
    ga.send_assignment(assigned_0);
    let completed_0 = ga.expect_complete();
    assert_eq!(completed_0.local_feedback.successful_matchings, 1);

    ga.expect_ready();
    let assigned_1 = make_assigned(&mut matrix, 1, 0, &[0, 10]);
    ga.send_assignment(assigned_1);
    let completed_1 = ga.expect_complete();
    assert_eq!(completed_1.local_feedback.successful_matchings, 1);

    ga.join();
}

#[test]
fn ga_loop_releases_lease_with_remaining_data_on_complete() {
    let (mut matrix, mut postbox) = RequestMatrix::new(1, 20);
    populate_bucket(&mut postbox, 0, 20);

    let assigned = make_assigned(&mut matrix, 0, 0, &[20]);

    let ga = GaHarness::spawn(0);
    ga.expect_ready();
    ga.send_assignment(assigned);
    let completed = ga.expect_complete();

    assert_eq!(completed.local_feedback.successful_matchings, 1);
    assert_eq!(
        *completed
            .local_feedback
            .drained_counts
            .get_offset(0)
            .unwrap(),
        10
    );

    let remaining = completed.lease.as_ref().unwrap().try_peek(0).unwrap();
    assert_eq!(remaining.request_id, 11);

    ga.join();
}

#[test]
fn ga_loop_exits_when_channel_is_dropped() {
    let ga = GaHarness::spawn(0);
    ga.expect_ready();

    drop(ga.tx_to_ga);
    drop(ga.rx_from_ga);

    ga.handle.join().unwrap();
}
