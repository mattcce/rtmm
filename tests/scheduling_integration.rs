use std::time::{Duration, UNIX_EPOCH};

use rt_matchmaking::matchmaking::config::MatchmakingQueueParameters;
use rt_matchmaking::matchmaking::ground_allocation::{
    GroundAllocationControlSignals, GroundAllocator,
};
use rt_matchmaking::matchmaking::scheduling::Assigned;
use rt_matchmaking::matchmaking::scheduling::scheduling_table::SchedulingTable;
use rt_matchmaking::prelude::MatchmakingRequest;

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

fn control_signals(
    assigned: &Assigned,
    maximum_matchings: usize,
) -> GroundAllocationControlSignals {
    GroundAllocationControlSignals {
        maximum_matchings,
        requests_per_matching: 10,
        window_eligible_request_counts: assigned.eligible_request_counts.map(|count| *count),
    }
}

#[test]
fn successful_round_trip_requeues_remaining_anchor_work() {
    let mut table = SchedulingTable::new(test_queue_parameters());
    let requests = (1..=20).map(|request_id| request(request_id, 50)).collect();

    table.post_request_batch(requests).unwrap();

    let (matchings, ticket, summary) = {
        let assigned = table.next_assignment().unwrap();
        let signals = control_signals(&assigned, 1);
        let allocator = GroundAllocator::new(assigned, signals);
        let (matchings, assigned, summary) = allocator.run_allocation();
        (matchings, assigned, summary)
    };

    assert_eq!(matchings.len(), 1);
    assert_eq!(matchings[0].grouped_requests.len(), 10);

    table.readmit_assigned(ticket, summary);

    let reassigned = table.next_assignment().unwrap();
    assert_eq!(reassigned.anchor_bucket_index(), 0);
    assert_eq!(reassigned.lease.try_peek(0).unwrap().request_id, 11);
}

#[test]
fn unsuccessful_round_trip_waits_ticket() {
    let mut table = SchedulingTable::new(test_queue_parameters());

    table.post_request_batch(vec![request(1, 50)]).unwrap();

    let (ticket, summary) = {
        let assigned = table.next_assignment().unwrap();
        let signals = control_signals(&assigned, 1);
        let allocator = GroundAllocator::new(assigned, signals);
        let (_matchings, assigned, summary) = allocator.run_allocation();
        (assigned, summary)
    };

    assert_eq!(summary.successful_matchings, 0);

    table.readmit_assigned(ticket, summary);

    assert!(table.next_assignment().is_none());
}

#[test]
fn drained_anchor_wakes_again_on_new_batch() {
    let mut table = SchedulingTable::new(test_queue_parameters());
    let requests = (1..=10).map(|request_id| request(request_id, 50)).collect();

    table.post_request_batch(requests).unwrap();

    let (ticket, summary) = {
        let assigned = table.next_assignment().unwrap();
        let signals = control_signals(&assigned, 1);
        let allocator = GroundAllocator::new(assigned, signals);
        let (_matchings, assigned, summary) = allocator.run_allocation();
        (assigned, summary)
    };

    table.readmit_assigned(ticket, summary);

    assert!(table.next_assignment().is_none());

    table.post_request_batch(vec![request(11, 50)]).unwrap();

    let reassigned = table.next_assignment().unwrap();
    assert_eq!(reassigned.anchor_bucket_index(), 0);
    assert_eq!(reassigned.lease.try_peek(0).unwrap().request_id, 11);
}
