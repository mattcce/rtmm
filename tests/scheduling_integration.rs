use std::time::Duration;

use rt_matchmaking::matchmaking::config::MatchmakingQueueParameters;
use rt_matchmaking::matchmaking::ground_allocation::GroundAllocator;
use rt_matchmaking::matchmaking::scheduling::scheduling_table::SchedulingTable;
use rt_matchmaking::prelude::MatchmakingRequest;

fn request(request_id: u32, skill_rating: u32) -> MatchmakingRequest {
    MatchmakingRequest {
        request_id,
        skill_rating,
        submission_timestamp: Duration::from_secs(request_id as u64),
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
fn successful_round_trip_requeues_remaining_anchor_work() {
    let mut table = SchedulingTable::new(test_queue_parameters());
    let requests = (1..=20).map(|request_id| request(request_id, 50)).collect();

    table.post_request_batch(requests).unwrap();

    let (matchings, completed) = {
        let mut assigned = table.next_assignment().unwrap();
        assigned.ground_allocation_control_signals.maximum_matchings = 1;
        let allocator = GroundAllocator::new(assigned);
        allocator.run_allocation()
    };

    assert_eq!(matchings.len(), 1);
    assert_eq!(matchings[0].grouped_requests.len(), 10);

    table.readmit_completed(completed);

    let reassigned = table.next_assignment().unwrap();
    assert_eq!(reassigned.anchor_bucket_index(), 0);
    assert_eq!(reassigned.lease.try_peek(0).unwrap().request_id, 11);
}

#[test]
fn unsuccessful_round_trip_waits_ticket() {
    let mut table = SchedulingTable::new(test_queue_parameters());

    table.post_request_batch(vec![request(1, 50)]).unwrap();

    let completed = {
        let assigned = table.next_assignment().unwrap();
        let allocator = GroundAllocator::new(assigned);
        let (_matchings, completed) = allocator.run_allocation();
        completed
    };

    assert_eq!(completed.local_feedback.successful_matchings, 0);

    table.readmit_completed(completed);

    assert!(table.next_assignment().is_none());
}

#[test]
fn drained_anchor_wakes_again_on_new_batch() {
    let mut table = SchedulingTable::new(test_queue_parameters());
    let requests = (1..=10).map(|request_id| request(request_id, 50)).collect();

    table.post_request_batch(requests).unwrap();

    let completed = {
        let mut assigned = table.next_assignment().unwrap();
        assigned.ground_allocation_control_signals.maximum_matchings = 1;
        let allocator = GroundAllocator::new(assigned);
        let (_matchings, completed) = allocator.run_allocation();
        completed
    };

    table.readmit_completed(completed);

    assert!(table.next_assignment().is_none());

    table.post_request_batch(vec![request(11, 50)]).unwrap();

    let reassigned = table.next_assignment().unwrap();
    assert_eq!(reassigned.anchor_bucket_index(), 0);
    assert_eq!(reassigned.lease.try_peek(0).unwrap().request_id, 11);
}

#[test]
fn delta_persists_across_successful_matching_cycle() {
    let mut table = SchedulingTable::new(test_queue_parameters());

    // post 20 requests to bucket 0: enough for 2 matches
    let requests: Vec<MatchmakingRequest> = (1..=20).map(|i| request(i, 50)).collect();
    table.post_request_batch(requests).unwrap();

    // set delta to 1 before first assignment to simulate prior resize
    {
        let mut assigned = table.next_assignment().unwrap();
        assigned.ticket.delta = 1;
        assigned.ground_allocation_control_signals.maximum_matchings = 1;
        let allocator = GroundAllocator::new(assigned);
        let (_matchings, completed) = allocator.run_allocation();
        assert_eq!(completed.local_feedback.furthest_bucket_distance, 0);
        assert_eq!(completed.local_feedback.successful_matchings, 1);
        table.readmit_completed(completed);
    }

    // ticket should cycle back to ready and be reassignable
    let reassigned = table.next_assignment().unwrap();
    assert_eq!(reassigned.anchor_bucket_index(), 0);
    // remaining requests: request IDs 11..20
    assert_eq!(reassigned.lease.try_peek(0).unwrap().request_id, 11);
}

#[test]
fn priority_ordering_prefers_oldest_anchor_request() {
    let mut table = SchedulingTable::new(test_queue_parameters());

    // bucket 0: request ID 1 (oldest)
    // bucket 1: request ID 2 (newer)
    table
        .post_request_batch(vec![request(1, 50), request(2, 150)])
        .unwrap();

    // bucket 0 has the oldest request → should be assigned first
    let assigned = table.next_assignment().unwrap();
    assert_eq!(assigned.anchor_bucket_index(), 0);
}
