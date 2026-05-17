use std::time::Duration;

use rt_matchmaking::matchmaking::config::MatchmakingQueueParameters;
use rt_matchmaking::matchmaking::ground_allocation::GroundAllocator;
use rt_matchmaking::matchmaking::scheduling::scheduling_table::SchedulingTable;
use rt_matchmaking::prelude::MatchmakingRequest;
use rt_matchmaking::validator::validator::MatchmakingAssignmentValidator;

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
        .bucket_capacity(50)
        .build()
        .unwrap()
}

#[test]
fn full_roundtrip_from_post_to_validator() {
    let mut table = SchedulingTable::new(test_queue_parameters());

    // post enough requests across 3 adjacent buckets for 3 full matches
    let mut requests: Vec<MatchmakingRequest> = Vec::new();
    for i in 1..=10 {
        requests.push(request(i, 150)); // bucket 1
    }
    for i in 11..=20 {
        requests.push(request(i, 50)); // bucket 0
    }
    for i in 21..=30 {
        requests.push(request(i, 250)); // bucket 2
    }
    table.post_request_batch(requests).unwrap();

    let mut validator = MatchmakingAssignmentValidator::new();

    // run two assignment cycles
    for _ in 0..2 {
        let (matchings, completed) = {
            let assigned = table.next_assignment().unwrap();
            let allocator = GroundAllocator::new(assigned);
            allocator.run_allocation()
        };

        for m in matchings {
            validator.receive(m);
        }

        table.readmit_completed(completed);
    }

    let metrics = validator.evaluate_performance();
    // pipeline ran without panic; metrics produced
    assert!(metrics.rating_variability_average_penalty >= 0.0);
    assert!(metrics.mean_response_time >= 0.0);
    assert!(metrics.evaluated_timestamp > Duration::ZERO);
}

#[test]
fn residency_cycle_exhausts_bucket_then_goes_empty() {
    let mut table = SchedulingTable::new(test_queue_parameters());

    // 30 requests to bucket 0 → should yield 3 matches (10 each) then empty
    let requests: Vec<MatchmakingRequest> =
        (1..=30).map(|i| request(i, 50)).collect();
    table.post_request_batch(requests).unwrap();

    // 1st match
    let completed = {
        let mut assigned = table.next_assignment().unwrap();
        assigned.ground_allocation_control_signals.maximum_matchings = 1;
        let allocator = GroundAllocator::new(assigned);
        let (matchings, completed) = allocator.run_allocation();
        assert_eq!(matchings.len(), 1);
        completed
    };
    assert_eq!(completed.local_feedback.successful_matchings, 1);
    table.readmit_completed(completed);

    // ticket should return to ready (anchor not empty)
    let reassigned = table.next_assignment();
    assert!(reassigned.is_some());

    // 2nd match
    let completed = {
        let mut assigned = reassigned.unwrap();
        assigned.ground_allocation_control_signals.maximum_matchings = 1;
        let allocator = GroundAllocator::new(assigned);
        let (matchings, completed) = allocator.run_allocation();
        assert_eq!(matchings.len(), 1);
        completed
    };
    assert_eq!(completed.local_feedback.successful_matchings, 1);
    table.readmit_completed(completed);

    // 3rd match — anchor now empty after 30 requests consumed
    let completed = {
        let mut assigned = table.next_assignment().unwrap();
        assigned.ground_allocation_control_signals.maximum_matchings = 1;
        let allocator = GroundAllocator::new(assigned);
        let (matchings, completed) = allocator.run_allocation();
        assert_eq!(matchings.len(), 1);
        completed
    };
    assert_eq!(completed.local_feedback.successful_matchings, 1);
    table.readmit_completed(completed);

    // anchor empty → no more assignments
    assert!(table.next_assignment().is_none());
}
