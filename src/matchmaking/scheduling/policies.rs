use std::cmp::max;
use std::time::{Duration, SystemTime};

use crate::matchmaking::config::MatchmakingQueueParameters;
use crate::matchmaking::ground_allocation::GroundAllocationSessionSummary;
use crate::matchmaking::scheduling::scheduling_table::SchedulerBookkeeping;
use crate::matchmaking::scheduling::tickets::Ticket;
use crate::matchmaking::utils::OffsetIndexedSlice;

/// Retry and resize policies.
pub fn compute_target_delta(
    ticket: &Ticket,
    bookkeeping: &SchedulerBookkeeping,
    queue_parameters: &MatchmakingQueueParameters,
    local_feedback: &GroundAllocationSessionSummary,
    global_feedback: &SchedulerGlobalFeedback,
) -> usize {
    let SchedulerBookkeeping {
        bucket_request_counts,
        proportion_table,
    } = bookkeeping;

    let SchedulerGlobalFeedback {
        average_global_arrival_rate,
        ..
    } = global_feedback;

    let GroundAllocationSessionSummary {
        completion_timestamp,
        ..
    } = local_feedback;

    let target_remaining_time = match ticket.oldest_request_timestamp {
        Some(oldest_request_timestamp) => max(
            Duration::from_nanos(1),
            queue_parameters
                .expected_time_to_matching()
                .checked_sub(
                    completion_timestamp
                        .duration_since(oldest_request_timestamp)
                        .unwrap(),
                )
                .unwrap_or(Duration::ZERO),
        ),
        None => queue_parameters.expected_time_to_matching(),
    };

    let proportion_minimum_threshold = (queue_parameters
        .requests_per_matching()
        .saturating_sub(bucket_request_counts[ticket.anchor_bucket_index()]))
        as f64
        / (target_remaining_time.as_secs_f64() * average_global_arrival_rate + f64::MIN_POSITIVE);

    for delta in 0..=queue_parameters.delta_ceiling() {
        let proportion = compute_proportion(proportion_table, ticket.anchor_bucket_index(), delta);

        if proportion >= proportion_minimum_threshold {
            return delta;
        }
    }

    queue_parameters.delta_ceiling()
}

fn compute_proportion(
    proportion_table: &Box<[f64]>,
    anchor_bucket_index: usize,
    delta: usize,
) -> f64 {
    let view =
        OffsetIndexedSlice::from_slice(&proportion_table, anchor_bucket_index, delta).unwrap();

    view.iter().sum()
}

pub fn compute_backoff_duration(
    ticket: &Ticket,
    bookkeeping: &SchedulerBookkeeping,
    queue_parameters: &MatchmakingQueueParameters,
    _local_feedback: &GroundAllocationSessionSummary,
    global_feedback: &SchedulerGlobalFeedback,
) -> Duration {
    const MAXIMUM_BACKOFF_DURATION_SECONDS: f64 = 10.0;

    let SchedulerGlobalFeedback {
        average_global_arrival_rate,
        ..
    } = global_feedback;

    let proportion = compute_proportion(
        &bookkeeping.proportion_table,
        ticket.anchor_bucket_index(),
        ticket.delta,
    );

    let expected_wait_seconds = queue_parameters.requests_per_matching() as f64
        / (proportion * average_global_arrival_rate + f64::MIN_POSITIVE);

    if !expected_wait_seconds.is_finite() || expected_wait_seconds >= MAXIMUM_BACKOFF_DURATION_SECONDS {
        Duration::from_secs_f64(MAXIMUM_BACKOFF_DURATION_SECONDS)
    } else {
        Duration::from_secs_f64(expected_wait_seconds)
    }
}

/// Global control signals needed for resize and retry policies.
#[derive(Debug, Clone, Copy)]
pub struct SchedulerGlobalFeedback {
    average_batch_interarrival_time: f64,
    last_batch_arrival_timestamp: Option<SystemTime>,
    average_global_arrival_rate: f64,
}

impl SchedulerGlobalFeedback {
    const ALPHA: f64 = 0.5; // EWMA weight

    pub fn new() -> SchedulerGlobalFeedback {
        SchedulerGlobalFeedback {
            average_batch_interarrival_time: 0.0,
            last_batch_arrival_timestamp: None,
            average_global_arrival_rate: 0.0,
        }
    }

    pub fn record_new_batch(&mut self, arrival_timestamp: SystemTime, batch_size: usize) {
        match self.last_batch_arrival_timestamp {
            Some(last_batch_arrival_timestamp) => {
                let most_recent_interarrival_time = arrival_timestamp
                    .duration_since(last_batch_arrival_timestamp)
                    .unwrap()
                    .as_secs_f64();
                let most_recent_arrival_rate: f64 =
                    batch_size as f64 / most_recent_interarrival_time;
                self.average_batch_interarrival_time = Self::ewma(
                    self.average_batch_interarrival_time,
                    most_recent_interarrival_time,
                    Self::ALPHA,
                );
                self.average_global_arrival_rate = Self::ewma(
                    self.average_global_arrival_rate,
                    most_recent_arrival_rate,
                    Self::ALPHA,
                );
            }
            None => (),
        }

        self.last_batch_arrival_timestamp = Some(arrival_timestamp);
    }

    fn ewma(last: f64, current: f64, alpha: f64) -> f64 {
        (1.0 - alpha) * last + alpha * current
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::{SchedulerGlobalFeedback, compute_backoff_duration, compute_target_delta};
    use crate::matchmaking::config::MatchmakingQueueParameters;
    use crate::matchmaking::ground_allocation::GroundAllocationSessionSummary;
    use crate::matchmaking::scheduling::scheduling_table::SchedulerBookkeeping;
    use crate::matchmaking::scheduling::tickets::Ticket;
    use crate::matchmaking::utils::OffsetIndexedSlice;

    fn feedback_with_rate(batch_size: usize, interval_seconds: u64) -> SchedulerGlobalFeedback {
        let mut feedback = SchedulerGlobalFeedback::new();
        feedback.record_new_batch(UNIX_EPOCH + Duration::from_secs(1), batch_size);
        feedback.record_new_batch(
            UNIX_EPOCH + Duration::from_secs(1 + interval_seconds),
            batch_size,
        );
        feedback
    }

    #[test]
    fn target_delta_grows_until_the_clipped_window_mass_is_sufficient() {
        let queue_parameters = MatchmakingQueueParameters::builder()
            .skill_rating_range(300)
            .bucket_width(100)
            .requests_per_matching(6)
            .expected_time_to_matching(Duration::from_secs(4))
            .delta_ceiling(2)
            .build()
            .unwrap();
        let bookkeeping = SchedulerBookkeeping {
            bucket_request_counts: vec![0, 0, 0].into_boxed_slice(),
            proportion_table: vec![0.2, 0.5, 0.3].into_boxed_slice(),
        };
        let local_feedback = GroundAllocationSessionSummary {
            successful_matchings: 0,
            furthest_bucket_distance: 0,
            completion_timestamp: UNIX_EPOCH + Duration::from_secs(10),
            drained_counts: OffsetIndexedSlice::from_slice(&[0usize, 0, 0], 0, 0).unwrap(),
        };
        let feedback = feedback_with_rate(10, 2);
        let ticket = Ticket::new(0);

        assert_eq!(
            compute_target_delta(
                &ticket,
                &bookkeeping,
                &queue_parameters,
                &local_feedback,
                &feedback,
            ),
            1
        );
    }

    #[test]
    fn backoff_duration_is_capped() {
        let queue_parameters = MatchmakingQueueParameters::builder()
            .skill_rating_range(300)
            .bucket_width(100)
            .requests_per_matching(100)
            .build()
            .unwrap();
        let bookkeeping = SchedulerBookkeeping {
            bucket_request_counts: vec![0, 0, 0].into_boxed_slice(),
            proportion_table: vec![0.2, 0.5, 0.3].into_boxed_slice(),
        };
        let local_feedback = GroundAllocationSessionSummary {
            successful_matchings: 0,
            furthest_bucket_distance: 0,
            completion_timestamp: UNIX_EPOCH + Duration::from_secs(10),
            drained_counts: OffsetIndexedSlice::from_slice(&[0usize], 0, 0).unwrap(),
        };
        let feedback = feedback_with_rate(2, 4);
        let ticket = Ticket::new(1);

        assert_eq!(
            compute_backoff_duration(
                &ticket,
                &bookkeeping,
                &queue_parameters,
                &local_feedback,
                &feedback,
            ),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn backoff_duration_is_total_at_zero_observed_arrival_rate() {
        let queue_parameters = MatchmakingQueueParameters::builder()
            .skill_rating_range(300)
            .bucket_width(100)
            .build()
            .unwrap();
        let bookkeeping = SchedulerBookkeeping {
            bucket_request_counts: vec![1, 0, 0].into_boxed_slice(),
            proportion_table: vec![0.2, 0.5, 0.3].into_boxed_slice(),
        };
        let local_feedback = GroundAllocationSessionSummary {
            successful_matchings: 0,
            furthest_bucket_distance: 0,
            completion_timestamp: UNIX_EPOCH + Duration::from_secs(10),
            drained_counts: OffsetIndexedSlice::from_slice(&[0usize], 0, 0).unwrap(),
        };
        let feedback = SchedulerGlobalFeedback::new();
        let ticket = Ticket::new(0);

        assert_eq!(
            compute_backoff_duration(
                &ticket,
                &bookkeeping,
                &queue_parameters,
                &local_feedback,
                &feedback,
            ),
            Duration::from_secs(10)
        );
    }
}
