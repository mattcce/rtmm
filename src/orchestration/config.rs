//! Configuration for setting up multithreading behaviour.

use crate::matchmaking::config::MatchmakingQueueParameters;

pub struct SetupParameters {
    pub ground_allocator_thread_count: usize,
    pub matchmaking_queue_parameters: MatchmakingQueueParameters,
}
