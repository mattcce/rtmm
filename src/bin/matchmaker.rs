use env_logger::{Builder, Target};
use log::info;
use rt_matchmaking::matchmaking::config::MatchmakingQueueParametersBuilder;
use rt_matchmaking::orchestration::config::SetupParameters;
use rt_matchmaking::orchestration::workers::setup;

fn main() {
    Builder::from_default_env()
        .target(Target::Stdout)
        .filter_level(log::LevelFilter::Debug)
        .init();

    info!("Matchmaker process started.");

    setup(SetupParameters {
        ground_allocator_thread_count: 4,
        matchmaking_queue_parameters: MatchmakingQueueParametersBuilder::default()
            .bucket_capacity(1_000_000)
            .build()
            .unwrap(),
    })
}
