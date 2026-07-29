use std::time::{Duration, SystemTime};

use env_logger::{Builder, Target};
use log::info;
use rt_matchmaking::locations::MATCHMAKER_INGRESS_SOCKET_PATH;
use rt_matchmaking::orchestration::io::spawn_socket_sender;
use rt_matchmaking::prelude::MatchmakingRequestGenerator;

fn main() {
    Builder::from_default_env()
        .target(Target::Stdout)
        .filter_level(log::LevelFilter::Debug)
        .init();

    const BATCH_COUNT_LIMIT: usize = 100;
    const BATCH_SIZE: usize = 1_000_000;
    const INTERARRIVAL_TIME: u64 = 1_000;

    let mut generator = MatchmakingRequestGenerator::new();
    let start = SystemTime::now();

    let (_, tx) = spawn_socket_sender(MATCHMAKER_INGRESS_SOCKET_PATH);

    for i in 0..BATCH_COUNT_LIMIT {
        tx.send(generator.batch_sample(BATCH_SIZE)).unwrap();

        info!("Batch {i} sent.");

        std::thread::sleep(Duration::from_millis(INTERARRIVAL_TIME));
    }

    println!("Completed in {:?}.", start.elapsed().unwrap());
}
