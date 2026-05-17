use crossbeam_channel::Receiver;
use env_logger::{Builder, Target};
use log::info;
use rt_matchmaking::locations::MATCHMAKER_EGRESS_SOCKET_PATH;
use rt_matchmaking::orchestration::io::spawn_socket_listener;
use rt_matchmaking::validator::validator::{MatchmakingAssignment, MatchmakingAssignmentValidator};

fn main() {
    Builder::from_default_env()
        .target(Target::Stdout)
        .filter_level(log::LevelFilter::Debug)
        .init();

    let mut validator = MatchmakingAssignmentValidator::new();

    let (_, rx): (_, Receiver<Vec<MatchmakingAssignment>>) =
        spawn_socket_listener(MATCHMAKER_EGRESS_SOCKET_PATH);

    let mut batch_count = 0;
    for batch in rx {
        info!("Assignment batch received.");

        batch_count += 1;

        for assignment in batch {
            validator.receive(assignment)
        }

        let perf = validator.evaluate_performance();

        info!("Batch {batch_count} accepted: {perf}");
    }
}
