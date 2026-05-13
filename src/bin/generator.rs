use std::time::SystemTime;

use rt_matchmaking::prelude::MatchmakingRequestGenerator;

fn main() {
    let mut generator = MatchmakingRequestGenerator::new();

    let start = SystemTime::now();

    generator.batch_sample(1_000_000);

    print!("Completed in {:?}.", start.elapsed().unwrap());
}
