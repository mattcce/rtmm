//! Matchmaking request generation.

use std::time::SystemTime;

use super::sampling::StandardNormalDistributionGenerator;

#[derive(Debug, Clone, Copy)]
pub struct MatchmakingRequest {
    pub request_id: u32,
    pub skill_rating: u32,
    pub submission_timestamp: SystemTime,
}

/// Matchmaking request generator.
pub struct MatchmakingRequestGenerator {
    skill_rating_random_variable: SkillRatingRandomVariable,
    generated_sample_count: u32,
}

impl MatchmakingRequestGenerator {
    pub fn new() -> MatchmakingRequestGenerator {
        MatchmakingRequestGenerator {
            skill_rating_random_variable: SkillRatingRandomVariable::new(),
            generated_sample_count: 0,
        }
    }

    pub fn sample(&mut self) -> MatchmakingRequest {
        self.generated_sample_count += 1;
        MatchmakingRequest {
            request_id: self.generated_sample_count,
            skill_rating: self.skill_rating_random_variable.sample(),
            submission_timestamp: SystemTime::now(),
        }
    }

    pub fn batch_sample(&mut self, batch_size: usize) -> Vec<MatchmakingRequest> {
        let mut request_batch = Vec::with_capacity(batch_size);

        for _ in 0..batch_size {
            request_batch.push(self.sample());
        }

        request_batch
    }
}

/// Skill rating random variable. Generates skill ratings from a normal
/// distribution of mean 1000 and variance 200^2.
struct SkillRatingRandomVariable {
    generator: StandardNormalDistributionGenerator,
}

impl SkillRatingRandomVariable {
    const MEAN: f32 = 1000.0;
    const STANDARD_DEVIATION: f32 = 200.0;

    fn new() -> SkillRatingRandomVariable {
        SkillRatingRandomVariable {
            generator: StandardNormalDistributionGenerator::new(),
        }
    }

    fn sample(&mut self) -> u32 {
        (Self::MEAN + Self::STANDARD_DEVIATION * self.generator.next())
            .max(0.0)
            .round() as u32
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn generator() -> MatchmakingRequestGenerator {
        MatchmakingRequestGenerator::new()
    }

    #[test]
    fn batch_sample_returns_requested_number_of_requests() {
        let mut generator = generator();

        let batch = generator.batch_sample(8);

        assert_eq!(batch.len(), 8);
    }

    #[test]
    fn request_ids_are_unique_within_one_generator() {
        let mut generator = generator();

        let batch = generator.batch_sample(10_000);
        let unique_ids: HashSet<u32> = batch.iter().map(|request| request.request_id).collect();

        assert_eq!(
            unique_ids.len(),
            batch.len(),
            "expected generated request ids to be unique within one generator instance"
        );
    }

    #[test]
    fn sampled_skill_ratings_are_centered_on_documented_distribution() {
        let mut generator = generator();

        let sample_count = 20_000;
        let total_rating: u64 = (0..sample_count)
            .map(|_| u64::from(generator.sample().skill_rating))
            .sum();
        let mean_rating = total_rating as f64 / sample_count as f64;

        assert!(
            (mean_rating - 1000.0).abs() < 40.0,
            "expected mean skill rating near 1000, got {mean_rating}"
        );
    }
}
