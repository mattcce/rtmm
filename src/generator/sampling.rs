//! Fast sampling from normal probability distribution.
use std::f32::consts::PI;

use rand::prelude::*;

/// Exponential distribution generator for the Exp(1) distribution.
struct ExponentialDistributionGenerator {
    generator: Box<dyn Rng>,
}

impl ExponentialDistributionGenerator {
    /// Creates a new exponential distribution generator object.
    fn new() -> ExponentialDistributionGenerator {
        ExponentialDistributionGenerator {
            generator: Box::new(rand::make_rng::<SmallRng>()),
        }
    }

    /// Samples from an Exp(1) distribution.
    fn next(&mut self) -> f32 {
        // Inverse-transform method

        let u = self.generator.random_range(f32::MIN_POSITIVE..1.0);

        // inverse cdf of Exp(1) distribution
        -u.ln()
    }
}

/// Normal distribution generator for the N(0, 1) (standard normal) distribution.
pub struct StandardNormalDistributionGenerator {
    exponential_generator: ExponentialDistributionGenerator,
    uniform_generator: Box<dyn Rng>,
}

impl StandardNormalDistributionGenerator {
    /// Creates a new standard normal distribution generator object.
    pub fn new() -> StandardNormalDistributionGenerator {
        StandardNormalDistributionGenerator {
            exponential_generator: ExponentialDistributionGenerator::new(),
            uniform_generator: Box::new(rand::make_rng::<SmallRng>()),
        }
    }

    /// Samples from a standard normal distribution.
    pub fn next(&mut self) -> f32 {
        // Accept-reject algorithm
        // Sample a half-normal random variable using an exponential proposal,
        // then assign a uniformly random sign to recover symmetry.

        loop {
            let y = self.exponential_generator.next();
            let threshold = (-0.5 * (y - 1.0) * (y - 1.0)).exp();

            if self.uniform_generator.random::<f32>() <= threshold {
                let sign = if self.uniform_generator.random::<bool>() {
                    1.0
                } else {
                    -1.0
                };

                return sign * y;
            }
        }
    }

    /// Computes the pdf of the standard normal distribution at a particular point.
    pub fn pdf(t: f32) -> f32 {
        (-0.5 * t * t).exp() / (2.0 * PI).sqrt()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_normal_produces_both_signs() {
        let mut generator = StandardNormalDistributionGenerator::new();

        let mut saw_positive = false;
        let mut saw_negative = false;

        for _ in 0..2048 {
            let sample = generator.next();
            saw_positive |= sample > 0.0;
            saw_negative |= sample < 0.0;

            if saw_positive && saw_negative {
                return;
            }
        }

        panic!("standard normal sampler never produced both positive and negative samples");
    }

    #[test]
    fn standard_normal_has_mean_near_zero() {
        let mut generator = StandardNormalDistributionGenerator::new();

        let sample_count = 20_000;
        let sum: f32 = (0..sample_count).map(|_| generator.next()).sum();
        let mean = sum / sample_count as f32;

        assert!(
            mean.abs() < 0.1,
            "expected sample mean near zero, got {mean}"
        );
    }

    #[test]
    fn standard_normal_has_variance_near_one() {
        let mut generator = StandardNormalDistributionGenerator::new();

        let sample_count = 20_000;
        let mut sum = 0.0f32;
        let mut sum_of_squares = 0.0f32;

        for _ in 0..sample_count {
            let sample = generator.next();
            sum += sample;
            sum_of_squares += sample * sample;
        }

        let mean = sum / sample_count as f32;
        let variance = sum_of_squares / sample_count as f32 - mean * mean;

        assert!(
            (variance - 1.0).abs() < 0.15,
            "expected sample variance near one, got {variance}"
        );
    }
}
