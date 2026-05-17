use std::time::Duration;

use statrs::distribution::Normal;

#[derive(Debug, Clone, Copy)]
pub struct MatchmakingQueueParameters {
    // skill rating distribution
    skill_rating_range: u32,
    distribution: Normal,

    // matching parameters
    requests_per_matching: usize,

    // bucketing parameters
    bucket_width: u32,
    bucket_capacity: usize,

    // static control parameters
    expected_time_to_matching: Duration,
    delta_ceiling: usize,
}

impl MatchmakingQueueParameters {
    fn defaults() -> Self {
        Self {
            skill_rating_range: 2500,
            distribution: Normal::new(1000.0, 200.0).unwrap(),
            requests_per_matching: 10,
            bucket_width: 50,
            bucket_capacity: 1000,
            expected_time_to_matching: Duration::from_mins(3),
            delta_ceiling: 5,
        }
    }

    pub fn builder() -> MatchmakingQueueParametersBuilder {
        MatchmakingQueueParametersBuilder::default()
    }

    #[inline]
    pub fn skill_rating_range(&self) -> u32 {
        self.skill_rating_range
    }

    #[inline]
    pub fn distribution(&self) -> &Normal {
        &self.distribution
    }

    #[inline]
    pub fn requests_per_matching(&self) -> usize {
        self.requests_per_matching
    }

    #[inline]
    pub fn bucket_width(&self) -> u32 {
        self.bucket_width
    }

    #[inline]
    pub fn bucket_capacity(&self) -> usize {
        self.bucket_capacity
    }

    #[inline]
    pub fn expected_time_to_matching(&self) -> Duration {
        self.expected_time_to_matching
    }

    #[inline]
    pub fn delta_ceiling(&self) -> usize {
        self.delta_ceiling
    }

    #[inline]
    pub fn bucket_count(&self) -> usize {
        (self.skill_rating_range / self.bucket_width) as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchmakingQueueParametersBuildError {
    ZeroSkillRatingRange,
    ZeroRequestsPerMatching,
    ZeroBucketWidth,
    ZeroBucketCapacity,
    ZeroExpectedTimeToMatching,
    UnalignedBucketWidth,
}

#[derive(Debug, Clone, Copy)]
pub struct MatchmakingQueueParametersBuilder {
    skill_rating_range: u32,
    distribution: Normal,
    requests_per_matching: usize,
    bucket_width: u32,
    bucket_capacity: usize,
    expected_time_to_matching: Duration,
    delta_ceiling: usize,
}

impl Default for MatchmakingQueueParametersBuilder {
    fn default() -> Self {
        let defaults = MatchmakingQueueParameters::defaults();

        Self {
            skill_rating_range: defaults.skill_rating_range,
            distribution: defaults.distribution,
            requests_per_matching: defaults.requests_per_matching,
            bucket_width: defaults.bucket_width,
            bucket_capacity: defaults.bucket_capacity,
            expected_time_to_matching: defaults.expected_time_to_matching,
            delta_ceiling: defaults.delta_ceiling,
        }
    }
}

impl MatchmakingQueueParametersBuilder {
    pub fn skill_rating_range(mut self, skill_rating_range: u32) -> Self {
        self.skill_rating_range = skill_rating_range;
        self
    }

    pub fn distribution(mut self, distribution: Normal) -> Self {
        self.distribution = distribution;
        self
    }

    pub fn requests_per_matching(mut self, requests_per_matching: usize) -> Self {
        self.requests_per_matching = requests_per_matching;
        self
    }

    pub fn bucket_width(mut self, bucket_width: u32) -> Self {
        self.bucket_width = bucket_width;
        self
    }

    pub fn bucket_capacity(mut self, bucket_capacity: usize) -> Self {
        self.bucket_capacity = bucket_capacity;
        self
    }

    pub fn expected_time_to_matching(mut self, expected_time_to_matching: Duration) -> Self {
        self.expected_time_to_matching = expected_time_to_matching;
        self
    }

    pub fn delta_ceiling(mut self, delta_ceiling: usize) -> Self {
        self.delta_ceiling = delta_ceiling;
        self
    }

    pub fn build(self) -> Result<MatchmakingQueueParameters, MatchmakingQueueParametersBuildError> {
        if self.skill_rating_range == 0 {
            return Err(MatchmakingQueueParametersBuildError::ZeroSkillRatingRange);
        }
        if self.requests_per_matching == 0 {
            return Err(MatchmakingQueueParametersBuildError::ZeroRequestsPerMatching);
        }
        if self.bucket_width == 0 {
            return Err(MatchmakingQueueParametersBuildError::ZeroBucketWidth);
        }
        if self.bucket_capacity == 0 {
            return Err(MatchmakingQueueParametersBuildError::ZeroBucketCapacity);
        }
        if self.expected_time_to_matching.is_zero() {
            return Err(MatchmakingQueueParametersBuildError::ZeroExpectedTimeToMatching);
        }
        if !self.skill_rating_range.is_multiple_of(self.bucket_width) {
            return Err(MatchmakingQueueParametersBuildError::UnalignedBucketWidth);
        }

        Ok(MatchmakingQueueParameters {
            skill_rating_range: self.skill_rating_range,
            distribution: self.distribution,
            requests_per_matching: self.requests_per_matching,
            bucket_width: self.bucket_width,
            bucket_capacity: self.bucket_capacity,
            expected_time_to_matching: self.expected_time_to_matching,
            delta_ceiling: self.delta_ceiling,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{MatchmakingQueueParameters, MatchmakingQueueParametersBuildError};

    #[test]
    fn builder_uses_documented_defaults() {
        let config = MatchmakingQueueParameters::builder().build().unwrap();

        assert_eq!(config.skill_rating_range(), 2500);
        assert_eq!(config.requests_per_matching(), 10);
        assert_eq!(config.bucket_width(), 50);
        assert_eq!(config.bucket_capacity(), 1000);
        assert_eq!(config.expected_time_to_matching(), Duration::from_mins(3));
        assert_eq!(config.delta_ceiling(), 5);
        assert_eq!(config.bucket_count(), 50);
    }

    #[test]
    fn builder_rejects_zero_and_misaligned_values() {
        assert_eq!(
            MatchmakingQueueParameters::builder()
                .bucket_width(0)
                .build()
                .unwrap_err(),
            MatchmakingQueueParametersBuildError::ZeroBucketWidth
        );
        assert_eq!(
            MatchmakingQueueParameters::builder()
                .expected_time_to_matching(Duration::ZERO)
                .build()
                .unwrap_err(),
            MatchmakingQueueParametersBuildError::ZeroExpectedTimeToMatching
        );
        assert_eq!(
            MatchmakingQueueParameters::builder()
                .skill_rating_range(2550)
                .bucket_width(128)
                .build()
                .unwrap_err(),
            MatchmakingQueueParametersBuildError::UnalignedBucketWidth
        );
    }

    #[test]
    fn builder_rejects_zero_skill_rating_range() {
        assert_eq!(
            MatchmakingQueueParameters::builder()
                .skill_rating_range(0)
                .build()
                .unwrap_err(),
            MatchmakingQueueParametersBuildError::ZeroSkillRatingRange
        );
    }

    #[test]
    fn builder_rejects_zero_requests_per_matching() {
        assert_eq!(
            MatchmakingQueueParameters::builder()
                .requests_per_matching(0)
                .build()
                .unwrap_err(),
            MatchmakingQueueParametersBuildError::ZeroRequestsPerMatching
        );
    }

    #[test]
    fn builder_rejects_zero_bucket_capacity() {
        assert_eq!(
            MatchmakingQueueParameters::builder()
                .bucket_capacity(0)
                .build()
                .unwrap_err(),
            MatchmakingQueueParametersBuildError::ZeroBucketCapacity
        );
    }

    #[test]
    fn builder_overrides_selected_fields() {
        let config = MatchmakingQueueParameters::builder()
            .skill_rating_range(3000)
            .bucket_width(100)
            .bucket_capacity(128)
            .requests_per_matching(5)
            .delta_ceiling(7)
            .build()
            .unwrap();

        assert_eq!(config.skill_rating_range(), 3000);
        assert_eq!(config.bucket_width(), 100);
        assert_eq!(config.bucket_capacity(), 128);
        assert_eq!(config.requests_per_matching(), 5);
        assert_eq!(config.delta_ceiling(), 7);
        assert_eq!(config.bucket_count(), 30);
    }
}
