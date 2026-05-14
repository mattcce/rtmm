use std::cmp::min;

/// Internal-only struct used for ease of computing bounds. Not part of the
/// interface contract for OffsetIndexedSlice.
struct MaterialisedBounds {
    start: usize,
    end_exclusive: usize,
    base: i32,
}

impl MaterialisedBounds {
    fn len(&self) -> usize {
        self.end_exclusive.saturating_sub(self.start)
    }
}

fn materialised_bounds(container_len: usize, base: i32, limit: i32) -> MaterialisedBounds {
    // `OffsetIndexedSlice` stores `base` and `limit` in signed coordinates so
    // that callers can query relative offsets on both sides of the anchor. The
    // only public constructors take `usize`, so both values are nonnegative by
    // construction and convert back to `usize` losslessly here.
    let Ok(base) = usize::try_from(base) else {
        panic!();
    };
    let Ok(limit) = usize::try_from(limit) else {
        panic!();
    };

    // The inclusive left edge of the logical view is `base - limit`. When the
    // view extends past index 0, `saturating_sub` clips it to 0 without a
    // separate empty-range branch.
    let start = base.saturating_sub(limit);

    // The right edge is exclusive, so we compute `base + limit + 1`.
    // `saturating_add` makes that arithmetic total even for very large windows.
    // We do not need to rely on any platform-specific `Vec` size bound here:
    // anything beyond `container_len` is clipped away below, so a saturated
    // upper bound is equivalent to any larger mathematical value.
    let unclipped_end_exclusive = base.saturating_add(limit).saturating_add(1);

    // It is valid for `start > end_exclusive`: that denotes a view that lies
    // entirely to the right of the backing container. Callers must interpret
    // such inverted bounds as an empty materialization by using `len()` rather
    // than slicing directly with `start..end_exclusive`.
    let end_exclusive = min(container_len, unclipped_end_exclusive);

    MaterialisedBounds {
        start,
        end_exclusive,
        // `start` is `base - limit` clipped at 0, so `start <= base` always
        // holds. Therefore `base - start` fits back into `i32`, and this is
        // exactly the anchor index inside the materialised view.
        base: i32::try_from(base - start).unwrap(),
    }
}

/// Bidirectional symmetric limit offset-indexed fixed-length container.
#[derive(Debug)]
pub struct OffsetIndexedSlice<T> {
    container: Box<[T]>,
    base: i32,
    limit: i32,
}

impl<T> OffsetIndexedSlice<T> {
    fn materialised_bounds(&self) -> MaterialisedBounds {
        materialised_bounds(self.container.len(), self.base, self.limit)
    }

    pub fn new(container: Box<[T]>, base: usize, limit: usize) -> OffsetIndexedSlice<T> {
        let Ok(base) = i32::try_from(base) else {
            panic!();
        };
        let Ok(limit) = i32::try_from(limit) else {
            panic!();
        };

        OffsetIndexedSlice {
            container,
            base,
            limit,
        }
    }

    pub fn new_mut_view(
        container: &mut [T],
        base: usize,
        limit: usize,
    ) -> OffsetIndexedSlice<&mut T> {
        let Ok(base) = i32::try_from(base) else {
            panic!();
        };
        let Ok(limit) = i32::try_from(limit) else {
            panic!();
        };

        OffsetIndexedSlice {
            container: container.iter_mut().collect::<Vec<_>>().into_boxed_slice(),
            base,
            limit,
        }
    }

    pub fn zip<U>(self, other: OffsetIndexedSlice<U>) -> OffsetIndexedSlice<(T, U)> {
        let limit = min(self.limit, other.limit);

        let mut self_hold = self.map_into(|s| Some(s));
        let mut other_hold = other.map_into(|s| Some(s));

        let mut result = Vec::new();
        let mut first_overlap_offset = None;

        for offset in -limit..=limit {
            match (
                self_hold.get_offset_mut(offset),
                other_hold.get_offset_mut(offset),
            ) {
                (Some(t), Some(u)) => {
                    first_overlap_offset.get_or_insert(offset);
                    result.push((t.take().unwrap(), u.take().unwrap()));
                }
                _ => (),
            }
        }

        OffsetIndexedSlice {
            container: result.into_boxed_slice(),
            base: first_overlap_offset.map_or(0, |offset| -offset),
            limit,
        }
    }

    pub fn from_slice_map<'a, S, F: Fn(&'a T, usize) -> S>(
        container: &'a [T],
        base: usize,
        limit: usize,
        mapper: F,
    ) -> OffsetIndexedSlice<S> {
        let Ok(base) = i32::try_from(base) else {
            panic!();
        };
        let Ok(limit) = i32::try_from(limit) else {
            panic!();
        };
        let bounds = materialised_bounds(container.len(), base, limit);
        let mut result = Vec::with_capacity(bounds.len());

        for (i, item) in container
            .iter()
            .enumerate()
            .skip(bounds.start)
            .take(bounds.len())
        {
            result.push(mapper(item, i));
        }

        OffsetIndexedSlice {
            container: result.into_boxed_slice(),
            base: bounds.base,
            limit,
        }
    }

    pub fn try_from_slice_map<'a, S, F: Fn(&'a mut T) -> Option<S>>(
        container: &'a mut [T],
        base: usize,
        limit: usize,
        mapper: F,
    ) -> Result<OffsetIndexedSlice<S>, usize> {
        let Ok(base) = i32::try_from(base) else {
            panic!();
        };
        let Ok(limit) = i32::try_from(limit) else {
            panic!();
        };
        let bounds = materialised_bounds(container.len(), base, limit);
        let mut result = Vec::with_capacity(bounds.len());

        for (i, item) in container
            .iter_mut()
            .enumerate()
            .skip(bounds.start)
            .take(bounds.len())
        {
            match mapper(item) {
                Some(s) => result.push(s),
                None => return Err(i),
            }
        }

        Ok(OffsetIndexedSlice {
            container: result.into_boxed_slice(),
            base: bounds.base,
            limit,
        })
    }

    pub fn get_offset(&self, offset: i32) -> Option<&T> {
        if offset.abs() > self.limit {
            return None;
        }

        if let Ok(index) = usize::try_from(self.base + offset) {
            self.container.as_ref().as_ref().get(index)
        } else {
            None
        }
    }

    pub fn get_offset_mut(&mut self, offset: i32) -> Option<&mut T> {
        if offset.abs() > self.limit {
            return None;
        }

        if let Ok(index) = usize::try_from(self.base + offset) {
            self.container.as_mut().as_mut().get_mut(index)
        } else {
            None
        }
    }

    pub fn map<U, F: Fn(&T) -> U>(&self, mapper: F) -> OffsetIndexedSlice<U> {
        let bounds = self.materialised_bounds();
        let new_container = self
            .container
            .iter()
            .skip(bounds.start)
            .take(bounds.len())
            .map(mapper)
            .collect::<Vec<_>>();

        OffsetIndexedSlice {
            container: new_container.into_boxed_slice(),
            base: bounds.base,
            limit: self.limit,
        }
    }

    pub fn map_in_place<F: FnMut(&mut T)>(&mut self, mut mapper: F) {
        let bounds = self.materialised_bounds();

        for item in self
            .container
            .iter_mut()
            .skip(bounds.start)
            .take(bounds.len())
        {
            mapper(item);
        }
    }

    pub fn map_into<U, F: Fn(T) -> U>(self, mapper: F) -> OffsetIndexedSlice<U> {
        let bounds = self.materialised_bounds();
        let limit = self.limit;
        let trimmed = Vec::from(self.container)
            .into_iter()
            .skip(bounds.start)
            .take(bounds.len())
            .collect::<Vec<_>>();

        OffsetIndexedSlice {
            container: trimmed.into_iter().map(mapper).collect(),
            base: bounds.base,
            limit,
        }
    }

    pub fn scan<F: FnMut(&T)>(&self, mut mapper: F) {
        let bounds = self.materialised_bounds();

        for item in self.container.iter().skip(bounds.start).take(bounds.len()) {
            mapper(&item)
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        let bounds = self.materialised_bounds();

        self.container.iter().skip(bounds.start).take(bounds.len())
    }

    pub fn into_vec(self) -> Vec<T> {
        let bounds = self.materialised_bounds();

        Vec::from(self.container)
            .into_iter()
            .skip(bounds.start)
            .take(bounds.len())
            .collect()
    }

    /// Trims the internal container to keep only the elements within the view.
    pub fn trim(self) -> OffsetIndexedSlice<T> {
        let bounds = self.materialised_bounds();
        let limit = self.limit;
        let new_container = Vec::from(self.container)
            .into_iter()
            .skip(bounds.start)
            .take(bounds.len())
            .collect::<Vec<_>>();

        OffsetIndexedSlice {
            container: new_container.into_boxed_slice(),
            base: bounds.base,
            limit,
        }
    }
}

impl<T: Clone + 'static> OffsetIndexedSlice<T> {
    pub fn from_slice(container: &[T], base: usize, limit: usize) -> Option<OffsetIndexedSlice<T>> {
        let Ok(base) = i32::try_from(base) else {
            panic!();
        };
        let Ok(limit) = i32::try_from(limit) else {
            panic!();
        };
        let bounds = materialised_bounds(container.len(), base, limit);

        let materialised = container
            .iter()
            .skip(bounds.start)
            .take(bounds.len())
            .cloned()
            .collect::<Vec<_>>();

        Some(OffsetIndexedSlice {
            container: materialised.into_boxed_slice(),
            base: bounds.base,
            limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::OffsetIndexedSlice;

    #[test]
    fn from_slice_clips_at_left_edge() {
        let view = OffsetIndexedSlice::from_slice(&[10, 20, 30], 0, 2).unwrap();

        assert_eq!(view.get_offset(-1), None);
        assert_eq!(view.get_offset(0), Some(&10));
        assert_eq!(view.get_offset(1), Some(&20));
        assert_eq!(view.get_offset(2), Some(&30));
    }

    #[test]
    fn from_slice_returns_empty_when_window_is_fully_out_of_range() {
        let view = OffsetIndexedSlice::from_slice(&[10, 20, 30], 10, 2).unwrap();

        assert_eq!(view.iter().count(), 0);
        assert_eq!(view.into_vec(), Vec::<i32>::new());
    }

    #[test]
    fn from_slice_allows_empty_input() {
        let view = OffsetIndexedSlice::<i32>::from_slice(&[], 0, 3).unwrap();

        assert_eq!(view.iter().count(), 0);
        assert_eq!(view.into_vec(), Vec::<i32>::new());
    }

    #[test]
    fn try_from_slice_map_succeeds_with_empty_overlap() {
        let view =
            OffsetIndexedSlice::try_from_slice_map(&mut [1, 2, 3], 10, 2, |value| Some(*value * 2))
                .unwrap();

        assert_eq!(view.iter().count(), 0);
    }

    #[test]
    fn zip_preserves_only_overlapping_offsets() {
        let left = OffsetIndexedSlice::new(vec![1, 2].into_boxed_slice(), 0, 1);
        let right = OffsetIndexedSlice::new(vec![10, 20].into_boxed_slice(), 1, 1);

        let zipped = left.zip(right);

        assert_eq!(zipped.get_offset(-1), None);
        assert_eq!(zipped.get_offset(0), Some(&(1, 20)));
        assert_eq!(zipped.get_offset(1), None);
    }

    #[test]
    fn map_into_materializes_only_the_visible_view() {
        let view = OffsetIndexedSlice::new(vec![10, 20, 30].into_boxed_slice(), 2, 0);

        assert_eq!(view.map_into(|value| value + 1).into_vec(), vec![31]);
    }

    #[test]
    fn scan_only_visits_materialised_view() {
        let view = OffsetIndexedSlice::new(vec![10, 20, 30].into_boxed_slice(), 2, 0);
        let mut seen = Vec::new();

        view.scan(|value| seen.push(*value));

        assert_eq!(seen, vec![30]);
    }
}
