//! Auto-merging range vector.


use std::cmp::Ordering;
use std::fmt;


pub struct RangeVec<T> {
    data: Vec<(T, T)>
}

impl<T> RangeVec<T>
where
    T: Ord + Copy
{

    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub unsafe fn from_raw_unchecked(data: Vec<(T, T)>) -> Self {
        Self { data }
    }

    pub fn from_raw(data: Vec<(T, T)>) -> Option<Self> {
        let mut last_to = None;
        for &(from, to) in &data {
            if to <= from {
                return None;
            } else if let Some(last_to) = last_to {
                if from <= last_to {
                    return None;
                }
            }
            last_to = Some(to);
        }
        Some(unsafe { Self::from_raw_unchecked(data) })
    }

    pub fn push(&mut self, from: T, to: T) {

        assert!(to > from, "Invalid range.");

        // We mutate these in order to change them while merging ranges.
        let mut to = to;
        let mut from = from;

        // Indicates if we want to insert a new range at the 'work_idx'
        // instead of modifying existing range.
        let mut insert;
        // The index of the range we are working on, or where we need
        // to insert the range, depending on 'insert'.
        let mut work_idx;
        // The index of the next range to check for merging.
        let mut check_idx;

        match self.data.binary_search_by_key(&from, |&(from, _)| from) {
            Ok(found_idx) => {

                insert = false;
                work_idx = found_idx;
                check_idx = found_idx + 1;

                // Here we fix 'to' to avoid reducing the existing range.
                let &(_, found_to) = unsafe { self.data.get_unchecked(found_idx) };
                if to < found_to {
                    to = found_to;
                }

            }
            Err(insert_idx) => {

                insert = true;
                work_idx = insert_idx;
                check_idx = insert_idx;

                if let Some(prev_index) = insert_idx.checked_sub(1) {
                    // This is safe because all positive or null indices before
                    // the insert index are valid by definition. If we insert at
                    // the end, so at "size", then "size - 1" is a valid index.
                    let &(prev_from, prev_to) = unsafe { self.data.get_unchecked(prev_index) };
                    if from <= prev_to {
                        // If we are inserting a range that is intersecting with the previous
                        // one, we just make the inserting range starting from the previous
                        // one, just emulate the 'Ok(found_idx) =>' match case.
                        insert = false;
                        from = prev_from;
                        work_idx = prev_index;
                        // Here we fix 'to' to avoid reducing the existing range.
                        if to < prev_to {
                            to = prev_to;
                        }
                    }
                }

            }
        }

        let mut drain_idx = check_idx;

        while let Some(&(check_from, check_to)) = self.data.get(check_idx) {
            if to < check_from {
                // If our range don't intersect with the next one, just break.
                break;
            } else if to < check_to {
                // If we intersect with the next range, ensure that our range
                // will not reduce the current one.
                to = check_to;
            }
            check_idx += 1;
            // If we were in insert mode, just deactivate it and increment drain
            // index to avoid draining the range we are merging with.
            if insert {
                insert = false;
                drain_idx += 1;
            }
        }

        if check_idx > drain_idx {
            self.data.drain(drain_idx..check_idx);
        }

        if insert {
            self.data.insert(work_idx, (from, to));
        } else {
            unsafe {
                *self.data.get_unchecked_mut(work_idx) = (from, to);
            }
        }

    }

    #[inline]
    pub fn get_ranges(&self) -> &[(T, T)] {
        &self.data[..]
    }

    pub fn contains(&self, value: T) -> bool {
        self.data.binary_search_by(move |&(item_from, item_to)| {
            if value < item_from {
                Ordering::Greater
            } else if value >= item_to {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }).is_ok()
    }

}

impl<T> fmt::Debug for RangeVec<T>
where
    T: Copy + fmt::Debug
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{:?}", &self.data[..]))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn basic() {

        let mut vec = RangeVec::new();

        vec.push(3, 7);
        assert_eq!(vec.get_ranges(), &[(3, 7)]);

        vec.push(8, 13);
        assert_eq!(vec.get_ranges(), &[(3, 7), (8, 13)]);

        vec.push(3, 5);
        assert_eq!(vec.get_ranges(), &[(3, 7), (8, 13)]);

        vec.push(13, 20);
        assert_eq!(vec.get_ranges(), &[(3, 7), (8, 20)]);

        vec.push(0, 100);
        assert_eq!(vec.get_ranges(), &[(0, 100)]);

        vec.push(100, 101);
        assert_eq!(vec.get_ranges(), &[(0, 101)]);

        vec.push(-1, 0);
        assert_eq!(vec.get_ranges(), &[(-1, 101)]);

        vec.push(-10, -2);
        assert_eq!(vec.get_ranges(), &[(-10, -2), (-1, 101)]);

        vec.push(10, 50);
        assert_eq!(vec.get_ranges(), &[(-10, -2), (-1, 101)]);

        vec.push(-5, 50);
        assert_eq!(vec.get_ranges(), &[(-10, 101)]);

    }

}