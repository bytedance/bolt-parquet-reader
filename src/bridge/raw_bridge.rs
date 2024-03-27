// Copyright (c) ByteDance, Inc. and its affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Formatter;
use std::intrinsics::unlikely;

use crate::bridge::bridge_base::Bridge;
use crate::utils::exceptions::BoltReaderError;
use crate::utils::row_range_set::RowRangeSet;

// Currently, we display 10 pieces of data only
// todo: Create config module to handle the default const values.
const DEFAULT_DISPLAY_NUMBER: usize = 10;

/// The raw_validity might be shorter than raw_result.
/// In this case, the validity of these raw_result values are all true.
///
/// For example,
///
/// raw_validity: true, false, false
/// raw_result: 1, 2, 3, 4, 5
/// actual validity: true, false, false, true, true
pub struct RawBridge<T> {
    may_has_null: bool,
    raw_validity: Vec<bool>,
    raw_result: Vec<T>,
}

#[allow(dead_code)]
impl<T: ToString> std::fmt::Display for RawBridge<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let raw_validity_str = if self.may_has_null {
            "true, ".repeat(DEFAULT_DISPLAY_NUMBER - 1) + "true"
        } else {
            self.raw_validity
                .iter()
                .take(DEFAULT_DISPLAY_NUMBER)
                .map(bool::to_string)
                .collect::<Vec<String>>()
                .join(", ")
        };

        let raw_result_str = self
            .raw_result
            .iter()
            .take(DEFAULT_DISPLAY_NUMBER)
            .map(T::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        write!(f, "has null: {}, raw validity: {}, raw results: {} ... (showing only the first {} results)", self.may_has_null, raw_validity_str, raw_result_str, DEFAULT_DISPLAY_NUMBER)
    }
}

impl<T: Clone + Default + Copy> RawBridge<T> {
    pub fn new(may_has_null: bool, capacity: usize) -> RawBridge<T> {
        RawBridge {
            may_has_null,
            raw_validity: Default::default(),
            raw_result: Vec::with_capacity(capacity),
        }
    }

    /// Transfer the current self RawBridge values and Validity to another Bridge.
    ///
    /// # Arguments
    ///
    /// * `self_ranges` - The RowRangeSet corresponding to self RawBridge
    /// * `other_ranges` - The RowRangeSet corresponding to other RawBridge
    /// * `other_bridge` - The other Bridge, passed into the function as a dyn trait
    ///
    ///  Note: The both self_ranges and other_ranges should be guaranteed to be sorted in increasing order. And other_ranges should be a sub set of self_ranges. And multiple other_range can be a subset of self_range
    ///
    ///  For example
    ///  self_range: [[1,5), [7,9), [11,12)]
    ///  other_range:[[1,2), [4,5), [7,8)]
    ///  self_values: [1, 2, 3, 4, 7, 8, 11]
    ///
    /// In this example, [1,2) and [4,5) are all the subset of [1,5)
    /// And the result of other_bridge should be [1,4,7]
    pub fn transfer_values(
        &mut self,
        self_ranges: &RowRangeSet,
        other_ranges: &RowRangeSet,
        other_bridge: &mut dyn Bridge<T>,
    ) -> Result<(), BoltReaderError> {
        if self.is_empty() {
            return Err(BoltReaderError::BridgeError(String::from(
                "Raw Bridge: Can't retrieve values from an empty raw bridge",
            )));
        }

        if other_bridge.may_has_null() {
            self.materialize_validity();
        }

        let mut self_range_idx = 0;
        let mut base = 0;
        let other_offset = other_ranges.get_offset();

        for other_range in other_ranges.get_row_ranges() {
            while self_range_idx < self_ranges.get_row_ranges().len() {
                let self_range = &self_ranges.get_row_ranges()[self_range_idx];
                let self_offset = self_ranges.get_offset();
                let self_len = self_range.end - self_range.begin;

                let self_range_begin = self_range.begin + self_offset;
                let self_range_end = self_range.end + self_offset;
                let other_range_begin = other_range.begin + other_offset;
                let other_range_end = other_range.end + other_offset;

                if other_range_begin >= self_range_begin && other_range_end <= self_range_end {
                    let other_len = other_range_end - other_range_begin;
                    let self_start = other_range_begin + base - self_range_begin;

                    let value_slice = &self.raw_result[self_start..self_start + other_len];
                    if !self.may_has_null() && !other_bridge.may_has_null() {
                        other_bridge.append_non_null_results(value_slice)?;
                    } else {
                        let validity_slice = &self.raw_validity[self_start..self_start + other_len];
                        other_bridge.append_results(validity_slice, value_slice)?;
                    }
                    break;
                }

                base += self_len;
                self_range_idx += 1;
            }
        }

        Ok(())
    }

    /// Late materialize the validity vector when necessary.
    /// The may_have_null flag is set to be true.
    #[inline(always)]
    pub fn materialize_validity(&mut self) {
        if unlikely(self.raw_result.len() != self.raw_validity.len()) {
            self.may_has_null = true;
            self.raw_validity
                .extend(vec![true; self.raw_result.len() - self.raw_validity.len()]);
        }
    }
}

impl<T: Clone + Default + Copy> Bridge<T> for RawBridge<T> {
    fn get_bridge_name(&self) -> String {
        String::from("Raw Bridge")
    }

    fn is_empty(&self) -> bool {
        self.raw_result.is_empty()
    }

    fn get_size(&self) -> usize {
        self.raw_result.len()
    }

    fn may_has_null(&self) -> bool {
        self.may_has_null
    }

    fn append_result(&mut self, validity: bool, result: T) {
        self.materialize_validity();
        self.raw_validity.push(validity);
        self.raw_result.push(result);
    }

    fn append_non_null_result(&mut self, result: T) {
        self.raw_result.push(result);
    }

    fn append_results(&mut self, validity: &[bool], result: &[T]) -> Result<(), BoltReaderError> {
        if unlikely(validity.len() != result.len()) {
            return Err(BoltReaderError::BridgeError(String::from(
                "append_resutls() error. validity length is not equal to result length",
            )));
        }

        self.materialize_validity();
        let old_length = self.raw_result.len();
        let new_length = old_length + result.len();
        self.raw_validity.resize(new_length, bool::default());
        self.raw_validity[old_length..new_length].copy_from_slice(validity);
        self.raw_result.resize(new_length, T::default());
        self.raw_result[old_length..new_length].copy_from_slice(result);
        Ok(())
    }

    fn append_non_null_results(&mut self, result: &[T]) -> Result<(), BoltReaderError> {
        let old_length = self.raw_result.len();
        let new_length = old_length + result.len();
        self.raw_result.resize(new_length, T::default());
        self.raw_result[old_length..new_length].copy_from_slice(result);
        Ok(())
    }

    fn get_validity_and_value(
        &self,
        offset: usize,
        index: usize,
        ranges: &RowRangeSet,
    ) -> Result<(bool, T), BoltReaderError> {
        if unlikely(self.is_empty()) {
            return Err(BoltReaderError::BridgeError(String::from(
                "Raw Bridge: Can't retrieve value from empty raw bridge",
            )));
        }

        let mut base = 0;
        for range in ranges.get_row_ranges() {
            if offset + index >= range.begin + ranges.get_offset()
                && offset + index < range.end + ranges.get_offset()
            {
                let idx = offset + index + base - range.begin - ranges.get_offset();

                return Ok((
                    idx >= self.raw_validity.len() || self.raw_validity[idx],
                    self.raw_result[idx],
                ));
            }
            base += range.end - range.begin;
        }
        Err(BoltReaderError::BridgeError(format!(
            "Raw Bridge: Can't find the offset: {} and index: {} from empty raw bridge",
            offset, index
        )))
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use crate::bridge::bridge_base::Bridge;
    use crate::bridge::raw_bridge::RawBridge;
    use crate::utils::row_range_set::{RowRangeSet, RowRangeSetGenerator};

    fn create_row_range_set(offset: usize, bool_vec: &Vec<bool>) -> RowRangeSet {
        let mut row_range_set = RowRangeSet::new(offset);
        let mut generator = RowRangeSetGenerator::new(&mut row_range_set);

        for i in 0..bool_vec.len() {
            generator.update(i, bool_vec[i]);
        }
        generator.finish(bool_vec.len());
        row_range_set
    }

    fn create_raw_bridge(may_has_null: bool, capacity: usize, size: usize) -> RawBridge<i64> {
        let mut raw_bridge: RawBridge<i64> = RawBridge::new(may_has_null, capacity);
        for i in 1..=size {
            raw_bridge.append_result(true, i as i64);
        }
        raw_bridge
    }

    fn create_non_null_raw_bridge(
        may_has_null: bool,
        capacity: usize,
        size: usize,
    ) -> RawBridge<i64> {
        let mut raw_bridge: RawBridge<i64> = RawBridge::new(may_has_null, capacity);
        for i in 1..=size {
            raw_bridge.append_non_null_result(i as i64);
        }
        raw_bridge
    }

    fn create_raw_bridge_with_values(
        may_has_null: bool,
        capacity: usize,
        values: &Vec<i64>,
    ) -> RawBridge<i64> {
        let mut raw_bridge: RawBridge<i64> = RawBridge::new(may_has_null, capacity);
        for value in values.iter() {
            raw_bridge.append_non_null_result(*value);
        }
        raw_bridge
    }

    fn create_raw_bridge_with_values_and_validity(
        may_has_null: bool,
        capacity: usize,
        values: &Vec<i64>,
        validity: &Vec<bool>,
    ) -> RawBridge<i64> {
        let mut raw_bridge: RawBridge<i64> = RawBridge::new(may_has_null, capacity);
        for i in 0..values.len() {
            raw_bridge.append_result(validity[i], values[i]);
        }
        raw_bridge
    }

    fn create_random_bool_vec(vector_length: usize) -> Vec<bool> {
        let mut rng = rand::thread_rng();
        let random_bool_vec: Vec<bool> = (0..vector_length).map(|_| rng.gen::<bool>()).collect();

        random_bool_vec
    }

    fn create_values_from_bool_vec(bool_vec: &Vec<bool>) -> Vec<i64> {
        let result_vec = bool_vec
            .iter()
            .enumerate()
            .filter_map(|(index, &value)| if value { Some(index as i64 + 1) } else { None })
            .collect();

        result_vec
    }

    fn create_validity_from_bool_vec(bool_vec: &Vec<bool>) -> Vec<bool> {
        let result_vec = bool_vec
            .iter()
            .enumerate()
            .filter_map(|(index, &value)| if value { Some(index % 2 == 0) } else { None })
            .collect();

        result_vec
    }

    fn create_values_from_bool_vec_sub_set(
        bool_vec: &Vec<bool>,
        bool_vec_sub_set: &Vec<bool>,
    ) -> Vec<i64> {
        let result_vec: Vec<i64> = bool_vec
            .iter()
            .enumerate()
            .zip(bool_vec_sub_set.iter())
            .filter_map(|((index, &value1), &value2)| {
                if value1 && value2 {
                    Some((index + 1) as i64)
                } else {
                    None
                }
            })
            .collect();

        result_vec
    }

    fn create_validity_from_bool_vec_sub_set(
        bool_vec: &Vec<bool>,
        bool_vec_sub_set: &Vec<bool>,
    ) -> Vec<bool> {
        let result_vec: Vec<bool> = bool_vec
            .iter()
            .enumerate()
            .zip(bool_vec_sub_set.iter())
            .filter_map(|((index, &value1), &value2)| {
                if value1 && value2 {
                    Some(index % 2 == 0)
                } else {
                    None
                }
            })
            .collect();

        result_vec
    }

    fn create_random_bool_vec_sub_set(bool_vec: &Vec<bool>) -> Vec<bool> {
        let mut rng = rand::thread_rng();
        let mut new_bool_vec = bool_vec.clone();
        for i in 0..new_bool_vec.len() {
            if new_bool_vec[i] == true {
                let random_bool: bool = rng.gen();
                new_bool_vec[i] = random_bool;
            }
        }

        new_bool_vec
    }

    #[test]
    fn test_create_raw_bridge() {
        let may_has_null = false;
        let capacity = 10;

        let raw_bridge: RawBridge<i64> = RawBridge::new(may_has_null, capacity);

        assert_eq!(raw_bridge.raw_result.capacity(), 10);
        assert_eq!(raw_bridge.raw_result.len(), 0);
        assert_eq!(raw_bridge.may_has_null(), false);
        assert_eq!(raw_bridge.get_bridge_name(), "Raw Bridge");
    }

    #[test]
    fn test_append_result() {
        let may_has_null = false;
        let capacity = 10;
        let offset = 3;
        let raw_bridge_size = 5;
        let bool_vec = Vec::from([false, true, false, false, true, true, false, true, true]);

        let row_range_set = create_row_range_set(offset, &bool_vec);

        let raw_bridge = create_raw_bridge(may_has_null, capacity, raw_bridge_size);

        assert_eq!(raw_bridge.get_size(), raw_bridge_size);

        let first_element = raw_bridge.get_validity_and_value(3, 1, &row_range_set);
        assert!(first_element.is_ok());
        assert_eq!(first_element.unwrap(), (true, 1));

        let first_element = raw_bridge.get_validity_and_value(2, 2, &row_range_set);
        assert!(first_element.is_ok());
        assert_eq!(first_element.unwrap(), (true, 1));

        let indices: Vec<usize> = bool_vec
            .iter()
            .enumerate()
            .filter_map(|(index, &value)| if value { Some(index) } else { None })
            .collect();

        for i in 0..indices.len() {
            let res_value_validity =
                raw_bridge.get_validity_and_value(offset, indices[i], &row_range_set);
            assert!(res_value_validity.is_ok());
            assert_eq!(res_value_validity.unwrap(), (true, (i as i64) + 1));
        }

        let non_existing_res = raw_bridge.get_validity_and_value(offset, 11, &row_range_set);
        assert!(non_existing_res.is_err());
    }

    #[test]
    fn test_append_result_with_late_materialization() {
        let may_has_null = false;
        let capacity = 10;
        let offset = 3;
        let raw_bridge_size = 6;
        let bool_vec = Vec::from([
            false, true, false, false, true, true, false, true, true, true,
        ]);

        let row_range_set = create_row_range_set(offset, &bool_vec);

        let mut raw_bridge =
            create_non_null_raw_bridge(may_has_null, capacity, raw_bridge_size - 1);
        assert_eq!(raw_bridge.may_has_null(), false);
        assert_eq!(raw_bridge.raw_validity.len(), 0);

        raw_bridge.append_result(false, 6);

        assert_eq!(raw_bridge.get_size(), raw_bridge_size);

        let first_element = raw_bridge.get_validity_and_value(3, 1, &row_range_set);
        assert!(first_element.is_ok());
        assert_eq!(first_element.unwrap(), (true, 1));

        let first_element = raw_bridge.get_validity_and_value(2, 2, &row_range_set);
        assert!(first_element.is_ok());
        assert_eq!(first_element.unwrap(), (true, 1));

        let indices: Vec<usize> = bool_vec
            .iter()
            .enumerate()
            .filter_map(|(index, &value)| if value { Some(index) } else { None })
            .collect();

        for i in 0..indices.len() - 1 {
            let res_value_validity =
                raw_bridge.get_validity_and_value(offset, indices[i], &row_range_set);
            assert!(res_value_validity.is_ok());
            assert_eq!(res_value_validity.unwrap(), (true, (i as i64) + 1));
        }

        let res_value_validity =
            raw_bridge.get_validity_and_value(offset, indices[indices.len() - 1], &row_range_set);
        assert!(res_value_validity.is_ok());
        assert_eq!(
            res_value_validity.unwrap(),
            (false, (indices.len() as i64 - 1) + 1)
        );

        let non_existing_res = raw_bridge.get_validity_and_value(offset, 11, &row_range_set);
        assert!(non_existing_res.is_err());
    }

    #[test]
    fn test_append_results() {
        let may_has_null = false;
        let capacity = 10000;
        let offset = 3;

        let bool_vec = create_random_bool_vec(capacity);
        let range = create_row_range_set(offset, &bool_vec);
        let values = create_values_from_bool_vec(&bool_vec);
        let validity = create_validity_from_bool_vec(&bool_vec);

        let mut raw_bridge: RawBridge<i64> = RawBridge::new(may_has_null, capacity);
        let res = raw_bridge.append_results(&validity, &values);
        assert!(res.is_ok());

        let mut value_idx = 0;
        for i in 0..bool_vec.len() {
            if bool_vec[i] {
                let res = raw_bridge.get_validity_and_value(offset, i, &range);
                assert!(res.is_ok());
                assert_eq!(res.unwrap(), (validity[value_idx], values[value_idx]));
                value_idx += 1;
            }
        }

        let invalid_res = raw_bridge.get_validity_and_value(offset, bool_vec.len(), &range);
        assert!(invalid_res.is_err());
    }

    #[test]
    fn test_append_results_with_late_materialization() {
        let may_has_null = false;
        let capacity = 10000;
        let offset = 3;

        let bool_vec = create_random_bool_vec(capacity);
        let range = create_row_range_set(offset, &bool_vec);
        let values = create_values_from_bool_vec(&bool_vec);
        let validity = create_validity_from_bool_vec(&bool_vec);

        let first_half = values.len() / 2;

        let mut raw_bridge: RawBridge<i64> = RawBridge::new(may_has_null, capacity);
        let res = raw_bridge.append_non_null_results(&values[0..first_half]);
        assert!(res.is_ok());
        assert_eq!(raw_bridge.may_has_null(), false);
        assert_eq!(raw_bridge.raw_validity.len(), 0);

        let res = raw_bridge.append_results(&validity[first_half..], &values[first_half..]);
        assert!(res.is_ok());
        assert_eq!(raw_bridge.may_has_null(), true);
        assert_eq!(raw_bridge.raw_validity.len(), raw_bridge.raw_result.len());

        let mut value_idx = 0;
        for i in 0..bool_vec.len() {
            if bool_vec[i] {
                if value_idx < first_half {
                    let res = raw_bridge.get_validity_and_value(offset, i, &range);
                    assert!(res.is_ok());
                    assert_eq!(res.unwrap(), (true, values[value_idx]));
                } else {
                    let res = raw_bridge.get_validity_and_value(offset, i, &range);
                    assert!(res.is_ok());
                    assert_eq!(res.unwrap(), (validity[value_idx], values[value_idx]));
                }
                value_idx += 1;
            }
        }

        let invalid_res = raw_bridge.get_validity_and_value(offset, bool_vec.len(), &range);
        assert!(invalid_res.is_err());
    }

    #[test]
    fn test_transfer_random_nullable_results() {
        let may_has_null = true;
        let capacity = 1000;
        let offset = 3;

        let self_bool_vec = create_random_bool_vec(capacity);
        let other_bool_vec = create_random_bool_vec_sub_set(&self_bool_vec);

        let self_range = create_row_range_set(offset, &self_bool_vec);
        let other_range = create_row_range_set(offset, &other_bool_vec);

        let self_values = create_values_from_bool_vec(&self_bool_vec);
        let other_values = create_values_from_bool_vec_sub_set(&self_bool_vec, &other_bool_vec);

        let self_validity = create_validity_from_bool_vec(&self_bool_vec);
        let other_validity = create_validity_from_bool_vec_sub_set(&self_bool_vec, &other_bool_vec);

        let mut self_raw_bridge = create_raw_bridge_with_values_and_validity(
            may_has_null,
            capacity,
            &self_values,
            &self_validity,
        );
        let mut other_raw_bridge: RawBridge<i64> = RawBridge::new(may_has_null, capacity);
        let res = self_raw_bridge.transfer_values(&self_range, &other_range, &mut other_raw_bridge);
        assert!(res.is_ok());
        assert_eq!(other_raw_bridge.raw_validity, other_validity);
        assert_eq!(other_raw_bridge.raw_result, other_values);
    }

    #[test]
    fn test_transfer_random_other_nullable_results() {
        let capacity = 1000;
        let offset = 3;

        let self_bool_vec = create_random_bool_vec(capacity);
        let other_bool_vec = create_random_bool_vec_sub_set(&self_bool_vec);

        let self_range = create_row_range_set(offset, &self_bool_vec);
        let other_range = create_row_range_set(offset, &other_bool_vec);

        let self_values = create_values_from_bool_vec(&self_bool_vec);
        let other_values = create_values_from_bool_vec_sub_set(&self_bool_vec, &other_bool_vec);

        let other_validity = create_validity_from_bool_vec_sub_set(&self_bool_vec, &other_bool_vec);
        let other_validity = vec![true; other_validity.len()];
        let mut self_raw_bridge = create_raw_bridge_with_values(false, capacity, &self_values);
        let mut other_raw_bridge: RawBridge<i64> = RawBridge::new(true, capacity);
        let res = self_raw_bridge.transfer_values(&self_range, &other_range, &mut other_raw_bridge);
        assert!(res.is_ok());
        assert_eq!(other_raw_bridge.raw_validity, other_validity);
        assert_eq!(other_raw_bridge.raw_result, other_values);
    }

    #[test]
    fn test_transfer_random_non_null_results() {
        let capacity = 1000;
        let offset = 3;

        let self_bool_vec = create_random_bool_vec(capacity);
        let other_bool_vec = create_random_bool_vec_sub_set(&self_bool_vec);

        let self_range = create_row_range_set(offset, &self_bool_vec);
        let other_range = create_row_range_set(offset, &other_bool_vec);

        let self_values = create_values_from_bool_vec(&self_bool_vec);
        let other_values = create_values_from_bool_vec_sub_set(&self_bool_vec, &other_bool_vec);

        let mut self_raw_bridge = create_raw_bridge_with_values(false, capacity, &self_values);
        let mut other_raw_bridge: RawBridge<i64> = RawBridge::new(false, capacity);
        let res = self_raw_bridge.transfer_values(&self_range, &other_range, &mut other_raw_bridge);
        assert!(res.is_ok());
        assert_eq!(other_raw_bridge.raw_validity.len(), 0);
        assert_eq!(other_raw_bridge.may_has_null(), false);
        assert_eq!(other_raw_bridge.raw_result, other_values);
    }
}
