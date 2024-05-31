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
use std::string::String;

use arrow::array::BinaryArray;

use crate::bridge::result_bridge::{BridgeDataType, BridgeType, ResultBridge};
use crate::utils::exceptions::BoltReaderError;
use crate::utils::row_range_set::RowRangeSet;

// Currently, we display 10 pieces of data only
// todo: Create config module to handle the default const values.
const DEFAULT_DISPLAY_NUMBER: usize = 10;

pub type ByteArray = Vec<u8>;

#[allow(dead_code)]
pub struct ByteArrayBridge {
    bridge_type: BridgeType,
    bridge_data_type: BridgeDataType,
    may_has_null: bool,
    // If non-null results are added to nullable bridge, the non-null results will be deep copied to nullable results.
    non_null_data: Vec<ByteArray>,
    nullable_data: Vec<Option<ByteArray>>,
}
#[allow(dead_code)]
impl std::fmt::Display for ByteArrayBridge {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let result_str = if self.may_has_null {
            self.nullable_data
                .iter()
                .take(DEFAULT_DISPLAY_NUMBER)
                .map(|value| match value {
                    None => String::from("None"),
                    Some(_) => String::from("Byte Array"),
                })
                .collect::<Vec<String>>()
                .join(", ")
        } else {
            self.non_null_data
                .iter()
                .take(DEFAULT_DISPLAY_NUMBER)
                .map(|_| String::from("Byte Array"))
                .collect::<Vec<String>>()
                .join(", ")
        };

        write!(
            f,
            "{} has null: {}, raw results: {} ... (showing only the first {} results)",
            self.get_bridge_name(),
            self.may_has_null,
            result_str,
            DEFAULT_DISPLAY_NUMBER
        )
    }
}

impl ByteArrayBridge {
    pub fn new(may_has_null: bool, capacity: usize) -> ByteArrayBridge {
        ByteArrayBridge {
            bridge_type: BridgeType::RustVec,
            bridge_data_type: BridgeDataType::ByteArray,
            may_has_null,
            non_null_data: Vec::with_capacity(capacity),
            nullable_data: Vec::with_capacity(capacity),
        }
    }
}

impl ResultBridge for ByteArrayBridge {
    fn get_bridge_name(&self) -> String {
        String::from("Byte Array Bridge")
    }

    fn is_empty(&self) -> bool {
        if self.may_has_null {
            self.nullable_data.is_empty()
        } else {
            self.non_null_data.is_empty()
        }
    }

    fn get_size(&self) -> usize {
        if self.may_has_null {
            self.nullable_data.len()
        } else {
            self.non_null_data.len()
        }
    }

    fn may_has_null(&self) -> bool {
        self.may_has_null
    }

    fn set_may_has_null(&mut self, may_has_null: bool) {
        self.may_has_null = may_has_null;
    }

    fn append_nullable_byte_array_result(
        &mut self,
        result: Option<ByteArray>,
    ) -> Result<(), BoltReaderError> {
        self.nullable_data.push(result);

        Ok(())
    }

    fn append_nullable_byte_array_results(
        &mut self,
        result: &[Option<ByteArray>],
    ) -> Result<(), BoltReaderError> {
        for byte_array in result {
            self.nullable_data.push(byte_array.clone());
        }

        Ok(())
    }

    fn append_non_null_byte_array_result(
        &mut self,
        result: ByteArray,
    ) -> Result<(), BoltReaderError> {
        if unlikely(self.may_has_null) {
            return self.append_nullable_byte_array_result(Some(result));
        }

        self.non_null_data.push(result);

        Ok(())
    }

    fn append_non_null_byte_array_results(
        &mut self,
        result: &[ByteArray],
    ) -> Result<(), BoltReaderError> {
        if unlikely(self.may_has_null) {
            for byte_array in result {
                self.append_nullable_byte_array_result(Some(byte_array.clone()))?;
            }
        } else {
            for byte_array in result {
                self.append_non_null_byte_array_result(byte_array.clone())?;
            }
        }

        Ok(())
    }

    fn get_byte_array_validity_and_value(
        &self,
        offset: usize,
        index: usize,
        ranges: &RowRangeSet,
    ) -> Result<(bool, ByteArray), BoltReaderError> {
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

                if self.may_has_null {
                    if let Some(value) = self.nullable_data[idx].clone() {
                        return Ok((true, value));
                    } else {
                        return Ok((false, ByteArray::default()));
                    }
                } else {
                    return Ok((true, self.non_null_data[idx].clone()));
                }
            }
            base += range.end - range.begin;
        }
        Err(BoltReaderError::BridgeError(format!(
            "Raw Bridge: Can't find the offset: {} and index: {} from empty raw bridge",
            offset, index
        )))
    }

    fn to_byte_array_arrow_array(&mut self) -> Result<BinaryArray, BoltReaderError> {
        if self.may_has_null {
            let values = self
                .nullable_data
                .iter()
                .map(|value| value.as_ref().map(|value| value.as_slice()))
                .collect::<Vec<Option<&[u8]>>>();

            Ok(BinaryArray::from_opt_vec(values))
        } else {
            let values = self
                .non_null_data
                .iter()
                .map(|value| value.as_slice())
                .collect::<Vec<&[u8]>>();

            Ok(BinaryArray::from_vec(values))
        }
    }

    fn transfer_values(
        &mut self,
        self_ranges: &RowRangeSet,
        other_ranges: &RowRangeSet,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<(), BoltReaderError> {
        result_bridge.set_may_has_null(self.may_has_null);

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

                    if self.may_has_null() {
                        let value_slice = &self.nullable_data[self_start..self_start + other_len];
                        result_bridge.append_nullable_byte_array_results(value_slice)?;
                    } else {
                        let value_slice = &self.non_null_data[self_start..self_start + other_len];
                        result_bridge.append_non_null_byte_array_results(value_slice)?;
                    }

                    break;
                }

                base += self_len;
                self_range_idx += 1;
            }
        }

        Ok(())
    }

    fn as_nullable(&mut self) -> Result<(), BoltReaderError> {
        if unlikely(self.may_has_null) {
            return Err(BoltReaderError::BridgeError(format!(
                "The {} is already 'may_has_null = true'",
                self.get_bridge_name()
            )));
        }
        self.may_has_null = true;
        self.non_null_data.iter().enumerate().for_each(|(_, e)| {
            self.nullable_data.push(Some(e.clone()));
        });
        self.non_null_data.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::bridge::byte_array_bridge::{ByteArray, ByteArrayBridge};
    use crate::bridge::result_bridge::bridge_tests_utils::{
        create_random_bool_vec, create_random_bool_vec_sub_set, create_row_range_set,
    };
    use crate::bridge::result_bridge::ResultBridge;
    use crate::utils::row_range_set::RowRangeSet;

    fn get_non_null_byte_array_value(index: usize) -> ByteArray {
        index.to_string().into_bytes()
    }

    fn get_nullable_byte_array_value(index: usize) -> Option<ByteArray> {
        if index % 5 == 0 || index % 17 == 0 {
            None
        } else {
            Some(index.to_string().into_bytes())
        }
    }

    fn create_non_null_byte_array_data(row_range_set: &RowRangeSet) -> Vec<ByteArray> {
        let mut byte_array_vec = Vec::new();

        let offset = row_range_set.get_offset();
        for row_range in row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                byte_array_vec.push(get_non_null_byte_array_value(i + offset));
            }
        }

        byte_array_vec
    }

    fn create_nullable_byte_array_data(row_range_set: &RowRangeSet) -> Vec<Option<ByteArray>> {
        let mut byte_array_vec = Vec::new();

        let offset = row_range_set.get_offset();
        for row_range in row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                byte_array_vec.push(get_nullable_byte_array_value(i + offset));
            }
        }

        byte_array_vec
    }

    fn create_byte_array_bridge(
        may_has_null: bool,
        capacity: usize,
        row_range_set: &RowRangeSet,
    ) -> ByteArrayBridge {
        let mut bridge: ByteArrayBridge = ByteArrayBridge::new(may_has_null, capacity);

        let offset = row_range_set.get_offset();
        for row_range in row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                if may_has_null {
                    let _ = bridge.append_nullable_byte_array_result(
                        get_nullable_byte_array_value(i + offset),
                    );
                } else {
                    let _ = bridge.append_non_null_byte_array_result(
                        get_non_null_byte_array_value(i + offset),
                    );
                }
            }
        }

        bridge
    }

    fn verify_byte_array_raw_bridge_data(
        raw_bridge: &ByteArrayBridge,
        row_range_set: &RowRangeSet,
    ) {
        let offset = row_range_set.get_offset();
        for row_range in row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_byte_array_validity_and_value(offset, i, &row_range_set)
                    .unwrap();

                let index = i + offset;
                if raw_bridge.may_has_null {
                    if index % 5 == 0 || index % 17 == 0 {
                        assert_eq!(validity, false);
                    } else {
                        assert_eq!(validity, true);
                        assert_eq!(value, index.to_string().into_bytes());
                    }
                } else {
                    assert_eq!(validity, true);
                    assert_eq!(value, index.to_string().into_bytes());
                }
            }
        }
    }

    #[test]
    fn test_create_raw_bridge() {
        let may_has_null = false;
        let capacity = 10;

        let raw_bridge: ByteArrayBridge = ByteArrayBridge::new(may_has_null, capacity);

        assert_eq!(raw_bridge.non_null_data.capacity(), 10);
        assert_eq!(raw_bridge.non_null_data.len(), 0);
        assert_eq!(raw_bridge.nullable_data.capacity(), 10);
        assert_eq!(raw_bridge.nullable_data.len(), 0);
        assert_eq!(raw_bridge.may_has_null(), false);
        assert_eq!(raw_bridge.get_bridge_name(), "Byte Array Bridge");
    }

    #[test]
    fn test_append_result() {
        let may_has_null = false;
        let capacity = 10;
        let offset = 3;
        let raw_bridge_size = 5;
        let bool_vec = Vec::from([false, true, false, false, true, true, false, true, true]);

        let row_range_set = create_row_range_set(offset, &bool_vec);

        let raw_bridge = create_byte_array_bridge(may_has_null, capacity, &row_range_set);

        assert_eq!(raw_bridge.get_size(), raw_bridge_size);

        let first_element = raw_bridge.get_byte_array_validity_and_value(3, 1, &row_range_set);
        assert!(first_element.is_ok());
        assert_eq!(first_element.unwrap(), (true, 4.to_string().into_bytes()));

        let first_element = raw_bridge.get_byte_array_validity_and_value(2, 2, &row_range_set);
        assert!(first_element.is_ok());
        assert_eq!(first_element.unwrap(), (true, 4.to_string().into_bytes()));

        verify_byte_array_raw_bridge_data(&raw_bridge, &row_range_set);

        let non_existing_res =
            raw_bridge.get_byte_array_validity_and_value(offset, 11, &row_range_set);
        assert!(non_existing_res.is_err());
    }

    #[test]
    fn test_append_results() {
        let may_has_null = false;
        let capacity = 10000;
        let offset = 3;

        let bool_vec = create_random_bool_vec(capacity);
        let row_range_set = create_row_range_set(offset, &bool_vec);

        let values_to_append = create_non_null_byte_array_data(&row_range_set);
        let mut raw_bridge = ByteArrayBridge::new(may_has_null, capacity);

        assert!(raw_bridge
            .append_non_null_byte_array_results(&values_to_append)
            .is_ok());

        verify_byte_array_raw_bridge_data(&raw_bridge, &row_range_set);
    }

    #[test]
    fn test_append_nullable_results() {
        let may_has_null = true;
        let capacity = 10000;
        let offset = 3;

        let bool_vec = create_random_bool_vec(capacity);
        let row_range_set = create_row_range_set(offset, &bool_vec);

        let values_to_append = create_nullable_byte_array_data(&row_range_set);
        let mut raw_bridge = ByteArrayBridge::new(may_has_null, capacity);

        assert!(raw_bridge
            .append_nullable_byte_array_results(&values_to_append)
            .is_ok());

        verify_byte_array_raw_bridge_data(&raw_bridge, &row_range_set);
    }

    #[test]
    fn test_convert_to_nullable_bridge() {
        let may_has_null = false;
        let capacity = 10;
        let offset = 3;

        let bool_vec = Vec::from([
            false, true, false, false, true, true, false, true, true, true,
        ]);

        let raw_bridge_size = bool_vec
            .iter()
            .filter(|value| **value)
            .fold(0, |acc, value| if *value { acc + 1 } else { acc });

        let row_range_set = create_row_range_set(offset, &bool_vec);

        let mut raw_bridge = create_byte_array_bridge(may_has_null, capacity, &row_range_set);
        assert_eq!(raw_bridge.may_has_null(), false);
        assert_eq!(raw_bridge.non_null_data.len(), raw_bridge_size);
        assert_eq!(raw_bridge.nullable_data.len(), 0);

        assert!(raw_bridge.as_nullable().is_ok());

        assert_eq!(raw_bridge.non_null_data.len(), 0);
        assert_eq!(raw_bridge.nullable_data.len(), raw_bridge_size);
    }

    #[test]
    fn test_convert_to_nullable_bridge_incorrect() {
        let may_has_null = true;
        let capacity = 10;
        let offset = 3;
        let bool_vec = Vec::from([
            false, true, false, false, true, true, false, true, true, true,
        ]);

        let raw_bridge_size = bool_vec
            .iter()
            .filter(|value| **value)
            .fold(0, |acc, value| if *value { acc + 1 } else { acc });

        let row_range_set = create_row_range_set(offset, &bool_vec);
        let mut raw_bridge = create_byte_array_bridge(may_has_null, capacity, &row_range_set);
        assert_eq!(raw_bridge.may_has_null(), true);
        assert_eq!(raw_bridge.non_null_data.len(), 0);
        assert_eq!(raw_bridge.nullable_data.len(), raw_bridge_size);

        assert!(raw_bridge.as_nullable().is_err());
    }

    #[test]
    fn test_transfer_random_non_null_results() {
        let may_has_null = false;
        let capacity = 10000;
        let offset = 3;
        let num_tests = 100;

        for _ in 0..num_tests {
            let self_bool_vec = create_random_bool_vec(capacity);
            let other_bool_vec = create_random_bool_vec_sub_set(&self_bool_vec);

            let self_range = create_row_range_set(offset, &self_bool_vec);
            let other_range = create_row_range_set(offset, &other_bool_vec);

            let mut self_raw_bridge = create_byte_array_bridge(may_has_null, capacity, &self_range);
            let mut other_raw_bridge = ByteArrayBridge::new(may_has_null, capacity);
            assert!(self_raw_bridge
                .transfer_values(&self_range, &other_range, &mut other_raw_bridge)
                .is_ok());

            verify_byte_array_raw_bridge_data(&other_raw_bridge, &other_range);
        }
    }

    #[test]
    fn test_transfer_random_nullable_results() {
        let may_has_null = true;
        let capacity = 10000;
        let offset = 3;
        let num_tests = 100;

        for _ in 0..num_tests {
            let self_bool_vec = create_random_bool_vec(capacity);
            let other_bool_vec = create_random_bool_vec_sub_set(&self_bool_vec);

            let self_range = create_row_range_set(offset, &self_bool_vec);
            let other_range = create_row_range_set(offset, &other_bool_vec);

            let mut self_raw_bridge = create_byte_array_bridge(may_has_null, capacity, &self_range);
            let mut other_raw_bridge = ByteArrayBridge::new(may_has_null, capacity);
            assert!(self_raw_bridge
                .transfer_values(&self_range, &other_range, &mut other_raw_bridge)
                .is_ok());

            verify_byte_array_raw_bridge_data(&other_raw_bridge, &other_range);
        }
    }

    #[test]
    fn test_convert_to_arrow() {
        let may_has_null = true;
        let capacity = 1000;
        let offset = 3;

        let bool_vec = Vec::from([false, true, false, false, true, true, false, true, true]);
        let row_range_set = create_row_range_set(offset, &bool_vec);

        let mut raw_bridge = create_byte_array_bridge(may_has_null, capacity, &row_range_set);

        assert!(raw_bridge.to_byte_array_arrow_array().is_ok());
    }
}
