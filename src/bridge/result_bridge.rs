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

use arrow::array::{BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array};

use crate::utils::exceptions::BoltReaderError;
use crate::utils::row_range_set::RowRangeSet;

pub enum BridgeType {
    RustVec,
}

pub enum BridgeDataType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
}

pub trait ResultBridge {
    fn get_bridge_name(&self) -> String;

    fn is_empty(&self) -> bool;

    fn get_size(&self) -> usize;

    fn may_has_null(&self) -> bool;

    fn set_may_has_null(&mut self, may_has_null: bool);

    fn append_nullable_bool_result(&mut self, _: Option<bool>) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append bool value to {}",
            self.get_bridge_name()
        )))
    }

    fn append_nullable_bool_results(&mut self, _: &[Option<bool>]) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append bool values to {}",
            self.get_bridge_name()
        )))
    }

    fn append_non_null_bool_result(&mut self, _: bool) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append bool value to {}",
            self.get_bridge_name()
        )))
    }

    fn append_non_null_bool_results(&mut self, _: &[bool]) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append bool values to {}",
            self.get_bridge_name()
        )))
    }

    fn get_bool_validity_and_value(
        &self,
        _offset: usize,
        _index: usize,
        _ranges: &RowRangeSet,
    ) -> Result<(bool, bool), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "get_bool_validity_and_value() is not supported on {}",
            self.get_bridge_name()
        )))
    }

    fn to_bool_arrow_array(&mut self) -> Result<BooleanArray, BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot construct boolean arrow array from {}",
            self.get_bridge_name()
        )))
    }

    fn append_nullable_int32_result(&mut self, _: Option<i32>) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append int32 value to {}",
            self.get_bridge_name()
        )))
    }

    fn append_nullable_int32_results(&mut self, _: &[Option<i32>]) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append int32 values to {}",
            self.get_bridge_name()
        )))
    }

    fn append_non_null_int32_result(&mut self, _: i32) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append int32 value to {}",
            self.get_bridge_name()
        )))
    }

    fn append_non_null_int32_results(&mut self, _: &[i32]) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append int32 values to {}",
            self.get_bridge_name()
        )))
    }

    fn get_int32_validity_and_value(
        &self,
        _offset: usize,
        _index: usize,
        _ranges: &RowRangeSet,
    ) -> Result<(bool, i32), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "get_validity_and_value_int32() is not supported on {}",
            self.get_bridge_name()
        )))
    }

    fn to_int32_arrow_array(&mut self) -> Result<Int32Array, BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot construct int32 arrow array from {}",
            self.get_bridge_name()
        )))
    }

    fn append_nullable_int64_result(&mut self, _: Option<i64>) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append int64 value to {}",
            self.get_bridge_name()
        )))
    }

    fn append_nullable_int64_results(&mut self, _: &[Option<i64>]) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append int64 values to {}",
            self.get_bridge_name()
        )))
    }

    fn append_non_null_int64_result(&mut self, _: i64) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append int64 value to {}",
            self.get_bridge_name()
        )))
    }

    fn append_non_null_int64_results(&mut self, _: &[i64]) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append int64 values to {}",
            self.get_bridge_name()
        )))
    }

    fn get_int64_validity_and_value(
        &self,
        _offset: usize,
        _index: usize,
        _ranges: &RowRangeSet,
    ) -> Result<(bool, i64), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "get_validity_and_value_int64() is not supported on {}",
            self.get_bridge_name()
        )))
    }

    fn to_int64_arrow_array(&mut self) -> Result<Int64Array, BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot construct int64 arrow array from {}",
            self.get_bridge_name()
        )))
    }

    fn append_nullable_float32_result(&mut self, _: Option<f32>) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append float32 value to {}",
            self.get_bridge_name()
        )))
    }

    fn append_nullable_float32_results(
        &mut self,
        _: &[Option<f32>],
    ) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append float32 values to {}",
            self.get_bridge_name()
        )))
    }

    fn append_non_null_float32_result(&mut self, _: f32) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append float32 value to {}",
            self.get_bridge_name()
        )))
    }

    fn append_non_null_float32_results(&mut self, _: &[f32]) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append float32 values to {}",
            self.get_bridge_name()
        )))
    }

    fn get_float32_validity_and_value(
        &self,
        _offset: usize,
        _index: usize,
        _ranges: &RowRangeSet,
    ) -> Result<(bool, f32), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "get_validity_and_value_float32() is not supported on {}",
            self.get_bridge_name()
        )))
    }

    fn to_float32_arrow_array(&mut self) -> Result<Float32Array, BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot construct float32 arrow array from {}",
            self.get_bridge_name()
        )))
    }

    fn append_nullable_float64_result(&mut self, _: Option<f64>) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append float64 value to {}",
            self.get_bridge_name()
        )))
    }

    fn append_nullable_float64_results(
        &mut self,
        _: &[Option<f64>],
    ) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append float64 values to {}",
            self.get_bridge_name()
        )))
    }

    fn append_non_null_float64_result(&mut self, _: f64) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append float64 value to {}",
            self.get_bridge_name()
        )))
    }

    fn append_non_null_float64_results(&mut self, _: &[f64]) -> Result<(), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot append float64 values to {}",
            self.get_bridge_name()
        )))
    }

    fn get_float64_validity_and_value(
        &self,
        _offset: usize,
        _index: usize,
        _ranges: &RowRangeSet,
    ) -> Result<(bool, f64), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "get_validity_and_value_float64() is not supported on {}",
            self.get_bridge_name()
        )))
    }

    fn to_float64_arrow_array(&mut self) -> Result<Float64Array, BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "Cannot construct float64 arrow array from {}",
            self.get_bridge_name()
        )))
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
    fn transfer_values(
        &mut self,
        self_ranges: &RowRangeSet,
        other_ranges: &RowRangeSet,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<(), BoltReaderError>;

    // Convert the non-null bridge to nullable bridge.
    fn as_nullable(&mut self) -> Result<(), BoltReaderError>;
}

#[cfg(test)]
pub mod bridge_tests_utils {
    use rand::Rng;

    use crate::utils::row_range_set::{RowRangeSet, RowRangeSetGenerator};

    pub fn create_row_range_set(offset: usize, bool_vec: &Vec<bool>) -> RowRangeSet {
        let mut row_range_set = RowRangeSet::new(offset);
        let mut generator = RowRangeSetGenerator::new(&mut row_range_set);

        for i in 0..bool_vec.len() {
            generator.update(i, bool_vec[i]);
        }
        generator.finish(bool_vec.len());
        row_range_set
    }

    pub fn create_random_bool_vec(vector_length: usize) -> Vec<bool> {
        let mut rng = rand::thread_rng();
        let random_bool_vec: Vec<bool> = (0..vector_length).map(|_| rng.gen::<bool>()).collect();

        random_bool_vec
    }

    pub fn create_random_bool_vec_sub_set(bool_vec: &Vec<bool>) -> Vec<bool> {
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
}
