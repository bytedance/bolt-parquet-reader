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

use crate::utils::exceptions::BoltReaderError;
use crate::utils::row_range_set::RowRangeSet;
pub trait Bridge<T> {
    fn get_bridge_name(&self) -> String;

    fn is_empty(&self) -> bool;

    fn get_size(&self) -> usize;

    fn may_has_null(&self) -> bool;

    fn append_result(&mut self, validity: bool, result: T);

    #[inline(always)]
    fn append_non_null_result(&mut self, result: T) {
        self.append_result(true, result);
    }

    fn append_results(&mut self, validity: &[bool], result: &[T]) -> Result<(), BoltReaderError>;

    #[inline(always)]
    fn append_non_null_results(&mut self, results: &[T]) -> Result<(), BoltReaderError> {
        self.append_results(&vec![true; results.len()], results)
    }

    fn get_validity_and_value(
        &self,
        _offset: usize,
        _index: usize,
        _ranges: &RowRangeSet,
    ) -> Result<(bool, T), BoltReaderError> {
        Err(BoltReaderError::BridgeError(format!(
            "get_validity_and_value() is not supported on {}",
            self.get_bridge_name()
        )))
    }
}
