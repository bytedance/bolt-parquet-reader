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

use crate::bridge::bridge_base::Bridge;
use crate::bridge::result_bridge::ResultBridge;
use crate::utils::exceptions::BoltReaderError;
use crate::utils::row_range_set::{RowRange, RowRangeSet};

pub enum PhysicalDataType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    None,
}

pub trait ColumnReaderNew {
    fn get_column_num_values(&self) -> usize;

    fn get_data_type_size(&self) -> usize;

    fn read(
        &mut self,
        to_read: RowRange,
        to_read_offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<(), BoltReaderError>;

    fn read_with_filter(
        &mut self,
        to_read: RowRange,
        to_read_offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<(), BoltReaderError>;
}

pub trait ColumnReader<T> {
    fn get_column_num_values(&self) -> usize;

    fn get_data_type_size(&self) -> usize;

    fn read(
        &mut self,
        to_read: RowRange,
        to_read_offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn Bridge<T>,
    ) -> Result<(), BoltReaderError>;

    fn read_with_filter(
        &mut self,
        to_read: RowRange,
        to_read_offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn Bridge<T>,
    ) -> Result<(), BoltReaderError>;
}
