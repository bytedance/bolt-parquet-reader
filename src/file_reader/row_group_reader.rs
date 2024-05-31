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

use std::cmp::min;
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;

use crate::bridge::boolean_bridge::BooleanBridge;
use crate::bridge::byte_array_bridge::ByteArrayBridge;
use crate::bridge::float32_bridge::Float32Bridge;
use crate::bridge::float64_bridge::Float64Bridge;
use crate::bridge::int32_bridge::Int32Bridge;
use crate::bridge::int64_bridge::Int64Bridge;
use crate::bridge::result_bridge::{ResultBridge, ResultBridgeEnum};
use crate::column_reader::column_reader_base::{ColumnReader, PhysicalDataType};
use crate::column_reader::primitive_column_reader::PrimitiveColumnReader;
use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
use crate::metadata::parquet_metadata_thrift::RowGroup;
use crate::metadata::utils::ColumnSchema;
use crate::utils::byte_buffer_base::BufferEnum;
use crate::utils::exceptions::BoltReaderError;
use crate::utils::row_range_set::{RowRange, RowRangeSet};
use crate::utils::shared_memory_buffer::SharedMemoryBuffer;

pub struct RowGroupReader<'a> {
    num_values: usize,
    reading_index: usize,
    columns_with_filters: Vec<PrimitiveColumnReader<'a>>,
    columns_without_filters: Vec<PrimitiveColumnReader<'a>>,
}

// TODO: 1. the path will be refactored to be a buffer loader
// TODO: 2. Support complex type
impl<'a> RowGroupReader<'a> {
    // TODO: Change the buffer to BufferEnum
    pub fn from_buffer(
        buffer: SharedMemoryBuffer,
        row_group_meta_data: &RowGroup,
        column_schema: &HashMap<String, ColumnSchema>,
        columns_to_read: &HashMap<String, Option<&'a dyn FixedLengthRangeFilter>>,
    ) -> Result<RowGroupReader<'a>, BoltReaderError> {
        let mut columns_with_filters: Vec<PrimitiveColumnReader<'a>> = Default::default();
        let mut columns_without_filters: Vec<PrimitiveColumnReader<'a>> = Default::default();

        for (column, filter) in columns_to_read {
            let schema = column_schema.get(column);
            match schema {
                Some(schema) => {
                    let column_metadata = row_group_meta_data.columns[schema.column_idx]
                        .meta_data
                        .as_ref()
                        .unwrap();

                    if column_metadata.index_page_offset.is_some() {
                        return Err(BoltReaderError::NotYetImplementedError(String::from(
                            "Index page is not yet implemented",
                        )));
                    }

                    let row_group_file_offset =
                        Self::get_row_group_file_offset(row_group_meta_data)?;

                    let column_offset = if let Some(dictionary_page_offset) =
                        column_metadata.dictionary_page_offset
                    {
                        dictionary_page_offset as usize
                    } else {
                        column_metadata.data_page_offset as usize
                    } - row_group_file_offset as usize;

                    let column_buffer = SharedMemoryBuffer::from_shared_memory_buffer(
                        &buffer,
                        column_offset,
                        column_metadata.total_compressed_size as usize,
                    );

                    let mut primitive_column_reader = PrimitiveColumnReader::new(
                        schema.max_rep,
                        schema.max_def,
                        column.to_string(),
                        BufferEnum::SharedMemoryBuffer(column_buffer?),
                        *filter,
                    )?;
                    primitive_column_reader.prepare_column_reader(column_metadata)?;

                    match filter {
                        None => {
                            columns_without_filters.push(primitive_column_reader);
                        }
                        Some(_) => columns_with_filters.push(primitive_column_reader),
                    };
                }

                None => {
                    return Err(BoltReaderError::RowGroupReaderError(format!(
                        "Column {} is not found in the metadata",
                        column
                    )))
                }
            }
        }

        Ok(RowGroupReader {
            num_values: row_group_meta_data.num_rows as usize,
            reading_index: 0,
            columns_with_filters,
            columns_without_filters,
        })
    }

    pub fn get_row_group_file_offset(
        row_group_meta_data: &RowGroup,
    ) -> Result<i64, BoltReaderError> {
        match row_group_meta_data.file_offset {
            None => {
                let first_column_metadata = row_group_meta_data.columns[0].meta_data.as_ref();
                match first_column_metadata {
                    None => Err(BoltReaderError::FileReaderError(String::from(
                        "Unable to find the offset of the row group",
                    ))),
                    Some(first_column_metadata) => {
                        let dictionary_page_offset =
                            first_column_metadata.dictionary_page_offset.as_ref();
                        match dictionary_page_offset {
                            None => Ok(first_column_metadata.data_page_offset),
                            Some(dictionary_page_offset) => Ok(*dictionary_page_offset),
                        }
                    }
                }
            }

            Some(row_group_file_offset) => Ok(row_group_file_offset),
        }
    }

    pub fn prepare_result_bridge(
        column: &PrimitiveColumnReader,
        to_read_size: usize,
    ) -> Result<ResultBridgeEnum, BoltReaderError> {
        match column.get_physical_type() {
            PhysicalDataType::Boolean => Ok(ResultBridgeEnum::BooleanBridge(BooleanBridge::new(
                false,
                to_read_size,
            ))),
            PhysicalDataType::Int32 => Ok(ResultBridgeEnum::Int32Bridge(Int32Bridge::new(
                false,
                to_read_size,
            ))),

            PhysicalDataType::Int64 => Ok(ResultBridgeEnum::Int64Bridge(Int64Bridge::new(
                false,
                to_read_size,
            ))),

            PhysicalDataType::Float32 => Ok(ResultBridgeEnum::Float32Bridge(Float32Bridge::new(
                false,
                to_read_size,
            ))),

            PhysicalDataType::Float64 => Ok(ResultBridgeEnum::Float64Bridge(Float64Bridge::new(
                false,
                to_read_size,
            ))),

            PhysicalDataType::ByteArray => Ok(ResultBridgeEnum::ByteArrayBridge(
                ByteArrayBridge::new(false, to_read_size),
            )),

            PhysicalDataType::None => Err(BoltReaderError::RowGroupReaderError(String::from(
                "Unable to load the physical type",
            ))),
        }
    }

    pub fn read_with_filter(
        &mut self,
        initial_row_range_set: RowRangeSet,
        offset: usize,
        to_read_size: usize,
    ) -> Result<(HashMap<String, ResultBridgeEnum>, Rc<RowRangeSet>), BoltReaderError> {
        let mut row_range_sets: Vec<Rc<RowRangeSet>> = Default::default();
        let mut result_bridges: Vec<ResultBridgeEnum> = Default::default();

        let mut to_read_row_range_set = Rc::from(initial_row_range_set);
        for column in &mut self.columns_with_filters {
            let mut result_bridge = RowGroupReader::prepare_result_bridge(column, to_read_size)?;
            let mut result_row_range_set = RowRangeSet::new(offset);
            for to_read in to_read_row_range_set.get_row_ranges() {
                column.read_with_filter(
                    RowRange::new(to_read.begin, to_read.end),
                    offset,
                    &mut result_row_range_set,
                    &mut result_bridge,
                )?;
            }

            to_read_row_range_set = Rc::from(result_row_range_set);
            row_range_sets.push(Rc::clone(&to_read_row_range_set));
            result_bridges.push(result_bridge);
        }

        let final_row_range_set = row_range_sets[row_range_sets.len() - 1].clone();

        let last_filtered_result = result_bridges.pop().unwrap();
        let mut final_results: HashMap<String, ResultBridgeEnum> = Default::default();

        final_results.insert(
            self.columns_with_filters[self.columns_with_filters.len() - 1]
                .get_column_name()
                .clone(),
            last_filtered_result,
        );

        for i in 0..row_range_sets.len() - 1 {
            let mut final_result =
                RowGroupReader::prepare_result_bridge(&self.columns_with_filters[i], to_read_size)?;
            result_bridges[i].transfer_values(
                row_range_sets[i].deref(),
                final_row_range_set.deref(),
                &mut final_result,
            )?;
            final_results.insert(
                self.columns_with_filters[i].get_column_name().clone(),
                final_result,
            );
        }

        Ok((final_results, final_row_range_set))
    }

    #[allow(clippy::type_complexity)]
    pub fn read(
        &mut self,
        batch_size: usize,
    ) -> Result<(HashMap<String, ResultBridgeEnum>, Rc<RowRangeSet>, bool), BoltReaderError> {
        let offset = 0;
        let begin = self.reading_index;
        let to_read_size = min(batch_size, self.num_values - self.reading_index);
        let end = begin + to_read_size;
        self.reading_index += to_read_size;

        let mut initial_row_range_set = RowRangeSet::new(offset);
        initial_row_range_set.add_row_ranges(begin, end);

        let (mut result_bridges, to_read_row_range_set) = if !self.columns_with_filters.is_empty() {
            self.read_with_filter(initial_row_range_set, offset, to_read_size)?
        } else {
            (Default::default(), Rc::from(initial_row_range_set))
        };

        for column in &mut self.columns_without_filters {
            let mut result_bridge = RowGroupReader::prepare_result_bridge(column, to_read_size)?;
            let mut result_row_range_set = RowRangeSet::new(offset);
            for to_read in to_read_row_range_set.get_row_ranges() {
                column.read(
                    RowRange::new(to_read.begin, to_read.end),
                    offset,
                    &mut result_row_range_set,
                    &mut result_bridge,
                )?;
            }

            result_bridges.insert(column.get_column_name().clone(), result_bridge);
        }

        Ok((
            result_bridges,
            to_read_row_range_set,
            self.reading_index == self.num_values,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::rc::Rc;
    use std::string::String;

    use crate::bridge::result_bridge::ResultBridgeEnum;
    use crate::file_reader::row_group_reader::RowGroupReader;
    use crate::filters::boolean_filter::BooleanFilter;
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
    use crate::filters::float_point_range_filter::FloatPointRangeFilter;
    use crate::filters::integer_range_filter::IntegerRangeFilter;
    use crate::metadata::parquet_footer::FileMetaDataLoader;
    use crate::metadata::parquet_metadata_thrift::FileMetaData;
    use crate::metadata::utils::prepare_schema;
    use crate::utils::exceptions::BoltReaderError;
    use crate::utils::file_loader::FileLoader;
    use crate::utils::local_file_loader::LocalFileLoader;
    use crate::utils::row_range_set::RowRangeSet;
    use crate::utils::shared_memory_buffer::SharedMemoryBuffer;
    use crate::utils::test_utils::test_utils::{
        verify_boolean_non_null_result, verify_boolean_nullable_result,
        verify_plain_byte_array_non_null_result, verify_plain_byte_array_nullable_result,
        verify_plain_float32_non_null_result, verify_plain_float32_nullable_result,
        verify_plain_float64_non_null_result, verify_plain_float64_nullable_result,
        verify_plain_int32_non_null_result, verify_plain_int32_nullable_result,
        verify_plain_int64_non_null_result, verify_plain_int64_nullable_result,
        verify_rle_bp_byte_array_non_null_result, verify_rle_bp_byte_array_nullable_result,
        verify_rle_bp_float32_non_null_result, verify_rle_bp_float32_nullable_result,
        verify_rle_bp_float64_non_null_result, verify_rle_bp_float64_nullable_result,
        verify_rle_bp_int32_non_null_result, verify_rle_bp_int32_nullable_result,
        verify_rle_bp_int64_non_null_result, verify_rle_bp_int64_nullable_result,
    };

    const DEFAULT_PARQUET_FOOTER_PRELOAD_SIZE: usize = 1 << 20;

    const BOOLEAN_COLUMN: &str = "boolean";
    const INT32_COLUMN: &str = "int";
    const INT64_COLUMN: &str = "bigint";
    const FLOAT32_COLUMN: &str = "float";
    const FLOAT64_COLUMN: &str = "double";
    const STRING_COLUMN: &str = "string";

    fn load_parquet_footer(file: &String) -> FileMetaData {
        let metadata_loader =
            FileMetaDataLoader::new(&String::from(file), DEFAULT_PARQUET_FOOTER_PRELOAD_SIZE);
        assert!(metadata_loader.is_ok());
        let mut metadata_loader = metadata_loader.unwrap();
        let res = metadata_loader.load_parquet_footer();
        assert!(res.is_ok());
        res.unwrap()
    }

    fn load_row_group_reader<'a>(
        path: &'a String,
        columns_to_read: &HashMap<String, Option<&'a dyn FixedLengthRangeFilter>>,
    ) -> Result<RowGroupReader<'a>, BoltReaderError> {
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();

        let footer = load_parquet_footer(&path);
        let res = prepare_schema(&footer.schema);
        assert!(res.is_ok());
        let column_schemas = res.unwrap();

        let res = SharedMemoryBuffer::from_file(&file, 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let buffer = res.unwrap();

        RowGroupReader::from_buffer(
            buffer,
            &footer.row_groups[0],
            &column_schemas,
            &columns_to_read,
        )
    }

    fn verify_plain_non_null_results(
        columns_to_read: &HashMap<String, Option<&dyn FixedLengthRangeFilter>>,
        result: &HashMap<String, ResultBridgeEnum>,
        result_row_set: Rc<RowRangeSet>,
    ) {
        for (column, filter) in columns_to_read {
            if *column == String::from(BOOLEAN_COLUMN) {
                verify_boolean_non_null_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(INT32_COLUMN) {
                verify_plain_int32_non_null_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(INT64_COLUMN) {
                verify_plain_int64_non_null_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(FLOAT32_COLUMN) {
                verify_plain_float32_non_null_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(FLOAT64_COLUMN) {
                verify_plain_float64_non_null_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(STRING_COLUMN) {
                verify_plain_byte_array_non_null_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                );
            }
        }
    }

    fn verify_plain_nullable_results(
        columns_to_read: &HashMap<String, Option<&dyn FixedLengthRangeFilter>>,
        result: &HashMap<String, ResultBridgeEnum>,
        result_row_set: Rc<RowRangeSet>,
    ) {
        for (column, filter) in columns_to_read {
            if *column == String::from(BOOLEAN_COLUMN) {
                verify_boolean_nullable_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(INT32_COLUMN) {
                verify_plain_int32_nullable_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(INT64_COLUMN) {
                verify_plain_int64_nullable_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(FLOAT32_COLUMN) {
                verify_plain_float32_nullable_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(FLOAT64_COLUMN) {
                verify_plain_float64_nullable_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(STRING_COLUMN) {
                verify_plain_byte_array_nullable_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                );
            }
        }
    }

    fn verify_rle_bp_non_null_results(
        columns_to_read: &HashMap<String, Option<&dyn FixedLengthRangeFilter>>,
        result: &HashMap<String, ResultBridgeEnum>,
        result_row_set: Rc<RowRangeSet>,
    ) {
        for (column, filter) in columns_to_read {
            if *column == String::from(BOOLEAN_COLUMN) {
                verify_boolean_non_null_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(INT32_COLUMN) {
                verify_rle_bp_int32_non_null_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(INT64_COLUMN) {
                verify_rle_bp_int64_non_null_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(FLOAT32_COLUMN) {
                verify_rle_bp_float32_non_null_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(FLOAT64_COLUMN) {
                verify_rle_bp_float64_non_null_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(STRING_COLUMN) {
                verify_rle_bp_byte_array_non_null_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                );
            }
        }
    }

    fn verify_rle_bp_nullable_results(
        columns_to_read: &HashMap<String, Option<&dyn FixedLengthRangeFilter>>,
        result: &HashMap<String, ResultBridgeEnum>,
        result_row_set: Rc<RowRangeSet>,
    ) {
        for (column, filter) in columns_to_read {
            if *column == String::from(BOOLEAN_COLUMN) {
                verify_boolean_nullable_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(INT32_COLUMN) {
                verify_rle_bp_int32_nullable_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(INT64_COLUMN) {
                verify_rle_bp_int64_nullable_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(FLOAT32_COLUMN) {
                verify_rle_bp_float32_nullable_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(FLOAT64_COLUMN) {
                verify_rle_bp_float64_nullable_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                    *filter,
                );
            } else if *column == String::from(STRING_COLUMN) {
                verify_rle_bp_byte_array_nullable_result(
                    result_row_set.as_ref(),
                    result.get(column).unwrap(),
                );
            }
        }
    }

    #[test]
    fn test_create_row_group_reader() {
        let path = String::from("src/sample_files/plain_row_group.parquet");
        let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
            HashMap::new();

        let int_filter = IntegerRangeFilter::new(100, 10000, true);
        let float_filter =
            FloatPointRangeFilter::new(500.0, 5000.0, true, true, false, false, false);

        columns_to_read.insert(String::from(BOOLEAN_COLUMN), None);
        columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
        columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
        columns_to_read.insert(String::from(INT64_COLUMN), None);
        columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
        columns_to_read.insert(String::from(STRING_COLUMN), None);

        let res = load_row_group_reader(&path, &columns_to_read);
        assert!(res.is_ok());
    }

    #[test]
    fn test_plain_row_group_reader_without_filter() {
        let path = String::from("src/sample_files/plain_row_group.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), None);
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_without_filter_gzip() {
        let path = String::from("src/sample_files/plain_row_group_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), None);
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_one_filter() {
        let path = String::from("src/sample_files/plain_row_group.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_one_filter_gzip() {
        let path = String::from("src/sample_files/plain_row_group_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_two_filters() {
        let path = String::from("src/sample_files/plain_row_group.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_two_filters_gzip() {
        let path = String::from("src/sample_files/plain_row_group_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_three_filters() {
        let path = String::from("src/sample_files/plain_row_group.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_three_filters_gzip() {
        let path = String::from("src/sample_files/plain_row_group_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_four_filters() {
        let path = String::from("src/sample_files/plain_row_group.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_four_filters_gzip() {
        let path = String::from("src/sample_files/plain_row_group_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_five_filters() {
        let path = String::from("src/sample_files/plain_row_group.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);
            let double_filter =
                FloatPointRangeFilter::new(400.0, 600000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), Some(&double_filter));
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_five_filters_gzip() {
        let path = String::from("src/sample_files/plain_row_group_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);
            let double_filter =
                FloatPointRangeFilter::new(400.0, 600000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), Some(&double_filter));
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_without_filter_with_nulls() {
        let path = String::from("src/sample_files/plain_row_group_with_nulls.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), None);
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_without_filter_with_nulls_gzip() {
        let path = String::from("src/sample_files/plain_row_group_with_nulls_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), None);
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_one_filter_with_nulls() {
        let path = String::from("src/sample_files/plain_row_group_with_nulls.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_one_filter_with_nulls_gzip() {
        let path = String::from("src/sample_files/plain_row_group_with_nulls_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_two_filters_with_nulls() {
        let path = String::from("src/sample_files/plain_row_group_with_nulls.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_two_filters_with_nulls_gzip() {
        let path = String::from("src/sample_files/plain_row_group_with_nulls_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_three_filters_with_nulls() {
        let path = String::from("src/sample_files/plain_row_group_with_nulls.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_three_filters_with_nulls_gzip() {
        let path = String::from("src/sample_files/plain_row_group_with_nulls_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_four_filters_with_nulls() {
        let path = String::from("src/sample_files/plain_row_group_with_nulls.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_four_filters_with_nulls_gzip() {
        let path = String::from("src/sample_files/plain_row_group_with_nulls_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_five_filters_with_nulls() {
        let path = String::from("src/sample_files/plain_row_group_with_nulls.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);
            let double_filter =
                FloatPointRangeFilter::new(400.0, 600000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), Some(&double_filter));
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_plain_row_group_reader_with_five_filters_with_nulls_gzip() {
        let path = String::from("src/sample_files/plain_row_group_with_nulls_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);
            let double_filter =
                FloatPointRangeFilter::new(400.0, 600000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), Some(&double_filter));
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_plain_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_without_filter() {
        let path = String::from("src/sample_files/rle_bp_row_group.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), None);
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_without_filter_gzip() {
        let path = String::from("src/sample_files/rle_bp_row_group_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), None);
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_one_filter() {
        let path = String::from("src/sample_files/rle_bp_row_group.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_one_filter_gzip() {
        let path = String::from("src/sample_files/rle_bp_row_group_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_two_filters() {
        let path = String::from("src/sample_files/rle_bp_row_group.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_two_filters_gzip() {
        let path = String::from("src/sample_files/rle_bp_row_group_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_three_filters() {
        let path = String::from("src/sample_files/rle_bp_row_group.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_three_filters_gzip() {
        let path = String::from("src/sample_files/rle_bp_row_group_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_four_filters() {
        let path = String::from("src/sample_files/rle_bp_row_group.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_four_filters_gzip() {
        let path = String::from("src/sample_files/rle_bp_row_group_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_five_filters() {
        let path = String::from("src/sample_files/rle_bp_row_group.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);
            let double_filter =
                FloatPointRangeFilter::new(400.0, 600000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), Some(&double_filter));
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_five_filters_gzip() {
        let path = String::from("src/sample_files/rle_bp_row_group_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);
            let double_filter =
                FloatPointRangeFilter::new(400.0, 600000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), Some(&double_filter));
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_non_null_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_without_filter_with_nulls() {
        let path = String::from("src/sample_files/rle_bp_row_group_with_nulls.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), None);
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_without_filter_with_nulls_gzip() {
        let path = String::from("src/sample_files/rle_bp_row_group_with_nulls_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), None);
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_one_filter_with_nulls() {
        let path = String::from("src/sample_files/rle_bp_row_group_with_nulls.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_one_filter_with_nulls_gzip() {
        let path = String::from("src/sample_files/rle_bp_row_group_with_nulls_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_two_filters_with_nulls() {
        let path = String::from("src/sample_files/rle_bp_row_group_with_nulls.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_two_filters_with_nulls_gzip() {
        let path = String::from("src/sample_files/rle_bp_row_group_with_nulls_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_three_filters_with_nulls() {
        let path = String::from("src/sample_files/rle_bp_row_group_with_nulls.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_three_filters_with_nulls_gzip() {
        let path = String::from("src/sample_files/rle_bp_row_group_with_nulls_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_four_filters_with_nulls() {
        let path = String::from("src/sample_files/rle_bp_row_group_with_nulls.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_four_filters_with_nulls_gzip() {
        let path = String::from("src/sample_files/rle_bp_row_group_with_nulls_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_five_filters_with_nulls() {
        let path = String::from("src/sample_files/rle_bp_row_group_with_nulls.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);
            let double_filter =
                FloatPointRangeFilter::new(400.0, 600000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), Some(&double_filter));
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }

    #[test]
    fn test_rle_bp_row_group_reader_with_five_filters_with_nulls_gzip() {
        let path = String::from("src/sample_files/rle_bp_row_group_with_nulls_gzip.parquet");
        let base_step = 1000;

        for i in 0..3 {
            let step = base_step * (1 << i);
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();

            let boolean_filter = BooleanFilter::new(true, false);
            let int_filter = IntegerRangeFilter::new(100, 900000, true);
            let float_filter =
                FloatPointRangeFilter::new(200.0, 800000.0, true, true, false, false, false);
            let bigint_filter = IntegerRangeFilter::new(300, 700000, true);
            let double_filter =
                FloatPointRangeFilter::new(400.0, 600000.0, true, true, false, false, false);

            columns_to_read.insert(String::from(BOOLEAN_COLUMN), Some(&boolean_filter));
            columns_to_read.insert(String::from(INT32_COLUMN), Some(&int_filter));
            columns_to_read.insert(String::from(FLOAT32_COLUMN), Some(&float_filter));
            columns_to_read.insert(String::from(INT64_COLUMN), Some(&bigint_filter));
            columns_to_read.insert(String::from(FLOAT64_COLUMN), Some(&double_filter));
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = load_row_group_reader(&path, &columns_to_read);
            assert!(res.is_ok());
            let mut row_group_reader = res.unwrap();

            let mut finished = false;
            while !finished {
                let res = row_group_reader.read(step);
                assert!(res.is_ok());
                let res = res.unwrap();
                let result = res.0;
                let row_range_set = res.1;
                finished = res.2;
                verify_rle_bp_nullable_results(&columns_to_read, &result, row_range_set);
            }
        }
    }
}
