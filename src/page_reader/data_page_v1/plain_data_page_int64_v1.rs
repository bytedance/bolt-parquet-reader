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
use std::mem;

use crate::bridge::result_bridge::ResultBridge;
use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
use crate::metadata::parquet_metadata_thrift;
use crate::metadata::parquet_metadata_thrift::PageHeader;
use crate::page_reader::data_page_v1::data_page_base::DataPage;
use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
use crate::utils::exceptions::BoltReaderError;
use crate::utils::row_range_set::{RowRange, RowRangeSet, RowRangeSetGenerator};

// Currently, we display 10 pieces of data only
// todo: Create config module to handle the default const values.
const DEFAULT_DISPLAY_NUMBER: usize = 10;

/// The Parquet Page Reader V1 Struct
/// current_offset: the offset in the whole column

pub struct PlainDataPageReaderInt64V1<'a> {
    has_null: bool,
    num_values: usize,
    current_offset: usize,
    type_size: usize,
    #[allow(dead_code)]
    zero_copy: bool,
    non_null_index: usize,
    nullable_index: usize,
    filter: Option<&'a dyn FixedLengthRangeFilter>,
    validity: Option<Vec<bool>>,
    data: Vec<i64>,
    data_with_nulls: Option<Vec<Option<i64>>>,
    nullable_selectivity: Option<Vec<bool>>,
}

impl<'a> Drop for PlainDataPageReaderInt64V1<'a> {
    fn drop(&mut self) {
        let data = mem::take(&mut self.data);
        if self.is_zero_copied() {
            mem::forget(data);
        }
    }
}

#[allow(dead_code)]
impl<'a> std::fmt::Display for PlainDataPageReaderInt64V1<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let data_str = self
            .data
            .iter()
            .take(DEFAULT_DISPLAY_NUMBER)
            .map(i64::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        let validity_str = match &self.validity {
            None => "true, ".repeat(DEFAULT_DISPLAY_NUMBER - 1) + "true",
            Some(validity) => validity
                .iter()
                .take(DEFAULT_DISPLAY_NUMBER)
                .map(bool::to_string)
                .collect::<Vec<String>>()
                .join(", "),
        };

        writeln!(
            f,
            "Plain Data Page Int64: has_null {}, num_values {}, current_offset {}\nData: {} ...\nValidity: {} ...",
             self.has_null, self.num_values, self.current_offset, data_str, validity_str
        )
    }
}

impl<'a> DataPage for PlainDataPageReaderInt64V1<'a> {
    fn data_page_has_null(&self) -> bool {
        self.has_null
    }

    fn get_data_page_num_values(&self) -> usize {
        self.num_values
    }

    fn get_data_page_offset(&self) -> usize {
        self.current_offset
    }

    fn get_data_page_type_size(&self) -> usize {
        self.type_size
    }

    fn is_zero_copied(&self) -> bool {
        self.zero_copy
    }

    fn read(
        &mut self,
        to_read: RowRange,
        offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<bool, BoltReaderError> {
        let start = to_read.begin + offset - self.current_offset;
        let end = to_read.end + offset - self.current_offset;

        let finished = if self.has_null {
            let validity = self.validity.as_ref().unwrap();
            let data_with_nulls = self.data_with_nulls.as_mut().unwrap();

            for i in self.nullable_index..end {
                if validity[i] {
                    data_with_nulls[i] = Some(self.data[self.non_null_index]);
                    self.non_null_index += 1;
                }
            }

            self.nullable_index = end;
            result_row_range_set.add_row_ranges(
                to_read.begin + offset - result_row_range_set.get_offset(),
                to_read.end + offset - result_row_range_set.get_offset(),
            );
            result_bridge.append_nullable_int64_results(&data_with_nulls[start..end])?;
            end == self.num_values
        } else {
            result_row_range_set.add_row_ranges(
                to_read.begin + offset - result_row_range_set.get_offset(),
                to_read.end + offset - result_row_range_set.get_offset(),
            );
            result_bridge.append_non_null_int64_results(&self.data[start..end])?;
            end == self.num_values
        };
        Ok(finished)
    }

    fn read_with_filter(
        &mut self,
        to_read: RowRange,
        offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<bool, BoltReaderError> {
        if self.has_null {
            self.read_nullable_with_filter(to_read, offset, result_row_range_set, result_bridge)
        } else {
            self.read_non_null_with_filter(to_read, offset, result_row_range_set, result_bridge)
        }
    }
}

#[allow(clippy::too_many_arguments)]
impl<'a> PlainDataPageReaderInt64V1<'a> {
    pub fn new(
        page_header: &PageHeader,
        buffer: &mut dyn ByteBufferBase,
        current_offset: usize,
        type_size: usize,
        has_null: bool,
        data_size: usize,
        filter: Option<&'a (dyn FixedLengthRangeFilter + 'a)>,
        validity: Option<Vec<bool>>,
    ) -> Result<PlainDataPageReaderInt64V1<'a>, BoltReaderError> {
        let header = match &page_header.data_page_header {
            Some(data_page_v1) => data_page_v1,
            None => {
                return Err(BoltReaderError::FixedLengthDataPageError(String::from(
                    "Error when reading Data Page V1 Header",
                )))
            }
        };

        let num_values = header.num_values as usize;
        let encoding = header.encoding;

        if encoding != parquet_metadata_thrift::Encoding::PLAIN {
            return Err(BoltReaderError::FixedLengthDataPageError(String::from(
                "Plain Data Page Encoding should be PLAIN",
            )));
        }

        let zero_copy;
        let data: Vec<i64> = if buffer.can_create_buffer_slice(buffer.get_rpos(), data_size) {
            zero_copy = true;
            let res = DirectByteBuffer::convert_byte_vec(
                buffer.load_bytes_to_byte_vec(buffer.get_rpos(), data_size)?,
                type_size,
            )?;
            buffer.set_rpos(buffer.get_rpos() + data_size);

            res
        } else {
            zero_copy = false;
            DirectByteBuffer::convert_byte_vec(
                buffer.load_bytes_to_byte_vec_deep_copy(buffer.get_rpos(), data_size)?,
                type_size,
            )?
        };

        let data_with_nulls = if has_null {
            Some(vec![None; num_values])
        } else {
            None
        };

        let nullable_selectivity = if has_null && filter.is_some() {
            Some(vec![false; num_values])
        } else {
            None
        };

        Ok(PlainDataPageReaderInt64V1 {
            has_null,
            num_values,
            current_offset,
            type_size,
            zero_copy,
            non_null_index: 0,
            nullable_index: 0,
            filter,
            validity,
            data,
            data_with_nulls,
            nullable_selectivity,
        })
    }

    pub fn read_non_null_with_filter(
        &self,
        to_read: RowRange,
        offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<bool, BoltReaderError> {
        let start = to_read.begin + offset - self.current_offset;
        let end = to_read.end + offset - self.current_offset;
        let finished = end == self.num_values;
        let filter = self.filter.as_ref().unwrap();
        let mut generator = RowRangeSetGenerator::new(result_row_range_set);

        for i in start..end {
            let filter_res = filter.check_i64(self.data[i]);
            generator.update(i + self.current_offset - offset, filter_res);
            if filter_res {
                result_bridge.append_non_null_int64_result(self.data[i])?;
            }
        }
        generator.finish(end + self.current_offset - offset);

        Ok(finished)
    }

    pub fn read_nullable_with_filter(
        &mut self,
        to_read: RowRange,
        offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<bool, BoltReaderError> {
        let start = to_read.begin + offset - self.current_offset;
        let end = to_read.end + offset - self.current_offset;
        let finished = end == self.num_values;
        let filter = self.filter.as_ref().unwrap();
        let validity = self.validity.as_ref().unwrap();
        let data_with_nulls = self.data_with_nulls.as_mut().unwrap();
        let nullable_selectivity = self.nullable_selectivity.as_mut().unwrap();

        for i in self.nullable_index..end {
            if validity[i] {
                let raw_data = self.data[self.non_null_index];
                data_with_nulls[i] = Some(raw_data);
                nullable_selectivity[i] = filter.check_i64_with_validity(raw_data, validity[i]);

                self.non_null_index += 1;
            } else {
                nullable_selectivity[i] =
                    filter.check_i64_with_validity(i64::default(), validity[i]);
            }
        }

        self.nullable_index = end;

        let mut generator = RowRangeSetGenerator::new(result_row_range_set);

        for i in start..end {
            generator.update(i + self.current_offset - offset, nullable_selectivity[i]);
            if nullable_selectivity[i] {
                result_bridge.append_nullable_int64_result(data_with_nulls[i])?;
            }
        }
        generator.finish(end + self.current_offset - offset);

        Ok(finished)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;
    use std::rc::Rc;

    use crate::bridge::int64_bridge::Int64Bridge;
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
    use crate::filters::integer_range_filter::IntegerRangeFilter;
    use crate::metadata::page_header::read_page_header;
    use crate::metadata::parquet_metadata_thrift::Encoding;
    use crate::page_reader::data_page_v1::data_page_base::{get_data_page_covered_range, DataPage};
    use crate::page_reader::data_page_v1::plain_data_page_int64_v1::PlainDataPageReaderInt64V1;
    use crate::utils::byte_buffer_base::ByteBufferBase;
    use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
    use crate::utils::exceptions::BoltReaderError;
    use crate::utils::file_loader::{FileLoader, FileLoaderEnum};
    use crate::utils::file_streaming_byte_buffer::{FileStreamingBuffer, StreamingByteBuffer};
    use crate::utils::local_file_loader::LocalFileLoader;
    use crate::utils::rep_def_parser::RepDefParser;
    use crate::utils::row_range_set::{RowRange, RowRangeSet};
    use crate::utils::test_utils::test_utils::{
        verify_plain_int64_non_null_result, verify_plain_int64_nullable_result,
    };

    fn load_plain_data_page_int64<'a>(
        buf: &'a mut dyn ByteBufferBase,
        filter: Option<&'a (dyn FixedLengthRangeFilter + 'a)>,
        offset: usize,
    ) -> Result<PlainDataPageReaderInt64V1<'a>, BoltReaderError> {
        let res = read_page_header(buf);
        assert!(res.is_ok());

        let page_header = res.unwrap();
        let data_page_header = page_header.data_page_header.as_ref().unwrap();

        let rpos = buf.get_rpos();
        let rep_rle_bp = data_page_header.repetition_level_encoding == Encoding::RLE
            || data_page_header.repetition_level_encoding == Encoding::BIT_PACKED;

        let def_rle_bp = data_page_header.definition_level_encoding == Encoding::RLE
            || data_page_header.definition_level_encoding == Encoding::BIT_PACKED;

        let validity = RepDefParser::parse_rep_def(
            buf,
            data_page_header.num_values as usize,
            0,
            rep_rle_bp,
            1,
            def_rle_bp,
        )
        .unwrap();

        let data_size = page_header.uncompressed_page_size - (buf.get_rpos() - rpos) as i32;

        PlainDataPageReaderInt64V1::new(
            &page_header,
            buf,
            offset,
            8,
            validity.0,
            data_size as usize,
            filter,
            validity.1,
        )
    }

    #[test]
    fn test_create_fixed_length_plain_data_page_v1() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_plain_data_page_int64(&mut buf, None, 100);
        assert!(res.is_ok());
        let data_page = res.unwrap();

        assert_eq!(data_page.to_string(), "Plain Data Page Int64: has_null false, num_values 131072, current_offset 100\nData: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ...\nValidity: true, true, true, true, true, true, true, true, true, true ...\n");
    }

    #[test]
    fn test_create_nullable_fixed_length_plain_data_page_v1() {
        let path = String::from("src/sample_files/plain_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_plain_data_page_int64(&mut buf, None, 100);
        assert!(res.is_ok());
        let data_page = res.unwrap();

        assert_eq!(data_page.to_string(), "Plain Data Page Int64: has_null true, num_values 174080, current_offset 100\nData: 1, 2, 3, 4, 6, 7, 8, 9, 11, 12 ...\nValidity: false, true, true, true, true, false, true, true, true, true ...\n");
    }

    #[test]
    fn test_create_fixed_length_plain_data_page_v1_drop() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_plain_data_page_int64(&mut buf, None, 100);

        assert!(res.is_ok());
        let boolean_zero_copy_page_reader = res.unwrap();

        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_plain_data_page_int64(&mut buf, None, 100);

        assert!(res.is_ok());
        let boolean_data_page_deep_copy = res.unwrap();

        assert_eq!(boolean_zero_copy_page_reader.is_zero_copied(), true);
        assert_eq!(boolean_data_page_deep_copy.is_zero_copied(), false);

        // Both deep copy and zero data page should be safely release at this point.
    }

    #[test]
    fn test_create_nullable_fixed_length_plain_data_page_v1_drop() {
        let path = String::from("src/sample_files/plain_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_plain_data_page_int64(&mut buf, None, 100);

        assert!(res.is_ok());
        let boolean_zero_copy_page_reader = res.unwrap();

        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_plain_data_page_int64(&mut buf, None, 100);

        assert!(res.is_ok());
        let boolean_data_page_deep_copy = res.unwrap();

        assert_eq!(boolean_zero_copy_page_reader.is_zero_copied(), true);
        assert_eq!(boolean_data_page_deep_copy.is_zero_copied(), false);

        // Both deep copy and zero data page should be safely release at this point.
    }

    #[test]
    fn test_read_plain_data_page_int64() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let res = load_plain_data_page_int64(&mut buf, None, 100);

        assert!(res.is_ok());
        let mut boolean_page_reader = res.unwrap();

        let num_values = boolean_page_reader.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = boolean_page_reader.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                boolean_page_reader.get_data_page_offset(),
                boolean_page_reader.get_data_page_offset()
                    + boolean_page_reader.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(false, capacity);
            let res = boolean_page_reader.read(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());

            verify_plain_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

            begin = end;
        }
    }

    #[test]
    fn test_read_plain_data_page_i64_with_filter() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let filter = IntegerRangeFilter::new(0, 54914, false);

        let res = load_plain_data_page_int64(&mut buf, Some(&filter), 100);

        assert!(res.is_ok());
        let mut boolean_page_reader = res.unwrap();

        let num_values = boolean_page_reader.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = boolean_page_reader.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                boolean_page_reader.get_data_page_offset(),
                boolean_page_reader.get_data_page_offset()
                    + boolean_page_reader.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(false, capacity);
            let res = boolean_page_reader.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());
            verify_plain_int64_non_null_result(&result_row_range_set, &raw_bridge, Some(&filter));
            begin = end;
        }
    }

    #[test]
    fn test_read_plain_data_page_i64_in_streaming_buffer() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let res = load_plain_data_page_int64(&mut buf, None, 100);

        assert!(res.is_ok());
        let mut boolean_page_reader = res.unwrap();

        let num_values = boolean_page_reader.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = boolean_page_reader.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                boolean_page_reader.get_data_page_offset(),
                boolean_page_reader.get_data_page_offset()
                    + boolean_page_reader.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(false, capacity);
            let res = boolean_page_reader.read(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());
            verify_plain_int64_non_null_result(&result_row_range_set, &raw_bridge, None);
            begin = end;
        }
    }

    #[test]
    fn test_read_boolean_data_page_with_filter_in_streaming_buffer() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let filter = IntegerRangeFilter::new(0, 54914, false);

        let res = load_plain_data_page_int64(&mut buf, Some(&filter), 100);

        assert!(res.is_ok());
        let mut boolean_page_reader = res.unwrap();

        let num_values = boolean_page_reader.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = boolean_page_reader.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                boolean_page_reader.get_data_page_offset(),
                boolean_page_reader.get_data_page_offset()
                    + boolean_page_reader.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(false, capacity);
            let res = boolean_page_reader.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());
            verify_plain_int64_non_null_result(&result_row_range_set, &raw_bridge, Some(&filter));
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_plain_data_page_int64() {
        let path = String::from("src/sample_files/plain_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let res = load_plain_data_page_int64(&mut buf, None, 100);

        assert!(res.is_ok());
        let mut boolean_page_reader = res.unwrap();

        let num_values = boolean_page_reader.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = boolean_page_reader.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                boolean_page_reader.get_data_page_offset(),
                boolean_page_reader.get_data_page_offset()
                    + boolean_page_reader.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(true, capacity);
            let res = boolean_page_reader.read(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());

            verify_plain_int64_nullable_result(&result_row_range_set, &raw_bridge, None);

            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_plain_data_page_i64_with_filter() {
        let path = String::from("src/sample_files/plain_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let filter = IntegerRangeFilter::new(0, 54914, false);

        let res = load_plain_data_page_int64(&mut buf, Some(&filter), 100);

        assert!(res.is_ok());
        let mut boolean_page_reader = res.unwrap();

        let num_values = boolean_page_reader.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = boolean_page_reader.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                boolean_page_reader.get_data_page_offset(),
                boolean_page_reader.get_data_page_offset()
                    + boolean_page_reader.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(true, capacity);
            let res = boolean_page_reader.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());
            verify_plain_int64_nullable_result(&result_row_range_set, &raw_bridge, Some(&filter));
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_plain_data_page_i64_in_streaming_buffer() {
        let path = String::from("src/sample_files/plain_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let res = load_plain_data_page_int64(&mut buf, None, 100);

        assert!(res.is_ok());
        let mut boolean_page_reader = res.unwrap();

        let num_values = boolean_page_reader.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = boolean_page_reader.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                boolean_page_reader.get_data_page_offset(),
                boolean_page_reader.get_data_page_offset()
                    + boolean_page_reader.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(true, capacity);
            let res = boolean_page_reader.read(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());
            verify_plain_int64_nullable_result(&result_row_range_set, &raw_bridge, None);
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_boolean_data_page_with_filter_in_streaming_buffer() {
        let path = String::from("src/sample_files/plain_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let filter = IntegerRangeFilter::new(0, 54914, false);

        let res = load_plain_data_page_int64(&mut buf, Some(&filter), 100);

        assert!(res.is_ok());
        let mut boolean_page_reader = res.unwrap();

        let num_values = boolean_page_reader.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = boolean_page_reader.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                boolean_page_reader.get_data_page_offset(),
                boolean_page_reader.get_data_page_offset()
                    + boolean_page_reader.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(true, capacity);
            let res = boolean_page_reader.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());
            verify_plain_int64_nullable_result(&result_row_range_set, &raw_bridge, Some(&filter));
            begin = end;
        }
    }
}
