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

pub struct BooleanDataPageReaderV1<'a> {
    has_null: bool,
    num_values: usize,
    current_offset: usize,
    type_size: usize,
    #[allow(dead_code)]
    zero_copy: bool,
    non_null_index: usize,
    nullable_index: usize,
    filter: Option<&'a dyn FixedLengthRangeFilter>,
    #[allow(dead_code)]
    validity: Option<Vec<bool>>,
    data: Vec<u8>,
    data_with_nulls: Option<Vec<Option<bool>>>,
    nullable_selectivity: Option<Vec<bool>>,
}

impl<'a> Drop for BooleanDataPageReaderV1<'a> {
    fn drop(&mut self) {
        let data = mem::take(&mut self.data);
        if self.is_zero_copied() {
            mem::forget(data);
        }
    }
}

#[allow(dead_code)]
impl<'a> std::fmt::Display for BooleanDataPageReaderV1<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Boolean Data Page: has_null {}, num_values {}, current_offset {}",
            self.has_null, self.num_values, self.current_offset
        )
    }
}

impl<'a> DataPage for BooleanDataPageReaderV1<'a> {
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

    // TODO: The Boolean data page is in Bit Pack by default. We can optimize it by using SIMD in the future.
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
                    data_with_nulls[i] = Some(
                        (self.data[self.non_null_index / 8] & 0x1 << (self.non_null_index % 8))
                            != 0,
                    );
                    self.non_null_index += 1;
                }
            }

            self.nullable_index = end;
            result_row_range_set.add_row_ranges(
                to_read.begin + offset - result_row_range_set.get_offset(),
                to_read.end + offset - result_row_range_set.get_offset(),
            );
            result_bridge.append_nullable_bool_results(&data_with_nulls[start..end])?;
            end == self.num_values
        } else {
            result_row_range_set.add_row_ranges(
                to_read.begin + offset - result_row_range_set.get_offset(),
                to_read.end + offset - result_row_range_set.get_offset(),
            );

            let start_byte = start / 8;
            let start_bit = start % 8;
            let end_byte = end / 8;
            let end_bit = end % 8;

            if start_byte < end_byte {
                for i in start_bit..8 {
                    result_bridge
                        .append_non_null_bool_result((self.data[start_byte] & (1 << i)) != 0)?;
                }
            }

            // This is to read 8 bits within a byte
            for i in start_byte + 1..end_byte {
                result_bridge.append_non_null_bool_result((self.data[i] & 0x1) != 0)?;
                result_bridge.append_non_null_bool_result((self.data[i] & 0x1 << 1) != 0)?;
                result_bridge.append_non_null_bool_result((self.data[i] & 0x1 << 2) != 0)?;
                result_bridge.append_non_null_bool_result((self.data[i] & 0x1 << 3) != 0)?;
                result_bridge.append_non_null_bool_result((self.data[i] & 0x1 << 4) != 0)?;
                result_bridge.append_non_null_bool_result((self.data[i] & 0x1 << 5) != 0)?;
                result_bridge.append_non_null_bool_result((self.data[i] & 0x1 << 6) != 0)?;
                result_bridge.append_non_null_bool_result((self.data[i] & 0x1 << 7) != 0)?;
            }

            for i in 0..end_bit {
                result_bridge.append_non_null_bool_result((self.data[end_byte] & (1 << i)) != 0)?;
            }

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

impl<'a> BooleanDataPageReaderV1<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        page_header: &PageHeader,
        buffer: &mut dyn ByteBufferBase,
        current_offset: usize,
        type_size: usize,
        has_null: bool,
        data_size: usize,
        filter: Option<&'a (dyn FixedLengthRangeFilter + 'a)>,
        validity: Option<Vec<bool>>,
    ) -> Result<BooleanDataPageReaderV1<'a>, BoltReaderError> {
        let header = match &page_header.data_page_header {
            Some(data_page_v1) => data_page_v1,
            None => {
                return Err(BoltReaderError::FixedLengthDataPageError(String::from(
                    "Error when reading Boolean Data Page V1 Header",
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
        let data: Vec<u8> = if buffer.can_create_buffer_slice(buffer.get_rpos(), data_size) {
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

        Ok(BooleanDataPageReaderV1 {
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

    #[inline(always)]
    pub fn read_bit(&self, byte_index: usize, bit_index: usize) -> bool {
        (self.data[byte_index] & (1 << bit_index)) != 0
    }

    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    pub fn read_bit_with_filter(
        &self,
        byte_data: u8,
        read_index: usize,
        bit_index: usize,
        filter: &'a dyn FixedLengthRangeFilter,
        rersult_index: usize,
        generator: &mut RowRangeSetGenerator,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<usize, BoltReaderError> {
        let value = (byte_data & (1 << bit_index)) != 0;
        let filter_res = filter.check_bool(value);
        generator.update(rersult_index, filter_res);
        if filter_res {
            result_bridge.append_non_null_bool_result(value)?;
        }

        Ok(read_index + 1)
    }

    pub fn read_nullable_with_filter(
        &mut self,
        to_read: RowRange,
        offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<bool, BoltReaderError> {
        let filter = self.filter.unwrap();
        let start = to_read.begin + offset - self.current_offset;
        let end = to_read.end + offset - self.current_offset;

        let validity = self.validity.as_ref().unwrap();
        let data_with_nulls = self.data_with_nulls.as_mut().unwrap();
        let nullable_selectivity = self.nullable_selectivity.as_mut().unwrap();

        for i in self.nullable_index..end {
            if validity[i] {
                let raw_data =
                    (self.data[self.non_null_index / 8] & 0x1 << (self.non_null_index % 8)) != 0;
                data_with_nulls[i] = Some(raw_data);
                nullable_selectivity[i] = filter.check_bool_with_validity(raw_data, validity[i]);

                self.non_null_index += 1;
            } else {
                nullable_selectivity[i] =
                    filter.check_bool_with_validity(bool::default(), validity[i]);
            }
        }
        let mut generator = RowRangeSetGenerator::new(result_row_range_set);

        for i in start..end {
            generator.update(i + self.current_offset - offset, nullable_selectivity[i]);
            if nullable_selectivity[i] {
                result_bridge.append_nullable_bool_result(data_with_nulls[i])?;
            }
        }

        self.nullable_index = end;
        Ok(end == self.num_values)
    }

    pub fn read_non_null_with_filter(
        &self,
        to_read: RowRange,
        offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<bool, BoltReaderError> {
        let filter = self.filter.unwrap();
        let start = to_read.begin + offset - self.current_offset;
        let end = to_read.end + offset - self.current_offset;

        let start_byte = start / 8;
        let start_bit = start % 8;
        let end_byte = end / 8;
        let end_bit = end % 8;

        let mut generator = RowRangeSetGenerator::new(result_row_range_set);
        let mut index = start;
        if start_byte < end_byte {
            for i in start_bit..8 {
                index = self.read_bit_with_filter(
                    self.data[start_byte],
                    index,
                    i,
                    filter,
                    index + self.current_offset - offset,
                    &mut generator,
                    result_bridge,
                )?;
            }
        }
        // This is to read 8 bits within a byte
        for i in start_byte + 1..end_byte {
            index = self.read_bit_with_filter(
                self.data[i],
                index,
                0,
                filter,
                index + self.current_offset - offset,
                &mut generator,
                result_bridge,
            )?;

            index = self.read_bit_with_filter(
                self.data[i],
                index,
                1,
                filter,
                index + self.current_offset - offset,
                &mut generator,
                result_bridge,
            )?;

            index = self.read_bit_with_filter(
                self.data[i],
                index,
                2,
                filter,
                index + self.current_offset - offset,
                &mut generator,
                result_bridge,
            )?;

            index = self.read_bit_with_filter(
                self.data[i],
                index,
                3,
                filter,
                index + self.current_offset - offset,
                &mut generator,
                result_bridge,
            )?;

            index = self.read_bit_with_filter(
                self.data[i],
                index,
                4,
                filter,
                index + self.current_offset - offset,
                &mut generator,
                result_bridge,
            )?;

            index = self.read_bit_with_filter(
                self.data[i],
                index,
                5,
                filter,
                index + self.current_offset - offset,
                &mut generator,
                result_bridge,
            )?;

            index = self.read_bit_with_filter(
                self.data[i],
                index,
                6,
                filter,
                index + self.current_offset - offset,
                &mut generator,
                result_bridge,
            )?;

            index = self.read_bit_with_filter(
                self.data[i],
                index,
                7,
                filter,
                index + self.current_offset - offset,
                &mut generator,
                result_bridge,
            )?;
        }

        for i in 0..end_bit {
            index = self.read_bit_with_filter(
                self.data[end_byte],
                index,
                i,
                filter,
                index + self.current_offset - offset,
                &mut generator,
                result_bridge,
            )?;
        }
        generator.finish(end + self.current_offset - offset);
        Ok(end == self.num_values)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;
    use std::rc::Rc;
    use std::string::String;

    use crate::bridge::boolean_bridge::BooleanBridge;
    use crate::filters::boolean_filter::BooleanFilter;
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
    use crate::metadata::page_header::read_page_header;
    use crate::metadata::parquet_metadata_thrift::Encoding;
    use crate::page_reader::data_page_v1::boolean_data_page_v1::BooleanDataPageReaderV1;
    use crate::page_reader::data_page_v1::data_page_base::{get_data_page_covered_range, DataPage};
    use crate::utils::byte_buffer_base::ByteBufferBase;
    use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
    use crate::utils::exceptions::BoltReaderError;
    use crate::utils::file_loader::{FileLoader, FileLoaderEnum};
    use crate::utils::file_streaming_byte_buffer::{FileStreamingBuffer, StreamingByteBuffer};
    use crate::utils::local_file_loader::LocalFileLoader;
    use crate::utils::rep_def_parser::RepDefParser;
    use crate::utils::row_range_set::{RowRange, RowRangeSet};
    use crate::utils::test_utils::test_utils::{
        verify_boolean_non_null_result, verify_boolean_nullable_result,
    };

    fn load_boolean_data_page<'a>(
        buf: &'a mut dyn ByteBufferBase,
        filter: Option<&'a (dyn FixedLengthRangeFilter + 'a)>,
        offset: usize,
    ) -> Result<BooleanDataPageReaderV1<'a>, BoltReaderError> {
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

        BooleanDataPageReaderV1::new(
            &page_header,
            buf,
            offset,
            1,
            validity.0,
            data_size as usize,
            filter,
            validity.1,
        )
    }

    #[test]
    fn test_create_boolean_data_page_v1() {
        let path = String::from("src/sample_files/boolean_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let res = load_boolean_data_page(&mut buf, None, 100);

        assert!(res.is_ok());
        let boolean_page_reader = res.unwrap();

        assert_eq!(
            boolean_page_reader.to_string(),
            "Boolean Data Page: has_null false, num_values 1000000, current_offset 100\n"
        );
    }

    #[test]
    fn test_create_boolean_data_page_v1_drop() {
        let path = String::from("src/sample_files/boolean_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_boolean_data_page(&mut buf, None, 100);

        assert!(res.is_ok());
        let boolean_zero_copy_page_reader = res.unwrap();

        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_boolean_data_page(&mut buf, None, 100);

        assert!(res.is_ok());
        let boolean_data_page_deep_copy = res.unwrap();

        assert_eq!(boolean_zero_copy_page_reader.is_zero_copied(), true);
        assert_eq!(boolean_data_page_deep_copy.is_zero_copied(), false);

        // Both deep copy and zero data page should be safely release at this point.
    }

    #[test]
    fn test_read_boolean_data_page() {
        let path = String::from("src/sample_files/boolean_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let res = load_boolean_data_page(&mut buf, None, 100);

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
            let mut boolean_bridge = BooleanBridge::new(false, capacity);
            let res = boolean_page_reader.read(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut boolean_bridge,
            );
            assert!(res.is_ok());
            verify_boolean_non_null_result(&result_row_range_set, &boolean_bridge, None);
            begin = end;
        }
    }

    #[test]
    fn test_read_boolean_data_page_with_filter() {
        let path = String::from("src/sample_files/boolean_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let filter = BooleanFilter::new(true, false);

        let res = load_boolean_data_page(&mut buf, Some(&filter), 100);

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
            let mut boolean_bridge = BooleanBridge::new(false, capacity);
            let res = boolean_page_reader.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut boolean_bridge,
            );
            assert!(res.is_ok());
            verify_boolean_non_null_result(&result_row_range_set, &boolean_bridge, Some(&filter));
            begin = end;
        }
    }

    #[test]
    fn test_read_boolean_data_page_in_streaming_buffer() {
        let path = String::from("src/sample_files/boolean_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let res = load_boolean_data_page(&mut buf, None, 100);

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
            let mut boolean_bridge = BooleanBridge::new(false, capacity);
            let res = boolean_page_reader.read(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut boolean_bridge,
            );
            assert!(res.is_ok());
            verify_boolean_non_null_result(&result_row_range_set, &boolean_bridge, None);
            begin = end;
        }
    }

    #[test]
    fn test_read_boolean_data_page_with_filter_in_streaming_buffer() {
        let path = String::from("src/sample_files/boolean_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let filter = BooleanFilter::new(true, false);

        let res = load_boolean_data_page(&mut buf, Some(&filter), 100);

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
            let mut boolean_bridge = BooleanBridge::new(false, capacity);
            let res = boolean_page_reader.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut boolean_bridge,
            );
            assert!(res.is_ok());
            verify_boolean_non_null_result(&result_row_range_set, &boolean_bridge, Some(&filter));
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_boolean_data_page() {
        let path = String::from("src/sample_files/boolean_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let res = load_boolean_data_page(&mut buf, None, 100);

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
            let mut boolean_bridge = BooleanBridge::new(true, capacity);
            let res = boolean_page_reader.read(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut boolean_bridge,
            );
            assert!(res.is_ok());
            verify_boolean_nullable_result(&result_row_range_set, &boolean_bridge, None);
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_boolean_data_page_with_filter() {
        let path = String::from("src/sample_files/boolean_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let filter = BooleanFilter::new(true, false);

        let res = load_boolean_data_page(&mut buf, Some(&filter), 100);

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
            let mut boolean_bridge = BooleanBridge::new(true, capacity);
            let res = boolean_page_reader.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut boolean_bridge,
            );
            assert!(res.is_ok());
            verify_boolean_nullable_result(&result_row_range_set, &boolean_bridge, Some(&filter));
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_boolean_data_page_in_streaming_buffer() {
        let path = String::from("src/sample_files/boolean_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let res = load_boolean_data_page(&mut buf, None, 100);

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
            let mut boolean_bridge = BooleanBridge::new(true, capacity);
            let res = boolean_page_reader.read(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut boolean_bridge,
            );
            assert!(res.is_ok());
            verify_boolean_nullable_result(&result_row_range_set, &boolean_bridge, None);
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_boolean_data_page_with_filter_in_streaming_buffer() {
        let path = String::from("src/sample_files/boolean_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let filter = BooleanFilter::new(true, false);

        let res = load_boolean_data_page(&mut buf, Some(&filter), 100);

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
            let mut boolean_bridge = BooleanBridge::new(true, capacity);
            let res = boolean_page_reader.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut boolean_bridge,
            );
            assert!(res.is_ok());
            verify_boolean_nullable_result(&result_row_range_set, &boolean_bridge, Some(&filter));
            begin = end;
        }
    }
}
