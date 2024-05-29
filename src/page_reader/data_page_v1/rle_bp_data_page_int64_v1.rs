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
use std::rc::Rc;

use crate::bridge::result_bridge::ResultBridge;
use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
use crate::metadata::parquet_metadata_thrift;
use crate::metadata::parquet_metadata_thrift::PageHeader;
use crate::page_reader::data_page_v1::data_page_base::DataPage;
use crate::page_reader::dictionary_page::dictionary_page_base::{
    DictionaryPageEnum, DictionaryPageNew,
};
use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::encoding::rle_bp::RleBpDecoder;
use crate::utils::exceptions::BoltReaderError;
use crate::utils::row_range_set::{RowRange, RowRangeSet, RowRangeSetGenerator};

// This RLE/BP Decoder is currently based on Rust Arrow2 Parquet.
// TODO: Implement a native RLE/BP Decoder, supporting filter push down.

// Currently, we display 10 pieces of data only
// todo: Create config module to handle the default const values.
const DEFAULT_DISPLAY_NUMBER: usize = 10;

/// The Parquet Page Reader V1 Struct
/// current_offset: the offset in the whole column
pub struct RleBpDataPageReaderInt64V1<'a> {
    has_null: bool,
    num_values: usize,
    current_offset: usize,
    type_size: usize,
    non_null_index: usize,
    nullable_index: usize,
    #[allow(dead_code)]
    bit_width: u8,
    filter: Option<&'a dyn FixedLengthRangeFilter>,
    validity: Option<Vec<bool>>,
    data: Vec<u32>,
    data_with_nulls: Option<Vec<Option<i64>>>,
    nullable_selectivity: Option<Vec<bool>>,
    dictionary_page: Rc<DictionaryPageEnum>,
}

#[allow(dead_code)]
impl<'a> std::fmt::Display for RleBpDataPageReaderInt64V1<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let data_str = self
            .data
            .iter()
            .take(DEFAULT_DISPLAY_NUMBER)
            .map(u32::to_string)
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
            "RLE/BP Data Page: has_null {}, num_values {}, current_offset {}\nData: {} ...\nValidity: {} ...",
             self.has_null, self.num_values, self.current_offset, data_str, validity_str
        )
    }
}

impl<'a> DataPage for RleBpDataPageReaderInt64V1<'a> {
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
        false
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
                    data_with_nulls[i] = Some(
                        self.dictionary_page
                            .find_int64(self.data[self.non_null_index] as usize),
                    );
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
            for i in start..end {
                result_bridge.append_non_null_int64_result(
                    self.dictionary_page.find_int64(self.data[i] as usize),
                )?;
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

#[allow(clippy::too_many_arguments)]
impl<'a> RleBpDataPageReaderInt64V1<'a> {
    pub fn new(
        page_header: &PageHeader,
        buffer: &mut dyn ByteBufferBase,
        current_offset: usize,
        type_size: usize,
        has_null: bool,
        mut data_size: usize,
        filter: Option<&'a (dyn FixedLengthRangeFilter + 'a)>,
        validity: Option<Vec<bool>>,
        dictionary_page: Rc<DictionaryPageEnum>,
    ) -> Result<RleBpDataPageReaderInt64V1<'a>, BoltReaderError> {
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

        if encoding != parquet_metadata_thrift::Encoding::RLE_DICTIONARY
            && encoding != parquet_metadata_thrift::Encoding::PLAIN_DICTIONARY
        {
            return Err(BoltReaderError::FixedLengthDataPageError(String::from(
                "RLE/BP Data Page Encoding should be RLE/BP encoded",
            )));
        }

        let bit_width = buffer.read_u8().unwrap();
        data_size -= 1;

        // The whole RLE/BP encoded data is decoded directly for now.
        // TODO 1: Optimize this logic and reduce the deecoding data size
        // TODO 2: Support filter push down to decoding
        let mut data: Vec<u32> = Vec::with_capacity(num_values);
        let mut bytes_read = 0;
        while bytes_read < data_size {
            let rpos = buffer.get_rpos();
            data.append(&mut RleBpDecoder::decode(buffer, bit_width as usize).unwrap());
            bytes_read += buffer.get_rpos() - rpos;
        }

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

        Ok(RleBpDataPageReaderInt64V1 {
            has_null,
            num_values,
            current_offset,
            type_size,
            non_null_index: 0,
            nullable_index: 0,
            bit_width,
            filter,
            validity,
            data,
            data_with_nulls,
            nullable_selectivity,
            dictionary_page,
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

        let mut generator = RowRangeSetGenerator::new(result_row_range_set);

        for i in start..end {
            let index = self.data[i] as usize;
            let filter_res = self.dictionary_page.validate(index);
            generator.update(i + self.current_offset - offset, filter_res);
            if filter_res {
                result_bridge
                    .append_non_null_int64_result(self.dictionary_page.find_int64(index))?;
            }
        }
        generator.finish(end + self.current_offset - offset);

        Ok(end == self.num_values)
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
        let filter = self.filter.as_ref().unwrap();

        let validity = self.validity.as_ref().unwrap();
        let data_with_nulls = self.data_with_nulls.as_mut().unwrap();
        let nullable_selectivity = self.nullable_selectivity.as_mut().unwrap();

        for i in self.nullable_index..end {
            if validity[i] {
                let index = self.data[self.non_null_index] as usize;
                data_with_nulls[i] = Some(self.dictionary_page.find_int64(index));
                nullable_selectivity[i] = self.dictionary_page.validate(index);

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

        Ok(end == self.num_values)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;
    use std::mem;
    use std::rc::Rc;

    use crate::bridge::int64_bridge::Int64Bridge;
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
    use crate::filters::integer_range_filter::IntegerRangeFilter;
    use crate::metadata::page_header::read_page_header;
    use crate::metadata::parquet_metadata_thrift::Encoding;
    use crate::page_reader::data_page_v1::data_page_base::{get_data_page_covered_range, DataPage};
    use crate::page_reader::data_page_v1::rle_bp_data_page_int64_v1::RleBpDataPageReaderInt64V1;
    use crate::page_reader::dictionary_page::dictionary_page_base::DictionaryPageEnum;
    use crate::page_reader::dictionary_page::dictionary_page_int64::DictionaryPageInt64;
    use crate::page_reader::dictionary_page::dictionary_page_int64_with_filters::DictionaryPageWithFilterInt64;
    use crate::utils::byte_buffer_base::ByteBufferBase;
    use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
    use crate::utils::exceptions::BoltReaderError;
    use crate::utils::file_loader::{FileLoader, FileLoaderEnum};
    use crate::utils::file_streaming_byte_buffer::{FileStreamingBuffer, StreamingByteBuffer};
    use crate::utils::local_file_loader::LocalFileLoader;
    use crate::utils::rep_def_parser::RepDefParser;
    use crate::utils::row_range_set::{RowRange, RowRangeSet};
    use crate::utils::test_utils::test_utils::{
        verify_rle_bp_int64_non_null_result, verify_rle_bp_int64_nullable_result,
    };

    const STEAMING_BUFFER_SIZE: usize = 1 << 8;

    fn load_dictionary_page(buf: &mut dyn ByteBufferBase) -> Rc<DictionaryPageEnum> {
        let page_header = read_page_header(buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();
        let dictionary_page = DictionaryPageInt64::new(&page_header, buf, mem::size_of::<i64>());
        assert!(dictionary_page.is_ok());
        Rc::from(DictionaryPageEnum::DictionaryPageInt64(
            dictionary_page.unwrap(),
        ))
    }

    fn load_dictionary_page_with_filter(
        buf: &mut dyn ByteBufferBase,
        filter: &dyn FixedLengthRangeFilter,
    ) -> Rc<DictionaryPageEnum> {
        let page_header = read_page_header(buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();
        let dictionary_page =
            DictionaryPageWithFilterInt64::new(&page_header, buf, mem::size_of::<i64>(), filter);
        assert!(dictionary_page.is_ok());
        Rc::from(DictionaryPageEnum::DictionaryPageWithFilterInt64(
            dictionary_page.unwrap(),
        ))
    }

    fn load_rle_bp_page<'a>(
        buf: &'a mut dyn ByteBufferBase,
        dictionary: Rc<DictionaryPageEnum>,
        filter: Option<&'a (dyn FixedLengthRangeFilter + 'a)>,
        offset: usize,
    ) -> Result<RleBpDataPageReaderInt64V1<'a>, BoltReaderError> {
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

        let data_page = RleBpDataPageReaderInt64V1::new(
            &page_header,
            buf,
            offset,
            8,
            validity.0,
            data_size as usize,
            filter,
            validity.1,
            dictionary,
        );
        data_page
    }

    #[test]
    fn test_create_rle_bp_data_page_v1() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let dictionary_page = load_dictionary_page(&mut buf);
        let data_page = load_rle_bp_page(&mut buf, Rc::clone(&dictionary_page), None, 100);
        assert!(data_page.is_ok());
        let data_page = data_page.unwrap();

        assert_eq!(data_page.to_string(), "RLE/BP Data Page: has_null false, num_values 762880, current_offset 100\nData: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ...\nValidity: true, true, true, true, true, true, true, true, true, true ...\n");
    }

    #[test]
    fn test_create_nullable_rle_bp_data_page_v1() {
        let path = String::from("src/sample_files/rle_bp_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let dictionary_page = load_dictionary_page(&mut buf);
        let data_page = load_rle_bp_page(&mut buf, Rc::clone(&dictionary_page), None, 100);
        assert!(data_page.is_ok());
        let data_page = data_page.unwrap();

        assert_eq!(data_page.to_string(), "RLE/BP Data Page: has_null true, num_values 1000000, current_offset 100\nData: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ...\nValidity: false, true, true, true, true, false, true, true, true, true ...\n");
    }

    #[test]
    fn test_read_non_null_data_page() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let dictionary_page = load_dictionary_page(&mut buf);
        let data_page = load_rle_bp_page(&mut buf, Rc::clone(&dictionary_page), None, 100);
        assert!(data_page.is_ok());
        let mut data_page = data_page.unwrap();

        let num_values = data_page.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = data_page.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                data_page.get_data_page_offset(),
                data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(false, capacity);
            let res = data_page.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
            assert!(res.is_ok());
            verify_rle_bp_int64_non_null_result(&result_row_range_set, &raw_bridge, None);
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_data_page() {
        let path = String::from("src/sample_files/rle_bp_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let dictionary_page = load_dictionary_page(&mut buf);
        let data_page = load_rle_bp_page(&mut buf, Rc::clone(&dictionary_page), None, 100);
        assert!(data_page.is_ok());
        let mut data_page = data_page.unwrap();

        let num_values = data_page.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = data_page.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                data_page.get_data_page_offset(),
                data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(true, capacity);
            let res = data_page.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
            assert!(res.is_ok());
            verify_rle_bp_int64_nullable_result(&result_row_range_set, &raw_bridge, None);
            begin = end;
        }
    }

    #[test]
    fn test_read_non_null_data_page_with_filter() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let filter = IntegerRangeFilter::new(0, 100, false);
        let dictionary_page = load_dictionary_page_with_filter(&mut buf, &filter);
        let data_page = load_rle_bp_page(&mut buf, Rc::clone(&dictionary_page), Some(&filter), 100);
        assert!(data_page.is_ok());
        let mut data_page = data_page.unwrap();

        let num_values = data_page.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = data_page.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                data_page.get_data_page_offset(),
                data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(false, capacity);
            let res = data_page.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());
            verify_rle_bp_int64_non_null_result(&result_row_range_set, &raw_bridge, Some(&filter));
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_data_page_with_non_null_filter() {
        let path = String::from("src/sample_files/rle_bp_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let non_null_filter = IntegerRangeFilter::new(0, 100, false);
        let dictionary_page = load_dictionary_page_with_filter(&mut buf, &non_null_filter);
        let data_page = load_rle_bp_page(
            &mut buf,
            Rc::clone(&dictionary_page),
            Some(&non_null_filter),
            100,
        );
        assert!(data_page.is_ok());
        let mut data_page = data_page.unwrap();

        let num_values = data_page.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = data_page.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                data_page.get_data_page_offset(),
                data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(true, capacity);
            let res = data_page.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());
            verify_rle_bp_int64_nullable_result(
                &result_row_range_set,
                &raw_bridge,
                Some(&non_null_filter),
            );
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_data_page_with_nullable_filter() {
        let path = String::from("src/sample_files/rle_bp_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let nullable_filter = IntegerRangeFilter::new(0, 100, true);
        let dictionary_page = load_dictionary_page_with_filter(&mut buf, &nullable_filter);
        let data_page = load_rle_bp_page(
            &mut buf,
            Rc::clone(&dictionary_page),
            Some(&nullable_filter),
            100,
        );
        assert!(data_page.is_ok());
        let mut data_page = data_page.unwrap();

        let num_values = data_page.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = data_page.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                data_page.get_data_page_offset(),
                data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(true, capacity);
            let res = data_page.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());
            verify_rle_bp_int64_nullable_result(
                &result_row_range_set,
                &raw_bridge,
                Some(&nullable_filter),
            );
            begin = end;
        }
    }

    #[test]
    fn test_read_non_null_data_page_in_streaming_buffer() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(
            file.clone(),
            4,
            file.get_file_size() - 4,
            STEAMING_BUFFER_SIZE,
        );
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let dictionary_page = load_dictionary_page(&mut buf);
        let data_page = load_rle_bp_page(&mut buf, Rc::clone(&dictionary_page), None, 100);
        assert!(data_page.is_ok());
        let mut data_page = data_page.unwrap();

        let num_values = data_page.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = data_page.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                data_page.get_data_page_offset(),
                data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(false, capacity);
            let res = data_page.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
            assert!(res.is_ok());
            verify_rle_bp_int64_non_null_result(&result_row_range_set, &raw_bridge, None);
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_data_page_in_streaming_buffer() {
        let path = String::from("src/sample_files/rle_bp_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(
            file.clone(),
            4,
            file.get_file_size() - 4,
            STEAMING_BUFFER_SIZE,
        );
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let dictionary_page = load_dictionary_page(&mut buf);
        let data_page = load_rle_bp_page(&mut buf, Rc::clone(&dictionary_page), None, 100);
        assert!(data_page.is_ok());
        let mut data_page = data_page.unwrap();

        let num_values = data_page.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = data_page.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                data_page.get_data_page_offset(),
                data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(true, capacity);
            let res = data_page.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
            assert!(res.is_ok());
            verify_rle_bp_int64_nullable_result(&result_row_range_set, &raw_bridge, None);
            begin = end;
        }
    }

    #[test]
    fn test_read_non_null_data_page_with_filter_in_streaming_buffer() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(
            file.clone(),
            4,
            file.get_file_size() - 4,
            STEAMING_BUFFER_SIZE,
        );
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let filter = IntegerRangeFilter::new(0, 100, false);
        let dictionary_page = load_dictionary_page_with_filter(&mut buf, &filter);
        let data_page = load_rle_bp_page(&mut buf, Rc::clone(&dictionary_page), Some(&filter), 100);
        assert!(data_page.is_ok());
        let mut data_page = data_page.unwrap();

        let num_values = data_page.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = data_page.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                data_page.get_data_page_offset(),
                data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(false, capacity);
            let res = data_page.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());
            verify_rle_bp_int64_non_null_result(&result_row_range_set, &raw_bridge, Some(&filter));
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_data_page_with_non_null_filter_in_streaming_buffer() {
        let path = String::from("src/sample_files/rle_bp_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(
            file.clone(),
            4,
            file.get_file_size() - 4,
            STEAMING_BUFFER_SIZE,
        );
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let non_null_filter = IntegerRangeFilter::new(0, 100, false);
        let dictionary_page = load_dictionary_page_with_filter(&mut buf, &non_null_filter);
        let data_page = load_rle_bp_page(
            &mut buf,
            Rc::clone(&dictionary_page),
            Some(&non_null_filter),
            100,
        );
        assert!(data_page.is_ok());
        let mut data_page = data_page.unwrap();

        let num_values = data_page.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = data_page.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                data_page.get_data_page_offset(),
                data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(true, capacity);
            let res = data_page.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());
            verify_rle_bp_int64_nullable_result(
                &result_row_range_set,
                &raw_bridge,
                Some(&non_null_filter),
            );
            begin = end;
        }
    }

    #[test]
    fn test_read_nullable_data_page_with_nullable_filter_in_streaming_buffer() {
        let path = String::from("src/sample_files/rle_bp_bigint_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(
            file.clone(),
            4,
            file.get_file_size() - 4,
            STEAMING_BUFFER_SIZE,
        );
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let nullable_filter = IntegerRangeFilter::new(0, 100, true);
        let dictionary_page = load_dictionary_page_with_filter(&mut buf, &nullable_filter);
        let data_page = load_rle_bp_page(
            &mut buf,
            Rc::clone(&dictionary_page),
            Some(&nullable_filter),
            100,
        );
        assert!(data_page.is_ok());
        let mut data_page = data_page.unwrap();

        let num_values = data_page.get_data_page_num_values();
        let mut begin = 0;
        let step = 10000;

        while begin < num_values {
            let end = min(begin + step, num_values);
            let to_read = RowRange::new(begin, end);
            let offset = data_page.get_data_page_offset();
            let capacity = step;

            let to_read = get_data_page_covered_range(
                data_page.get_data_page_offset(),
                data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
                offset,
                &to_read,
            );
            assert!(to_read.is_ok());
            let to_read = to_read.unwrap();
            assert!(to_read.is_some());
            let to_read = to_read.unwrap();

            let mut result_row_range_set = RowRangeSet::new(offset);
            let mut raw_bridge = Int64Bridge::new(true, capacity);
            let res = data_page.read_with_filter(
                to_read,
                offset,
                &mut result_row_range_set,
                &mut raw_bridge,
            );
            assert!(res.is_ok());
            verify_rle_bp_int64_nullable_result(
                &result_row_range_set,
                &raw_bridge,
                Some(&nullable_filter),
            );
            begin = end;
        }
    }

    #[test]
    fn test_read_non_null_data_page_random() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");

        for start in 0..60 {
            for j in 0..5 {
                let step = 1 << j;

                let mut begin = start;
                let mut end = begin + step;
                let res = LocalFileLoader::new(&path);
                assert!(res.is_ok());
                let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
                let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
                assert!(res.is_ok());
                let mut buf = res.unwrap();

                let dictionary_page = load_dictionary_page(&mut buf);
                let data_page = load_rle_bp_page(&mut buf, Rc::clone(&dictionary_page), None, 100);
                assert!(data_page.is_ok());
                let mut data_page = data_page.unwrap();

                while begin < 1200 {
                    let to_read = RowRange::new(begin, end);
                    let offset = 100;
                    let capacity = 1200;

                    let to_read = get_data_page_covered_range(
                        data_page.get_data_page_offset(),
                        data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
                        offset,
                        &to_read,
                    );
                    assert!(to_read.is_ok());
                    let to_read = to_read.unwrap();
                    assert!(to_read.is_some());
                    let to_read = to_read.unwrap();

                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = Int64Bridge::new(false, capacity);
                    let res =
                        data_page.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                    assert!(res.is_ok());

                    verify_rle_bp_int64_non_null_result(&result_row_range_set, &raw_bridge, None);
                    begin = end;
                    end = min(end + step, 1200);
                }
            }
        }
    }

    #[test]
    fn test_read_nullable_data_page_random() {
        let path = String::from("src/sample_files/rle_bp_bigint_column_with_nulls.parquet");

        for start in 0..60 {
            for j in 0..5 {
                let step = 1 << j;

                let mut begin = start;
                let mut end = begin + step;
                let res = LocalFileLoader::new(&path);
                assert!(res.is_ok());
                let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
                let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
                assert!(res.is_ok());
                let mut buf = res.unwrap();

                let dictionary_page = load_dictionary_page(&mut buf);
                let data_page = load_rle_bp_page(&mut buf, Rc::clone(&dictionary_page), None, 100);
                assert!(data_page.is_ok());
                let mut data_page = data_page.unwrap();

                while begin < 1200 {
                    let to_read = RowRange::new(begin, end);
                    let offset = 100;
                    let capacity = 1200;

                    let to_read = get_data_page_covered_range(
                        data_page.get_data_page_offset(),
                        data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
                        offset,
                        &to_read,
                    );
                    assert!(to_read.is_ok());
                    let to_read = to_read.unwrap();
                    assert!(to_read.is_some());
                    let to_read = to_read.unwrap();

                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = Int64Bridge::new(true, capacity);
                    let res =
                        data_page.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                    assert!(res.is_ok());

                    verify_rle_bp_int64_nullable_result(&result_row_range_set, &raw_bridge, None);
                    begin = end;
                    end = min(end + step, 1200);
                }
            }
        }
    }

    #[test]
    fn test_read_outside_of_data_page() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();

        let dictionary_page = load_dictionary_page(&mut buf);
        let data_page = load_rle_bp_page(&mut buf, Rc::clone(&dictionary_page), None, 100);
        assert!(data_page.is_ok());
        let mut data_page = data_page.unwrap();

        let num_values = data_page.get_data_page_num_values();

        let to_read = RowRange::new(num_values - 100, num_values + 100);
        let offset = data_page.get_data_page_offset();
        let capacity = 1000;

        let to_read = get_data_page_covered_range(
            data_page.get_data_page_offset(),
            data_page.get_data_page_offset() + data_page.get_data_page_num_values(),
            offset,
            &to_read,
        );
        assert!(to_read.is_ok());
        let to_read = to_read.unwrap();
        assert!(to_read.is_some());
        let to_read = to_read.unwrap();

        let mut result_row_range_set = RowRangeSet::new(offset);
        let mut raw_bridge = Int64Bridge::new(false, capacity);
        let res = data_page.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
        assert!(res.is_ok());
        verify_rle_bp_int64_non_null_result(&result_row_range_set, &raw_bridge, None);
    }
}
