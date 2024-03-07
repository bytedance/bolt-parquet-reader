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
use crate::column_reader::column_reader_base::ColumnReader;
use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
use crate::metadata::page_header::read_page_header;
use crate::metadata::parquet_metadata_thrift::{ColumnMetaData, Encoding, PageHeader};
use crate::page_reader::data_page_v1::data_page_base::{
    get_data_page_covered_range, get_data_page_remaining_range, DataPage,
};
use crate::page_reader::data_page_v1::fixed_length_plain_data_page_v1::FixedLengthPlainDataPageReaderV1;
use crate::page_reader::dictionary_page::fixed_length_dictionary_page::FixedLengthDictionary;
use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::exceptions::BoltReaderError;
use crate::utils::rep_def_parser::RepDefParser;
use crate::utils::row_range_set::{RowRange, RowRangeSet};

pub struct FixedLengthColumnReader<'a, T: 'static + std::marker::Copy> {
    num_values: usize,
    reading_index: usize,
    data_page_offset: usize,
    bytes_read: usize,
    type_size: usize,
    max_rep: u32,
    max_def: u32,
    column_size: usize,
    buffer: &'a mut dyn ByteBufferBase,
    dictionary_page: Option<FixedLengthDictionary<T>>,
    current_data_page_header: Option<PageHeader>,
    current_data_page: Option<FixedLengthPlainDataPageReaderV1<'a, T>>,
    filter: Option<&'a dyn FixedLengthRangeFilter>,
}

#[allow(dead_code)]
impl<'a, T: 'static + std::marker::Copy> std::fmt::Display for FixedLengthColumnReader<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Fixed Length Column: num_values {}, has dictionary {}",
            self.num_values,
            self.dictionary_page.is_some()
        )
    }
}

impl<'a, T: 'static + std::marker::Copy> ColumnReader<T> for FixedLengthColumnReader<'a, T> {
    fn get_column_num_values(&self) -> usize {
        self.num_values
    }

    fn get_data_type_size(&self) -> usize {
        self.type_size
    }

    fn read(
        &mut self,
        mut to_read: RowRange,
        to_read_offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn Bridge<T>,
    ) -> Result<(), BoltReaderError> {
        if self.dictionary_page.is_some() {
            return Err(BoltReaderError::NotYetImplementedError(String::from(
                "Dictionary encoding not yet implemented",
            )));
        }

        if to_read.get_length() == 0 {
            return Ok(());
        }

        if unlikely(to_read.begin + to_read_offset < self.reading_index) {
            return Err(BoltReaderError::FixedLengthColumnReaderError(format!(
                "Fixed Length Column Reader: Cannot read backward. Current reading index: {}, attemp to read: {}", self.reading_index, to_read.begin + to_read_offset
            )));
        }

        let mut finished = false;
        while !finished {
            if self.current_data_page.is_none() {
                self.prepare_data_page()?;
            }

            let page_header = self.current_data_page_header.as_ref().unwrap();
            let data_page_header = page_header.data_page_header.as_ref().unwrap();

            let covered_range = get_data_page_covered_range(
                self.data_page_offset,
                self.data_page_offset + data_page_header.num_values as usize,
                to_read_offset,
                &to_read,
            )?;

            let remaining_range = get_data_page_remaining_range(
                self.data_page_offset,
                self.data_page_offset + data_page_header.num_values as usize,
                to_read_offset,
                &to_read,
            )?;

            let mut need_next_page = true;
            if let Some(covered_range) = covered_range {
                if self.current_data_page.is_none() {
                    self.load_data_page()?;
                }
                let data_page = self.current_data_page.as_mut().unwrap();

                self.reading_index = covered_range.end + to_read_offset;
                need_next_page = data_page.read(
                    covered_range,
                    to_read_offset,
                    result_row_range_set,
                    result_bridge,
                )?;
            }

            finished = remaining_range.is_none();

            if let Some(remaining_range) = remaining_range {
                to_read = remaining_range;
            }

            if need_next_page && !self.get_next_data_page()? && !finished {
                return Err(BoltReaderError::FixedLengthColumnReaderError(String::from(
                    "Reading outside of the column",
                )));
            }
        }

        Ok(())
    }

    fn read_with_filter(
        &mut self,
        mut to_read: RowRange,
        to_read_offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn Bridge<T>,
    ) -> Result<(), BoltReaderError> {
        if self.dictionary_page.is_some() {
            return Err(BoltReaderError::NotYetImplementedError(String::from(
                "Dictionary encoding not yet implemented",
            )));
        }

        if self.filter.is_none() {
            return Err(BoltReaderError::FixedLengthColumnReaderError(String::from(
                "Filter doesn't exist for this column",
            )));
        }

        if to_read.get_length() == 0 {
            return Ok(());
        }

        if unlikely(to_read.begin + to_read_offset < self.reading_index) {
            return Err(BoltReaderError::FixedLengthColumnReaderError(format!(
                "Fixed Length Column Reader: Cannot read backward. Current reading index: {}, attemp to read: {}", self.reading_index, to_read.begin + to_read_offset
            )));
        }

        let mut finished = false;
        while !finished {
            if self.current_data_page.is_none() {
                self.prepare_data_page()?;
            }

            let page_header = self.current_data_page_header.as_ref().unwrap();
            let data_page_header = page_header.data_page_header.as_ref().unwrap();

            let covered_range = get_data_page_covered_range(
                self.data_page_offset,
                self.data_page_offset + data_page_header.num_values as usize,
                to_read_offset,
                &to_read,
            )?;

            let remaining_range = get_data_page_remaining_range(
                self.data_page_offset,
                self.data_page_offset + data_page_header.num_values as usize,
                to_read_offset,
                &to_read,
            )?;

            let mut need_next_page = true;

            if let Some(covered_range) = covered_range {
                if self.current_data_page.is_none() {
                    self.load_data_page()?;
                }
                let data_page = self.current_data_page.as_mut().unwrap();

                self.reading_index = covered_range.end + to_read_offset;
                need_next_page = data_page.read_with_filter(
                    covered_range,
                    to_read_offset,
                    result_row_range_set,
                    result_bridge,
                )?;
            }

            finished = remaining_range.is_none();

            if let Some(remaining_range) = remaining_range {
                to_read = remaining_range;
            }

            if need_next_page && !self.get_next_data_page()? && !finished {
                return Err(BoltReaderError::FixedLengthColumnReaderError(String::from(
                    "Reading outside of the column",
                )));
            }
        }

        Ok(())
    }
}

impl<'a, T: 'static + std::marker::Copy> FixedLengthColumnReader<'a, T> {
    pub fn new(
        column_meta_data: &ColumnMetaData,
        type_size: usize,
        max_rep: u32,
        max_def: u32,
        buffer: &'a mut dyn ByteBufferBase,
        filter: Option<&'a dyn FixedLengthRangeFilter>,
    ) -> Result<FixedLengthColumnReader<'a, T>, BoltReaderError> {
        let mut dictionary_page: Option<FixedLengthDictionary<T>> = None;
        match column_meta_data.dictionary_page_offset {
            None => None,

            Some(_) => {
                {
                    let dictionary_page_header = read_page_header(buffer)?;

                    dictionary_page = Some(FixedLengthDictionary::new(
                        &dictionary_page_header,
                        buffer,
                        type_size,
                    )?)
                };
                Some(())
            }
        };

        Ok(FixedLengthColumnReader {
            num_values: column_meta_data.num_values as usize,
            reading_index: 0,
            data_page_offset: 0,
            bytes_read: 0,
            type_size,
            max_rep,
            max_def,
            column_size: column_meta_data.total_compressed_size as usize,
            buffer,
            dictionary_page,
            current_data_page_header: Option::None,
            current_data_page: Option::None,
            filter,
        })
    }

    pub fn prepare_data_page(&mut self) -> Result<(), BoltReaderError> {
        let rpos = self.buffer.get_rpos();
        self.current_data_page_header = Some(read_page_header(self.buffer)?);
        self.bytes_read += self.buffer.get_rpos() - rpos;

        Ok(())
    }

    pub fn load_data_page(&mut self) -> Result<(), BoltReaderError> {
        let page_header = self.current_data_page_header.as_ref().unwrap();
        let data_page_header = page_header.data_page_header.as_ref().unwrap();

        let rep_rle_bp = data_page_header.repetition_level_encoding == Encoding::RLE
            || data_page_header.repetition_level_encoding == Encoding::BIT_PACKED;

        let def_rle_bp = data_page_header.definition_level_encoding == Encoding::RLE
            || data_page_header.definition_level_encoding == Encoding::BIT_PACKED;

        // TODO: Add decompression support
        if page_header.crc.is_none() {
            // Uncompressed
            let rpos = self.buffer.get_rpos();
            let validity = RepDefParser::parse_rep_def(
                self.buffer,
                data_page_header.num_values as usize,
                self.max_rep,
                rep_rle_bp,
                self.max_def,
                def_rle_bp,
            )?;

            let data_size =
                page_header.uncompressed_page_size - (self.buffer.get_rpos() - rpos) as i32;
            self.current_data_page = Some(FixedLengthPlainDataPageReaderV1::new(
                page_header,
                self.buffer,
                self.data_page_offset,
                self.type_size,
                validity.0,
                data_size as usize,
                self.filter,
                validity.1,
            )?);

            // For uncompressed conditions, we move the index back for zero copy.
            self.buffer.set_rpos(rpos);
        } else {
            return Err(BoltReaderError::NotYetImplementedError(String::from(
                "Decompression not yet implemented",
            )));
        }

        Ok(())
    }

    pub fn get_next_data_page(&mut self) -> Result<bool, BoltReaderError> {
        if unlikely(self.current_data_page_header.is_none()) {
            return Err(BoltReaderError::FixedLengthColumnReaderError(String::from(
                "Current Data Page Header is not ready",
            )));
        }

        let old_page_header = self.current_data_page_header.as_ref().unwrap();
        let old_data_page_header = old_page_header.data_page_header.as_ref().unwrap();
        self.bytes_read += old_page_header.compressed_page_size as usize;
        if unlikely(self.bytes_read > self.column_size) {
            return Err(BoltReaderError::FixedLengthColumnReaderError(format!(
                "Column read bytes {} is larger than the column size {}.",
                self.bytes_read, self.column_size
            )));
        } else if self.bytes_read == self.column_size {
            return Ok(false);
        }

        self.buffer
            .set_rpos(self.buffer.get_rpos() + old_page_header.compressed_page_size as usize);

        self.data_page_offset += old_data_page_header.num_values as usize;

        self.current_data_page = Option::None;
        self.current_data_page_header = Option::None;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::{max, min};

    use rand::{thread_rng, Rng};

    use crate::bridge::bridge_base::Bridge;
    use crate::bridge::raw_bridge::RawBridge;
    use crate::column_reader::column_reader_base::ColumnReader;
    use crate::column_reader::fixed_length_column_reader::FixedLengthColumnReader;
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
    use crate::filters::float_point_range_filter::FloatPointRangeFilter;
    use crate::filters::integer_range_filter::IntegerRangeFilter;
    use crate::metadata::parquet_footer::FileMetaDataLoader;
    use crate::metadata::parquet_metadata_thrift::FileMetaData;
    use crate::utils::byte_buffer_base::ByteBufferBase;
    use crate::utils::direct_byte_buffer::DirectByteBuffer;
    use crate::utils::exceptions::BoltReaderError;
    use crate::utils::file_loader::LoadFile;
    use crate::utils::file_streaming_byte_buffer::{FileStreamingBuffer, StreamingByteBuffer};
    use crate::utils::local_file_loader::LocalFileLoader;
    use crate::utils::row_range_set::{RowRange, RowRangeSet};

    fn get_random_number_in_range(shift: u32) -> u32 {
        let mut rng = thread_rng();
        let random_value: u32 = rng.gen();
        random_value % (1 << shift)
    }

    fn load_file_metadata(path: &String) -> FileMetaData {
        let metadata_loader = FileMetaDataLoader::new(&String::from(path), 1 << 20);
        assert!(metadata_loader.is_ok());
        let mut metadata_loader = metadata_loader.unwrap();
        let res = metadata_loader.load_parquet_footer();
        assert!(res.is_ok());
        res.unwrap()
    }

    fn load_column_to_direct_buffer(path: &String) -> DirectByteBuffer {
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();

        let res = file.load_file_to_buffer(4, file.get_file_size() - 4);
        assert!(res.is_ok());
        res.unwrap()
    }

    fn load_column_to_streaming_buffer<'a>(
        file: &'a (dyn LoadFile + 'a),
        buffer_size: usize,
    ) -> StreamingByteBuffer {
        let res = StreamingByteBuffer::from_file(file, 4, file.get_file_size() - 4, buffer_size);
        assert!(res.is_ok());
        res.unwrap()
    }

    fn load_column_reader<'a, T: std::marker::Copy>(
        path: &'a String,
        buffer: &'a mut dyn ByteBufferBase,
        filter: Option<&'a dyn FixedLengthRangeFilter>,
    ) -> Result<FixedLengthColumnReader<'a, T>, BoltReaderError> {
        let footer = load_file_metadata(&path);

        let column_meta_data = &footer.row_groups[0].columns[0].meta_data.as_ref().unwrap();
        FixedLengthColumnReader::new(column_meta_data, 8, 0, 1, buffer, filter)
    }

    #[test]
    fn test_loading_dictionary_page() {
        let path = String::from("src/sample_files/lineitem_dictionary.parquet");
        let footer = load_file_metadata(&path);

        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();

        let res = file.load_file_to_buffer(4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buffer = res.unwrap();
        let column_meta_data = &footer.row_groups[0].columns[0].meta_data.as_ref().unwrap();

        let column_reader: Result<FixedLengthColumnReader<i64>, BoltReaderError> =
            FixedLengthColumnReader::new(column_meta_data, 8, 0, 1, &mut buffer, None);

        assert!(column_reader.is_ok());

        let column_reader = column_reader.unwrap();
        let dictionary = column_reader.dictionary_page;
        assert!(dictionary.is_some());
    }

    #[test]
    fn test_loading_dictionary_page_streaming_buffer() {
        let path = String::from("src/sample_files/lineitem_dictionary.parquet");
        let footer = load_file_metadata(&path);

        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        let mut buffer = load_column_to_streaming_buffer(&file, 1 << 16);

        let column_meta_data = &footer.row_groups[0].columns[0].meta_data.as_ref().unwrap();

        let column_reader: Result<FixedLengthColumnReader<i64>, BoltReaderError> =
            FixedLengthColumnReader::new(column_meta_data, 8, 0, 1, &mut buffer, None);

        assert!(column_reader.is_ok());

        let column_reader = column_reader.unwrap();
        let dictionary = column_reader.dictionary_page;
        assert!(dictionary.is_some());
    }

    #[test]
    fn test_reading_plain_bigint_column_from_beginning() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");

        let num_values = 1000000;
        let num_tests = 100;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let mut buffer = load_column_to_direct_buffer(&path);
            let column_reader: Result<FixedLengthColumnReader<i64>, _> =
                load_column_reader(&path, &mut buffer, Option::None);
            assert!(column_reader.is_ok());
            let mut column_reader = column_reader.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = RawBridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    for i in begin..end {
                        assert_eq!(
                            raw_bridge
                                .get_validity_and_value(offset, i as usize, &result_row_range_set)
                                .unwrap(),
                            (true, i as i64)
                        );
                    }
                }
                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_middle() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");

        let num_values = 1000000;
        let num_tests = 10;

        for num_test in 0..num_tests {
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + num_test;
                let mut buffer = load_column_to_direct_buffer(&path);
                let column_reader: Result<FixedLengthColumnReader<i64>, _> =
                    load_column_reader(&path, &mut buffer, Option::None);
                assert!(column_reader.is_ok());
                let mut column_reader = column_reader.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = RawBridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    if !raw_bridge.is_empty() {
                        for i in begin..end {
                            assert_eq!(
                                raw_bridge
                                    .get_validity_and_value(
                                        offset,
                                        i as usize,
                                        &result_row_range_set
                                    )
                                    .unwrap(),
                                (true, i as i64)
                            );
                        }
                    }
                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_beginning_with_filter() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");

        let num_values = 1000000;
        let num_tests = 100;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let mut buffer = load_column_to_direct_buffer(&path);

            let filter_low = get_random_number_in_range(15) as i128;
            let filter_length = get_random_number_in_range(20) as i128;
            let filter = IntegerRangeFilter::new(filter_low, filter_low + filter_length, true);
            let column_reader: Result<FixedLengthColumnReader<i64>, _> =
                load_column_reader(&path, &mut buffer, Option::Some(&filter));
            assert!(column_reader.is_ok());
            let mut column_reader = column_reader.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = RawBridge::new(false, step as usize);

                let res = column_reader.read_with_filter(
                    to_read,
                    offset,
                    &mut result_row_range_set,
                    &mut raw_bridge,
                );
                assert!(res.is_ok());
                if !raw_bridge.is_empty() {
                    for i in
                        max(begin, filter_low as u32)..min(end, (filter_low + filter_length) as u32)
                    {
                        assert_eq!(
                            raw_bridge
                                .get_validity_and_value(offset, i as usize, &result_row_range_set)
                                .unwrap(),
                            (true, i as i64)
                        );
                    }
                }
                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_middle_filter() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");

        let num_values = 1000000;
        let num_tests = 10;

        for num_test in 0..num_tests {
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + num_test;
                let mut buffer = load_column_to_direct_buffer(&path);
                let filter_low = get_random_number_in_range(15) as i128;
                let filter_length = get_random_number_in_range(20) as i128;
                let filter = IntegerRangeFilter::new(filter_low, filter_low + filter_length, true);
                let column_reader: Result<FixedLengthColumnReader<i64>, _> =
                    load_column_reader(&path, &mut buffer, Option::Some(&filter));
                assert!(column_reader.is_ok());
                let mut column_reader = column_reader.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = RawBridge::new(false, step as usize);

                    let res = column_reader.read_with_filter(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    if !raw_bridge.is_empty() {
                        for i in max(begin, filter_low as u32)
                            ..min(end, (filter_low + filter_length) as u32)
                        {
                            assert_eq!(
                                raw_bridge
                                    .get_validity_and_value(
                                        offset,
                                        i as usize,
                                        &result_row_range_set
                                    )
                                    .unwrap(),
                                (true, i as i64)
                            );
                        }
                    }
                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_plain_double_column_from_beginning() {
        let path = String::from("src/sample_files/plain_double_column.parquet");

        let num_values = 1000000;
        let num_tests = 100;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let mut buffer = load_column_to_direct_buffer(&path);
            let column_reader: Result<FixedLengthColumnReader<f64>, _> =
                load_column_reader(&path, &mut buffer, Option::None);
            assert!(column_reader.is_ok());
            let mut column_reader = column_reader.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = RawBridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    for i in begin..end {
                        assert_eq!(
                            raw_bridge
                                .get_validity_and_value(offset, i as usize, &result_row_range_set)
                                .unwrap(),
                            (true, i as f64)
                        );
                    }
                }
                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_plain_double_column_from_middle() {
        let path = String::from("src/sample_files/plain_double_column.parquet");

        let num_values = 1000000;
        let num_tests = 10;

        for num_test in 0..num_tests {
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + num_test;
                let mut buffer = load_column_to_direct_buffer(&path);
                let column_reader: Result<FixedLengthColumnReader<f64>, _> =
                    load_column_reader(&path, &mut buffer, Option::None);
                assert!(column_reader.is_ok());
                let mut column_reader = column_reader.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = RawBridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    if !raw_bridge.is_empty() {
                        for i in begin..end {
                            assert_eq!(
                                raw_bridge
                                    .get_validity_and_value(
                                        offset,
                                        i as usize,
                                        &result_row_range_set
                                    )
                                    .unwrap(),
                                (true, i as f64)
                            );
                        }
                    }
                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_plain_double_column_from_beginning_with_filter() {
        let path = String::from("src/sample_files/plain_double_column.parquet");

        let num_values = 1000000;
        let num_tests = 100;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let mut buffer = load_column_to_direct_buffer(&path);

            let filter_low = get_random_number_in_range(15) as i64;
            let filter_length = get_random_number_in_range(20) as i64;
            let filter = FloatPointRangeFilter::new(
                filter_low as f64,
                (filter_low + filter_length) as f64,
                true,
                true,
                false,
                false,
                false,
            );
            let column_reader: Result<FixedLengthColumnReader<f64>, _> =
                load_column_reader(&path, &mut buffer, Option::Some(&filter));
            assert!(column_reader.is_ok());
            let mut column_reader = column_reader.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = RawBridge::new(false, step as usize);

                let res = column_reader.read_with_filter(
                    to_read,
                    offset,
                    &mut result_row_range_set,
                    &mut raw_bridge,
                );
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    for i in max(begin, filter_low as u32 + 1)
                        ..min(end, (filter_low + filter_length) as u32 - 1)
                    {
                        assert_eq!(
                            raw_bridge
                                .get_validity_and_value(offset, i as usize, &result_row_range_set)
                                .unwrap(),
                            (true, i as f64)
                        );
                    }
                }
                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_plain_double_column_from_middle_filter() {
        let path = String::from("src/sample_files/plain_double_column.parquet");

        let num_values = 1000000;
        let num_tests = 10;

        for num_test in 0..num_tests {
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + num_test;
                let mut buffer = load_column_to_direct_buffer(&path);
                let filter_low = get_random_number_in_range(15) as i64;
                let filter_length = get_random_number_in_range(20) as i64;
                let filter = FloatPointRangeFilter::new(
                    filter_low as f64,
                    (filter_low + filter_length) as f64,
                    true,
                    true,
                    false,
                    false,
                    false,
                );
                let column_reader: Result<FixedLengthColumnReader<f64>, _> =
                    load_column_reader(&path, &mut buffer, Option::Some(&filter));
                assert!(column_reader.is_ok());
                let mut column_reader = column_reader.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = RawBridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    if !raw_bridge.is_empty() {
                        for i in max(begin, filter_low as u32 + 1)
                            ..min(end, (filter_low + filter_length) as u32 - 1)
                        {
                            assert_eq!(
                                raw_bridge
                                    .get_validity_and_value(
                                        offset,
                                        i as usize,
                                        &result_row_range_set
                                    )
                                    .unwrap(),
                                (true, i as f64)
                            );
                        }
                    }
                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_beginning_streaming_buffer() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let num_values = 1000000;

        for shift in 12..16 {
            let buffer_size = 1 << shift;
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let res = LocalFileLoader::new(&path);
            assert!(res.is_ok());
            let file = res.unwrap();
            let mut buffer = load_column_to_streaming_buffer(&file, buffer_size);
            let column_reader: Result<FixedLengthColumnReader<i64>, _> =
                load_column_reader(&path, &mut buffer, Option::None);
            assert!(column_reader.is_ok());
            let mut column_reader = column_reader.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = RawBridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    for i in begin..end {
                        assert_eq!(
                            raw_bridge
                                .get_validity_and_value(offset, i as usize, &result_row_range_set)
                                .unwrap(),
                            (true, i as i64)
                        );
                    }
                }
                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_middle_streaming_buffer() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let num_values = 1000000;

        for shift in 12..16 {
            let buffer_size = 1 << shift;
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + shift;
                let res = LocalFileLoader::new(&path);
                assert!(res.is_ok());
                let file = res.unwrap();
                let mut buffer = load_column_to_streaming_buffer(&file, buffer_size);
                let column_reader: Result<FixedLengthColumnReader<i64>, _> =
                    load_column_reader(&path, &mut buffer, Option::None);
                assert!(column_reader.is_ok());
                let mut column_reader = column_reader.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = RawBridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    if !raw_bridge.is_empty() {
                        for i in begin..end {
                            assert_eq!(
                                raw_bridge
                                    .get_validity_and_value(
                                        offset,
                                        i as usize,
                                        &result_row_range_set
                                    )
                                    .unwrap(),
                                (true, i as i64)
                            );
                        }
                    }
                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_beginning_with_filter_streaming_buffer() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let num_values = 1000000;

        for shift in 12..16 {
            let buffer_size = 1 << shift;
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let res = LocalFileLoader::new(&path);
            assert!(res.is_ok());
            let file = res.unwrap();
            let mut buffer = load_column_to_streaming_buffer(&file, buffer_size);

            let filter_low = get_random_number_in_range(15) as i128;
            let filter_length = get_random_number_in_range(20) as i128;
            let filter = IntegerRangeFilter::new(filter_low, filter_low + filter_length, true);
            let column_reader: Result<FixedLengthColumnReader<i64>, _> =
                load_column_reader(&path, &mut buffer, Option::Some(&filter));
            assert!(column_reader.is_ok());
            let mut column_reader = column_reader.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = RawBridge::new(false, step as usize);

                let res = column_reader.read_with_filter(
                    to_read,
                    offset,
                    &mut result_row_range_set,
                    &mut raw_bridge,
                );
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    for i in
                        max(begin, filter_low as u32)..min(end, (filter_low + filter_length) as u32)
                    {
                        assert_eq!(
                            raw_bridge
                                .get_validity_and_value(offset, i as usize, &result_row_range_set)
                                .unwrap(),
                            (true, i as i64)
                        );
                    }
                }
                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_middle_filter_streaming_buffer() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let num_values = 1000000;

        for shift in 12..16 {
            let buffer_size = 1 << shift;
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + shift;
                let res = LocalFileLoader::new(&path);
                assert!(res.is_ok());
                let file = res.unwrap();
                let mut buffer = load_column_to_streaming_buffer(&file, buffer_size);

                let filter_low = get_random_number_in_range(15) as i128;
                let filter_length = get_random_number_in_range(20) as i128;
                let filter = IntegerRangeFilter::new(filter_low, filter_low + filter_length, true);
                let column_reader: Result<FixedLengthColumnReader<i64>, _> =
                    load_column_reader(&path, &mut buffer, Option::Some(&filter));
                assert!(column_reader.is_ok());
                let mut column_reader = column_reader.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = RawBridge::new(false, step as usize);

                    let res = column_reader.read_with_filter(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    if !raw_bridge.is_empty() {
                        for i in max(begin, filter_low as u32)
                            ..min(end, (filter_low + filter_length) as u32)
                        {
                            assert_eq!(
                                raw_bridge
                                    .get_validity_and_value(
                                        offset,
                                        i as usize,
                                        &result_row_range_set
                                    )
                                    .unwrap(),
                                (true, i as i64)
                            );
                        }
                    }
                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_double_filter() {
        let path = String::from("src/sample_files/plain_double_column.parquet");

        let mut buffer = load_column_to_direct_buffer(&path);
        let filter = FloatPointRangeFilter::new(100.0, 200.0, true, true, false, false, false);

        let column_reader: Result<FixedLengthColumnReader<f64>, _> =
            load_column_reader(&path, &mut buffer, Option::Some(&filter));
        assert!(column_reader.is_ok());

        let mut column_reader = column_reader.unwrap();
        let to_read = RowRange::new(0, 300);
        let offset = 0;
        let mut result_row_range_set = RowRangeSet::new(offset);
        let mut raw_bridge = RawBridge::new(false, 300);

        let res = column_reader.read_with_filter(
            to_read,
            offset,
            &mut result_row_range_set,
            &mut raw_bridge,
        );
        assert!(res.is_ok());
        assert_eq!(result_row_range_set.get_row_ranges()[0].get_length(), 99);

        let mut buffer = load_column_to_direct_buffer(&path);
        let filter = FloatPointRangeFilter::new(100.0, 200.0, true, true, true, false, false);

        let column_reader: Result<FixedLengthColumnReader<f64>, _> =
            load_column_reader(&path, &mut buffer, Option::Some(&filter));
        assert!(column_reader.is_ok());

        let mut column_reader = column_reader.unwrap();
        let to_read = RowRange::new(0, 300);
        let offset = 0;
        let mut result_row_range_set = RowRangeSet::new(offset);
        let mut raw_bridge = RawBridge::new(false, 300);

        let res = column_reader.read_with_filter(
            to_read,
            offset,
            &mut result_row_range_set,
            &mut raw_bridge,
        );
        assert!(res.is_ok());
        assert_eq!(result_row_range_set.get_row_ranges()[0].get_length(), 100);

        let mut buffer = load_column_to_direct_buffer(&path);
        let filter = FloatPointRangeFilter::new(100.0, 200.0, true, true, true, true, false);

        let column_reader: Result<FixedLengthColumnReader<f64>, _> =
            load_column_reader(&path, &mut buffer, Option::Some(&filter));
        assert!(column_reader.is_ok());

        let mut column_reader = column_reader.unwrap();
        let to_read = RowRange::new(0, 300);
        let offset = 0;
        let mut result_row_range_set = RowRangeSet::new(offset);
        let mut raw_bridge = RawBridge::new(false, 300);

        let res = column_reader.read_with_filter(
            to_read,
            offset,
            &mut result_row_range_set,
            &mut raw_bridge,
        );
        assert!(res.is_ok());
        assert_eq!(result_row_range_set.get_row_ranges()[0].get_length(), 101);
    }

    #[test]
    fn test_reading_filtered_empty() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let mut buffer = load_column_to_direct_buffer(&path);

        let filter = IntegerRangeFilter::new(10000000, 20000000, true);
        let column_reader: Result<FixedLengthColumnReader<i64>, _> =
            load_column_reader(&path, &mut buffer, Option::Some(&filter));
        assert!(column_reader.is_ok());

        let mut column_reader = column_reader.unwrap();
        let to_read = RowRange::new(0, 100);
        let offset = 0;
        let mut result_row_range_set = RowRangeSet::new(offset);
        let mut raw_bridge = RawBridge::new(false, 100);

        let res = column_reader.read_with_filter(
            to_read,
            offset,
            &mut result_row_range_set,
            &mut raw_bridge,
        );

        assert!(res.is_ok());
        assert_eq!(result_row_range_set.get_row_ranges().is_empty(), true);
        assert_eq!(raw_bridge.is_empty(), true);
    }

    #[test]
    fn test_reading_backward() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let mut buffer = load_column_to_direct_buffer(&path);
        let column_reader: Result<FixedLengthColumnReader<i64>, _> =
            load_column_reader(&path, &mut buffer, None);
        assert!(column_reader.is_ok());

        let mut column_reader = column_reader.unwrap();
        let to_read = RowRange::new(0, 100);
        let offset = 0;
        let mut result_row_range_set = RowRangeSet::new(offset);
        let mut raw_bridge = RawBridge::new(false, 100);

        let res = column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
        assert!(res.is_ok());

        let to_read = RowRange::new(50, 100);
        let mut result_row_range_set = RowRangeSet::new(offset);
        let mut raw_bridge = RawBridge::new(false, 100);

        let res = column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);

        assert!(res.is_err());
    }

    #[test]
    fn test_reading_exceeding_column_size() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let mut buffer = load_column_to_direct_buffer(&path);
        let column_reader: Result<FixedLengthColumnReader<i64>, _> =
            load_column_reader(&path, &mut buffer, None);
        assert!(column_reader.is_ok());

        let mut column_reader = column_reader.unwrap();
        let to_read = RowRange::new(999990, 1000010);
        let offset = 0;
        let mut result_row_range_set = RowRangeSet::new(offset);
        let mut raw_bridge = RawBridge::new(false, 100);

        let res = column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
        assert!(res.is_err());
    }
}
