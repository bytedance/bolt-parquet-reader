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

use std::any::TypeId;
use std::fmt::Formatter;
use std::mem;

use crate::bridge::bridge_base::Bridge;
use crate::convert_generic_vec;
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

pub struct FixedLengthPlainDataPageReaderV1<'a, T> {
    has_null: bool,
    num_values: usize,
    current_offset: usize,
    type_size: usize,
    #[allow(dead_code)]
    zero_copy: bool,
    filter: Option<&'a dyn FixedLengthRangeFilter>,
    validity: Option<Vec<bool>>,
    data: Vec<T>,
}

#[allow(dead_code)]
impl<'a, T: ToString> std::fmt::Display for FixedLengthPlainDataPageReaderV1<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let data_str = self
            .data
            .iter()
            .take(DEFAULT_DISPLAY_NUMBER)
            .map(T::to_string)
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
            "Plain Data Page: has_null {}, num_values {}, current_offset {}\nData: {} ...\nValidity: {} ...",
             self.has_null, self.num_values, self.current_offset, data_str, validity_str
        )
    }
}

impl<'a, T> DataPage<T> for FixedLengthPlainDataPageReaderV1<'a, T> {
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
}

impl<'a, T: 'static + std::marker::Copy> FixedLengthPlainDataPageReaderV1<'a, T> {
    pub fn new(
        page_header: &PageHeader,
        buffer: &mut dyn ByteBufferBase,
        current_offset: usize,
        type_size: usize,
        has_null: bool,
        filter: Option<&'a (dyn FixedLengthRangeFilter + 'a)>,
        validity: Option<Vec<bool>>,
    ) -> Result<FixedLengthPlainDataPageReaderV1<'a, T>, BoltReaderError> {
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
        let data_size: usize = num_values * type_size;

        let zero_copy;
        let data: Vec<T> = if buffer.can_create_buffer_slice(buffer.get_rpos(), data_size) {
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

        Ok(FixedLengthPlainDataPageReaderV1 {
            has_null,
            num_values,
            current_offset,
            type_size,
            zero_copy,
            filter,
            validity,
            data,
        })
    }

    pub fn read_with_filter(
        &self,
        to_read: RowRange,
        offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn Bridge<T>,
    ) -> Result<(), BoltReaderError> {
        let start = to_read.begin + offset - self.current_offset;
        let end = to_read.end + offset - self.current_offset;
        let filter = self.filter.unwrap();
        let vec_t = unsafe { convert_generic_vec!(&self.data[..], mem::size_of::<T>(), T) };

        if TypeId::of::<T>() == TypeId::of::<i64>() {
            let vec = unsafe { convert_generic_vec!(&self.data[..], mem::size_of::<i64>(), i64) };

            let mut generator = RowRangeSetGenerator::new(result_row_range_set);

            for i in start..end {
                let filter_res = filter.check_i64(vec[i]);
                generator.update(i + self.current_offset - offset, filter_res);
                if filter_res {
                    result_bridge.append_non_null_result(vec_t[i]);
                }
            }
            generator.finish(end + self.current_offset - offset);

            mem::forget(vec);
        } else if TypeId::of::<T>() == TypeId::of::<i32>() {
            let vec = unsafe { convert_generic_vec!(&self.data[..], mem::size_of::<i32>(), i32) };

            let mut generator = RowRangeSetGenerator::new(result_row_range_set);

            for i in start..end {
                let filter_res = filter.check_i32(vec[i]);
                generator.update(i + self.current_offset - offset, filter_res);
                if filter_res {
                    result_bridge.append_non_null_result(vec_t[i]);
                }
            }
            generator.finish(end + self.current_offset - offset);

            mem::forget(vec);
        } else if TypeId::of::<T>() == TypeId::of::<f64>() {
            let vec = unsafe { convert_generic_vec!(&self.data[..], mem::size_of::<f64>(), f64) };

            let mut generator = RowRangeSetGenerator::new(result_row_range_set);

            for i in start..end {
                let filter_res = filter.check_f64(vec[i]);
                generator.update(i + self.current_offset - offset, filter_res);
                if filter_res {
                    result_bridge.append_non_null_result(vec_t[i]);
                }
            }
            generator.finish(end + self.current_offset - offset);

            mem::forget(vec);
        } else if TypeId::of::<T>() == TypeId::of::<f32>() {
            let vec = unsafe { convert_generic_vec!(&self.data[..], mem::size_of::<f32>(), f32) };

            let mut generator = RowRangeSetGenerator::new(result_row_range_set);

            for i in start..end {
                let filter_res = filter.check_f32(vec[i]);
                generator.update(i + self.current_offset - offset, filter_res);
                if filter_res {
                    result_bridge.append_non_null_result(vec_t[i]);
                }
            }
            generator.finish(end + self.current_offset - offset);

            mem::forget(vec);
        }

        mem::forget(vec_t);
        Ok(())
    }

    pub fn read(
        &self,
        to_read: RowRange,
        offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn Bridge<T>,
    ) -> Result<(), BoltReaderError> {
        if self.has_null {
            return Err(BoltReaderError::NotYetImplementedError(String::from(
                "Not Yet Implemented: Read Data Page with nulls",
            )));
        } else {
            let start = to_read.begin + offset - self.current_offset;
            let end = to_read.end + offset - self.current_offset;
            result_row_range_set.add_row_ranges(
                to_read.begin + offset - result_row_range_set.get_offset(),
                to_read.end + offset - result_row_range_set.get_offset(),
            );
            result_bridge.append_non_null_results(&self.data[start..end])?;
        }
        Ok(())
    }
}

#[allow(dead_code)]
#[inline(always)]
pub fn destroy_fixed_length_plain_data_page_v1<T>(data_page: FixedLengthPlainDataPageReaderV1<T>) {
    mem::forget(data_page.data);
}

#[cfg(test)]
mod tests {

    use std::mem;

    use crate::bridge::bridge_base::Bridge;
    use crate::bridge::raw_bridge::RawBridge;
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
    use crate::filters::integer_range_filter::IntegerRangeFilter;
    use crate::metadata::page_header::read_page_header;
    use crate::page_reader::data_page_v1::data_page_base::DataPage;
    use crate::page_reader::data_page_v1::fixed_length_plain_data_page_v1::{
        destroy_fixed_length_plain_data_page_v1, FixedLengthPlainDataPageReaderV1,
    };
    use crate::utils::byte_buffer_base::ByteBufferBase;
    use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
    use crate::utils::exceptions::BoltReaderError;
    use crate::utils::file_loader::LoadFile;
    use crate::utils::file_streaming_byte_buffer::{FileStreamingBuffer, StreamingByteBuffer};
    use crate::utils::local_file_loader::LocalFileLoader;
    use crate::utils::row_range_set::{RowRange, RowRangeSet};

    fn load_non_null_plain_data_page<'a, T: 'static + std::marker::Copy>(
        data_page_offset: usize,
        path: String,
    ) -> (
        Result<FixedLengthPlainDataPageReaderV1<'a, T>, BoltReaderError>,
        DirectByteBuffer,
    ) {
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        let res = DirectByteBuffer::from_file(&file, 0, file.get_file_size());

        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();

        buf.set_rpos(buf.get_rpos() + 8);

        (
            FixedLengthPlainDataPageReaderV1::new(
                &page_header,
                &mut buf,
                data_page_offset,
                mem::size_of::<T>(),
                false,
                None,
                Option::None,
            ),
            buf,
        )
    }

    fn load_non_null_plain_data_page_with_filter<'a, T: 'static + std::marker::Copy>(
        data_page_offset: usize,
        path: String,
        filter: &'a (dyn FixedLengthRangeFilter + 'a),
    ) -> (
        Result<FixedLengthPlainDataPageReaderV1<'a, T>, BoltReaderError>,
        DirectByteBuffer,
    ) {
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        let res = DirectByteBuffer::from_file(&file, 0, file.get_file_size());

        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();

        buf.set_rpos(buf.get_rpos() + 8);

        (
            FixedLengthPlainDataPageReaderV1::new(
                &page_header,
                &mut buf,
                data_page_offset,
                mem::size_of::<T>(),
                false,
                Option::Some(filter),
                Option::None,
            ),
            buf,
        )
    }

    fn load_non_null_plain_data_page_in_streaming_buffer<'a, T: 'static + std::marker::Copy>(
        file: &'a (dyn LoadFile + 'a),
        data_page_offset: usize,
        buffer_size: usize,
    ) -> (
        Result<FixedLengthPlainDataPageReaderV1<T>, BoltReaderError>,
        StreamingByteBuffer,
    ) {
        let res = StreamingByteBuffer::from_file(file, 0, file.get_file_size(), buffer_size);

        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();

        buf.set_rpos(buf.get_rpos() + 8);

        (
            FixedLengthPlainDataPageReaderV1::new(
                &page_header,
                &mut buf,
                data_page_offset,
                mem::size_of::<T>(),
                false,
                None,
                Option::None,
            ),
            buf,
        )
    }

    fn load_non_null_plain_data_page_with_filter_in_streaming_buffer<
        'a,
        T: 'static + std::marker::Copy,
    >(
        file: &'a (dyn LoadFile + 'a),
        data_page_offset: usize,
        buffer_size: usize,
        filter: &'a (dyn FixedLengthRangeFilter + 'a),
    ) -> (
        Result<FixedLengthPlainDataPageReaderV1<'a, T>, BoltReaderError>,
        StreamingByteBuffer<'a>,
    ) {
        let res = StreamingByteBuffer::from_file(file, 0, file.get_file_size(), buffer_size);

        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();

        buf.set_rpos(buf.get_rpos() + 8);

        (
            FixedLengthPlainDataPageReaderV1::new(
                &page_header,
                &mut buf,
                data_page_offset,
                mem::size_of::<T>(),
                false,
                Option::Some(filter),
                Option::None,
            ),
            buf,
        )
    }

    #[test]
    fn test_create_fixed_length_plain_data_page_v1() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
            load_non_null_plain_data_page(100, path);
        assert!(data_page.is_ok());
        let data_page = data_page.unwrap();

        assert_eq!(data_page.to_string(), "Plain Data Page: has_null false, num_values 11212, current_offset 100\nData: 429, 54914, 54914, 54914, 54914, 54914, 54914, 54914, 54915, 54916 ...\nValidity: true, true, true, true, true, true, true, true, true, true ...\n");

        if data_page.zero_copy {
            destroy_fixed_length_plain_data_page_v1(data_page);
        }
    }

    #[test]
    fn test_read_data_page() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
            load_non_null_plain_data_page(100, path);
        assert!(data_page.is_ok());
        let data_page = data_page.unwrap();
        let to_read = RowRange::new(50, 60);
        let offset = 50;
        let capacity = 1024;

        let to_read = data_page.get_data_page_covered_range(
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
        let mut raw_bridge = RawBridge::new(false, capacity);
        let res = data_page.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
        assert!(res.is_ok());

        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 50, &result_row_range_set)
                .unwrap(),
            (true, 429)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 51, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 52, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 53, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 54, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 55, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 56, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 57, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 58, &result_row_range_set)
                .unwrap(),
            (true, 54915)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 59, &result_row_range_set)
                .unwrap(),
            (true, 54916)
        );

        if data_page.zero_copy {
            destroy_fixed_length_plain_data_page_v1(data_page);
        }
    }

    #[test]
    fn test_read_data_page_with_filter() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let filter = IntegerRangeFilter::new(0, 54914, true);
        let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
            load_non_null_plain_data_page_with_filter(100, path, &filter);
        assert!(data_page.is_ok());
        let data_page = data_page.unwrap();
        let to_read = RowRange::new(50, 60);
        let offset = 50;
        let capacity = 1024;

        let to_read = data_page.get_data_page_covered_range(
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
        let mut raw_bridge = RawBridge::new(false, capacity);
        let res =
            data_page.read_with_filter(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
        assert!(res.is_ok());

        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 50, &result_row_range_set)
                .unwrap(),
            (true, 429)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 51, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 52, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 53, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 54, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 55, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 56, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 57, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );

        for i in 0..raw_bridge.get_size() {
            let value = raw_bridge
                .get_validity_and_value(offset, i + 50, &result_row_range_set)
                .unwrap()
                .1;
            assert!(filter.check_i64(value));
        }

        if data_page.zero_copy {
            destroy_fixed_length_plain_data_page_v1(data_page);
        }
    }

    #[test]
    fn test_read_data_page_in_streaming_buffer() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();

        for i in 0..16 {
            let buffer_size = 1 << i;
            let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
                load_non_null_plain_data_page_in_streaming_buffer(&file, 100, buffer_size);
            assert!(data_page.is_ok());
            let data_page = data_page.unwrap();
            let to_read = RowRange::new(50, 60);
            let offset = 50;
            let capacity = 1024;

            let to_read = data_page.get_data_page_covered_range(
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
            let mut raw_bridge = RawBridge::new(false, capacity);
            let res = data_page.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
            assert!(res.is_ok());

            assert_eq!(
                raw_bridge
                    .get_validity_and_value(offset, 50, &result_row_range_set)
                    .unwrap(),
                (true, 429)
            );
            assert_eq!(
                raw_bridge
                    .get_validity_and_value(offset, 51, &result_row_range_set)
                    .unwrap(),
                (true, 54914)
            );
            assert_eq!(
                raw_bridge
                    .get_validity_and_value(offset, 52, &result_row_range_set)
                    .unwrap(),
                (true, 54914)
            );
            assert_eq!(
                raw_bridge
                    .get_validity_and_value(offset, 53, &result_row_range_set)
                    .unwrap(),
                (true, 54914)
            );
            assert_eq!(
                raw_bridge
                    .get_validity_and_value(offset, 54, &result_row_range_set)
                    .unwrap(),
                (true, 54914)
            );
            assert_eq!(
                raw_bridge
                    .get_validity_and_value(offset, 55, &result_row_range_set)
                    .unwrap(),
                (true, 54914)
            );
            assert_eq!(
                raw_bridge
                    .get_validity_and_value(offset, 56, &result_row_range_set)
                    .unwrap(),
                (true, 54914)
            );
            assert_eq!(
                raw_bridge
                    .get_validity_and_value(offset, 57, &result_row_range_set)
                    .unwrap(),
                (true, 54914)
            );
            assert_eq!(
                raw_bridge
                    .get_validity_and_value(offset, 58, &result_row_range_set)
                    .unwrap(),
                (true, 54915)
            );
            assert_eq!(
                raw_bridge
                    .get_validity_and_value(offset, 59, &result_row_range_set)
                    .unwrap(),
                (true, 54916)
            );

            if data_page.zero_copy {
                destroy_fixed_length_plain_data_page_v1(data_page);
            }
        }
    }

    #[test]
    fn test_read_data_page_with_filter_in_streaming() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        let filter = IntegerRangeFilter::new(0, 54914, true);

        let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
            load_non_null_plain_data_page_with_filter_in_streaming_buffer(&file, 100, 5, &filter);
        assert!(data_page.is_ok());
        let data_page = data_page.unwrap();
        let to_read = RowRange::new(50, 60);
        let offset = 50;
        let capacity = 1024;

        let to_read = data_page.get_data_page_covered_range(
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
        let mut raw_bridge = RawBridge::new(false, capacity);
        let res =
            data_page.read_with_filter(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
        assert!(res.is_ok());

        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 50, &result_row_range_set)
                .unwrap(),
            (true, 429)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 51, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 52, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 53, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 54, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 55, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 56, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(offset, 57, &result_row_range_set)
                .unwrap(),
            (true, 54914)
        );

        for i in 0..raw_bridge.get_size() {
            let value = raw_bridge
                .get_validity_and_value(offset, i + 50, &result_row_range_set)
                .unwrap()
                .1;
            assert!(filter.check_i64(value));
        }

        if data_page.zero_copy {
            destroy_fixed_length_plain_data_page_v1(data_page);
        }
    }

    #[test]
    fn test_read_outside_of_data_page() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
            load_non_null_plain_data_page(100, path);
        assert!(data_page.is_ok());
        let data_page = data_page.unwrap();
        let to_read = RowRange::new(
            data_page.get_data_page_num_values() - 10,
            data_page.get_data_page_num_values() + 10,
        );
        let offset = 100;
        let capacity = 1024;

        let to_read = data_page.get_data_page_covered_range(
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
        let mut raw_bridge = RawBridge::new(false, capacity);
        let res = data_page.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
        assert!(res.is_ok());
        assert_eq!(raw_bridge.get_size(), 10);

        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 10,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26182)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 9,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26182)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 8,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26182)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 7,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26183)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 6,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26208)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 5,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26208)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 4,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26208)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 3,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26208)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 2,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26208)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 1,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26208)
        );

        if data_page.zero_copy {
            destroy_fixed_length_plain_data_page_v1(data_page);
        }
    }

    #[test]
    fn test_read_outside_of_data_page_filter() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
            load_non_null_plain_data_page(100, path);
        assert!(data_page.is_ok());
        let data_page = data_page.unwrap();
        let to_read = RowRange::new(
            data_page.get_data_page_num_values() - 10,
            data_page.get_data_page_num_values() + 10,
        );
        let offset = 100;
        let capacity = 1024;

        let to_read = data_page.get_data_page_covered_range(
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
        let mut raw_bridge = RawBridge::new(false, capacity);
        let res = data_page.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
        assert!(res.is_ok());
        assert_eq!(raw_bridge.get_size(), 10);

        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 10,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26182)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 9,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26182)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 8,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26182)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 7,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26183)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 6,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26208)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 5,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26208)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 4,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26208)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 3,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26208)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 2,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26208)
        );
        assert_eq!(
            raw_bridge
                .get_validity_and_value(
                    offset,
                    data_page.get_data_page_num_values() - 1,
                    &result_row_range_set
                )
                .unwrap(),
            (true, 26208)
        );

        if data_page.zero_copy {
            destroy_fixed_length_plain_data_page_v1(data_page);
        }
    }
}
