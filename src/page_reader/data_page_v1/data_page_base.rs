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

use std::intrinsics::unlikely;

use crate::bridge::bridge_base::Bridge;
use crate::utils::exceptions::BoltReaderError;
use crate::utils::row_range_set::{RowRange, RowRangeSet};

pub trait DataPage<T> {
    fn data_page_has_null(&self) -> bool;

    fn get_data_page_num_values(&self) -> usize;

    fn get_data_page_offset(&self) -> usize;

    fn get_data_page_type_size(&self) -> usize;

    fn is_zero_copied(&self) -> bool;

    fn read(
        &mut self,
        to_read: RowRange,
        offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn Bridge<T>,
    ) -> Result<bool, BoltReaderError>;

    fn read_with_filter(
        &mut self,
        to_read: RowRange,
        offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn Bridge<T>,
    ) -> Result<bool, BoltReaderError>;
}

pub fn get_data_page_covered_range(
    page_begin: usize,
    page_end: usize,
    row_range_offset: usize,
    row_range: &RowRange,
) -> Result<Option<RowRange>, BoltReaderError> {
    if unlikely(row_range.begin + row_range_offset < page_begin) {
        return Err(BoltReaderError::FixedLengthDataPageError(format!("Range processing error. Input range begin: {} cannot be smaller than the data page begin: {} with offset", row_range.begin + row_range_offset, page_begin)));
    }

    row_range.get_covered_range(row_range_offset, page_begin, page_end)
}

pub fn get_data_page_remaining_range(
    page_begin: usize,
    page_end: usize,
    row_range_offset: usize,
    row_range: &RowRange,
) -> Result<Option<RowRange>, BoltReaderError> {
    if unlikely(row_range.begin + row_range_offset < page_begin) {
        return Err(BoltReaderError::FixedLengthDataPageError(format!("Range processing error. Input range begin: {} cannot be smaller than the data page begin: {} with offset", row_range.begin + row_range_offset, page_begin)));
    }
    row_range.get_right_remaining_range(row_range_offset, page_begin, page_end)
}

#[cfg(test)]
mod tests {
    use std::mem;

    use crate::metadata::page_header::read_page_header;
    use crate::page_reader::data_page_v1::data_page_base::{
        get_data_page_covered_range, get_data_page_remaining_range, DataPage,
    };
    use crate::page_reader::data_page_v1::fixed_length_plain_data_page_v1::FixedLengthPlainDataPageReaderV1;
    use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
    use crate::utils::exceptions::BoltReaderError;
    use crate::utils::file_loader::LoadFile;
    use crate::utils::local_file_loader::LocalFileLoader;
    use crate::utils::row_range_set::RowRange;

    fn load_plain_data_page<'a, T: 'static + std::marker::Copy + std::default::Default>(
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
        let data_size = page_header.uncompressed_page_size - 8;

        (
            FixedLengthPlainDataPageReaderV1::new(
                &page_header,
                &mut buf,
                data_page_offset,
                mem::size_of::<T>(),
                false,
                data_size as usize,
                None,
                Option::None,
            ),
            buf,
        )
    }

    #[test]
    fn test_covered_range() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let data_page_offset = 100;

        let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
            load_plain_data_page(data_page_offset, path);
        assert!(data_page.is_ok());

        let data_page = data_page.unwrap();

        let row_range = RowRange::new(1, 5);
        let offset = 1000;
        let res = get_data_page_covered_range(
            data_page.get_data_page_offset(),
            data_page.get_data_page_num_values() + data_page.get_data_page_offset(),
            offset,
            &row_range,
        );
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_some());
        let covered_range = res.unwrap();
        assert_eq!(covered_range.begin, row_range.begin);
        assert_eq!(covered_range.end, covered_range.end);

        let row_range = RowRange::new(1, 100000000);
        let offset = 1000;
        let res = get_data_page_covered_range(
            data_page.get_data_page_offset(),
            data_page.get_data_page_num_values() + data_page.get_data_page_offset(),
            offset,
            &row_range,
        );
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_some());
        let covered_range = res.unwrap();
        assert_eq!(covered_range.begin, row_range.begin);
        assert_eq!(
            covered_range.end,
            data_page.get_data_page_num_values() + data_page_offset - offset
        );
    }

    #[test]
    fn test_nonexistent_covered_range() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let data_page_offset = 100;

        let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
            load_plain_data_page(data_page_offset, path);
        assert!(data_page.is_ok());

        let data_page = data_page.unwrap();

        let row_range = RowRange::new(1, 5);
        let offset = 10000000;
        let res = get_data_page_covered_range(
            data_page.get_data_page_offset(),
            data_page.get_data_page_num_values() + data_page.get_data_page_offset(),
            offset,
            &row_range,
        );

        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn test_invalid_covered_range() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let data_page_offset = 100;

        let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
            load_plain_data_page(data_page_offset, path);
        assert!(data_page.is_ok());

        let data_page = data_page.unwrap();

        let row_range = RowRange::new(1, 5);
        let offset = 10;
        let res = get_data_page_covered_range(
            data_page.get_data_page_offset(),
            data_page.get_data_page_num_values() + data_page.get_data_page_offset(),
            offset,
            &row_range,
        );

        assert!(res.is_err());
    }

    #[test]
    fn test_remaining_range() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let data_page_offset = 100;

        let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
            load_plain_data_page(data_page_offset, path);
        assert!(data_page.is_ok());

        let data_page = data_page.unwrap();

        let row_range = RowRange::new(1, 100000000);
        let offset = 1000;
        let res = get_data_page_remaining_range(
            data_page.get_data_page_offset(),
            data_page.get_data_page_num_values() + data_page.get_data_page_offset(),
            offset,
            &row_range,
        );
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_some());
        let covered_range = res.unwrap();
        assert_eq!(
            covered_range.begin,
            data_page.get_data_page_num_values() + data_page_offset - offset
        );
        assert_eq!(covered_range.end, row_range.end);

        let row_range = RowRange::new(10000000, 100000000);
        let offset = 1000;
        let res = get_data_page_remaining_range(
            data_page.get_data_page_offset(),
            data_page.get_data_page_num_values() + data_page.get_data_page_offset(),
            offset,
            &row_range,
        );
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_some());
        let covered_range = res.unwrap();
        assert_eq!(covered_range.begin, row_range.begin);
        assert_eq!(covered_range.end, row_range.end);
    }

    #[test]
    fn test_nonexistent_remaining_range() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let data_page_offset = 100;

        let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
            load_plain_data_page(data_page_offset, path);
        assert!(data_page.is_ok());

        let data_page = data_page.unwrap();

        let row_range = RowRange::new(1, 5);
        let offset = 1000;
        let res = get_data_page_remaining_range(
            data_page.get_data_page_offset(),
            data_page.get_data_page_num_values() + data_page.get_data_page_offset(),
            offset,
            &row_range,
        );

        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn test_invalid_remaining_range() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let data_page_offset = 100;

        let (data_page, _buffer): (Result<FixedLengthPlainDataPageReaderV1<i64>, _>, _) =
            load_plain_data_page(data_page_offset, path);
        assert!(data_page.is_ok());

        let data_page = data_page.unwrap();

        let row_range = RowRange::new(1, 5);
        let offset = 10;
        let res = get_data_page_covered_range(
            data_page.get_data_page_offset(),
            data_page.get_data_page_num_values() + data_page.get_data_page_offset(),
            offset,
            &row_range,
        );

        assert!(res.is_err());
    }
}
