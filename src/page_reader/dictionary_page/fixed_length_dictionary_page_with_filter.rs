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
use std::cmp::min;
use std::fmt::{Display, Formatter};
use std::mem;

use crate::convert_generic_vec;
use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
use crate::metadata::parquet_metadata_thrift;
use crate::metadata::parquet_metadata_thrift::PageHeader;
use crate::page_reader::dictionary_page::dictionary_page_base::DictionaryPage;
use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::exceptions::BoltReaderError;

struct DictionaryValue<T> {
    pub validity: bool,
    pub value: T,
}

impl<T: std::marker::Copy> DictionaryValue<T> {
    #[inline(always)]
    pub fn new(validity: bool, value: T) -> DictionaryValue<T> {
        DictionaryValue { validity, value }
    }
}

impl<T: Display> std::fmt::Display for DictionaryValue<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.value, self.validity)
    }
}

pub struct FixedLengthDictionaryWithFilter<T> {
    dictionary: Vec<DictionaryValue<T>>,
    num_values: usize,
    sorted: bool,
    type_size: usize,
}

#[allow(dead_code)]
impl<T: Display> std::fmt::Display for FixedLengthDictionaryWithFilter<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let dict_str = (0..min(10, self.num_values))
            .map(|i| self.dictionary[i].to_string())
            .collect::<Vec<String>>()
            .join(", ");

        writeln!(
            f,
            "Dictionary page: num_values {}, sorted {}\nDictionary: {}...",
            self.num_values, self.sorted, dict_str
        )
    }
}

impl<T: std::marker::Copy> DictionaryPage<T> for FixedLengthDictionaryWithFilter<T> {
    #[inline(always)]
    fn validate(&self, index: usize) -> bool {
        self.dictionary[index].validity
    }

    #[inline(always)]
    fn find(&self, index: usize) -> T {
        self.dictionary[index].value
    }

    fn get_num_values(&self) -> usize {
        self.num_values
    }

    #[inline(always)]
    fn get_type_size(&self) -> usize {
        self.type_size
    }
}

impl<T> FixedLengthDictionaryWithFilter<T>
where
    T: std::marker::Copy + 'static,
{
    pub fn new(
        page_header: &PageHeader,
        buffer: &mut dyn ByteBufferBase,
        type_size: usize,
        filter: &dyn FixedLengthRangeFilter,
    ) -> Result<FixedLengthDictionaryWithFilter<T>, BoltReaderError> {
        let header = match &page_header.dictionary_page_header {
            Some(dictionary_header) => dictionary_header,
            None => {
                return Err(BoltReaderError::FixedLengthDictionaryPageError(
                    String::from("Error when reading Dictionary Page Header"),
                ))
            }
        };

        let num_values: usize = header.num_values as usize;
        let encoding = header.encoding;

        if encoding != parquet_metadata_thrift::Encoding::PLAIN_DICTIONARY
            && encoding != parquet_metadata_thrift::Encoding::PLAIN
        {
            return Err(BoltReaderError::FixedLengthDictionaryPageError(
                String::from("Dictionary Page Encoding should by either PLAIN or PLAIN_DICTIONARY"),
            ));
        }

        if buffer.len() < (num_values) * type_size {
            return Err(BoltReaderError::FixedLengthDictionaryPageError(
                String::from("Corrupted Dictionary Page"),
            ));
        }

        let dictionary_size: usize = num_values * type_size;

        let mut zero_copy = false;
        let vec = if buffer.can_create_buffer_slice(buffer.get_rpos(), dictionary_size) {
            zero_copy = true;
            buffer.load_bytes_to_byte_vec(buffer.get_rpos(), dictionary_size)?
        } else {
            buffer.load_bytes_to_byte_vec_deep_copy(buffer.get_rpos(), dictionary_size)?
        };

        let dictionary_slice = &vec;

        let mut dictionary: Vec<DictionaryValue<T>> = Vec::with_capacity(num_values);

        unsafe {
            let vec_t = convert_generic_vec!(dictionary_slice, mem::size_of::<T>(), T);

            if TypeId::of::<T>() == TypeId::of::<i64>() {
                let vec_i64 = convert_generic_vec!(dictionary_slice, mem::size_of::<i64>(), i64);
                for i in 0..num_values {
                    dictionary.push(DictionaryValue::new(filter.check_i64(vec_i64[i]), vec_t[i]));
                }
                mem::forget(vec_i64);
            } else if TypeId::of::<T>() == TypeId::of::<i32>() {
                let vec_i32 = convert_generic_vec!(dictionary_slice, mem::size_of::<i32>(), i32);
                for i in 0..num_values {
                    dictionary.push(DictionaryValue::new(filter.check_i32(vec_i32[i]), vec_t[i]));
                }
                mem::forget(vec_i32);
            } else if TypeId::of::<T>() == TypeId::of::<f64>() {
                let vec_f64 = convert_generic_vec!(dictionary_slice, mem::size_of::<f64>(), f64);
                for i in 0..num_values {
                    dictionary.push(DictionaryValue::new(filter.check_f64(vec_f64[i]), vec_t[i]));
                }
                mem::forget(vec_f64);
            } else if TypeId::of::<T>() == TypeId::of::<f32>() {
                let vec_f32 = convert_generic_vec!(dictionary_slice, mem::size_of::<f32>(), f32);
                for i in 0..num_values {
                    dictionary.push(DictionaryValue::new(filter.check_f32(vec_f32[i]), vec_t[i]));
                }
                mem::forget(vec_f32);
            } else {
                return Err(BoltReaderError::FixedLengthDictionaryPageError(format!(
                    "Parquet Fixed Length Dictionary Page unsupported type: {}",
                    std::any::type_name::<T>()
                )));
            }

            mem::forget(vec_t);
        }

        buffer.set_rpos(buffer.get_rpos() + dictionary_size);
        if zero_copy {
            mem::forget(vec);
        }

        Ok(FixedLengthDictionaryWithFilter {
            dictionary,
            num_values,
            sorted: header.is_sorted.unwrap_or(false),
            type_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
    use crate::filters::integer_range_filter::IntegerRangeFilter;
    use crate::metadata::page_header::read_page_header;
    use crate::page_reader::dictionary_page::fixed_length_dictionary_page_with_filter::{
        DictionaryPage, FixedLengthDictionaryWithFilter,
    };
    use crate::utils::byte_buffer_base::ByteBufferBase;
    use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
    use crate::utils::exceptions::BoltReaderError;
    use crate::utils::file_loader::LoadFile;
    use crate::utils::file_streaming_byte_buffer::{FileStreamingBuffer, StreamingByteBuffer};
    use crate::utils::local_file_loader::LocalFileLoader;

    fn load_parquet_first_dictionary_page<T: std::marker::Copy + 'static>(
        path: String,
        filter: &dyn FixedLengthRangeFilter,
    ) -> (
        Result<FixedLengthDictionaryWithFilter<T>, BoltReaderError>,
        DirectByteBuffer,
    ) {
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();

        let res = DirectByteBuffer::from_file(&file, 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();

        (
            FixedLengthDictionaryWithFilter::new(
                &page_header,
                &mut buf,
                mem::size_of::<T>(),
                filter,
            ),
            buf,
        )
    }

    fn load_parquet_first_dictionary_page_streaming_buffer<'a, T: std::marker::Copy + 'static>(
        file: &'a (dyn LoadFile + 'a),
        filter: &dyn FixedLengthRangeFilter,
        buffer_size: usize,
    ) -> (
        Result<FixedLengthDictionaryWithFilter<T>, BoltReaderError>,
        StreamingByteBuffer<'a>,
    ) {
        let res = StreamingByteBuffer::from_file(file, 4, file.get_file_size() - 4, buffer_size);

        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();

        (
            FixedLengthDictionaryWithFilter::new(
                &page_header,
                &mut buf,
                mem::size_of::<T>(),
                filter,
            ),
            buf,
        )
    }

    #[test]
    fn test_load_dictionary_page_with_filter() {
        let path = String::from("src/sample_files/lineitem_dictionary.parquet");
        let filter = IntegerRangeFilter::new(54914, 54930, true);

        let (dic, buffer): (Result<FixedLengthDictionaryWithFilter<i64>, _>, _) =
            load_parquet_first_dictionary_page(path, &filter);

        assert!(dic.is_ok());
        let dictionary = dic.unwrap();

        assert_eq!(dictionary.get_type_size(), 8);

        assert_eq!(dictionary.find(0), 429);
        assert_eq!(dictionary.find(1), 54914);
        assert_eq!(dictionary.find(2), 54915);
        assert_eq!(dictionary.find(3), 54916);
        assert_eq!(dictionary.find(4), 54917);

        assert_eq!(dictionary.validate(0), false);
        assert_eq!(dictionary.validate(1), true);
        assert_eq!(dictionary.validate(2), true);
        assert_eq!(dictionary.validate(3), true);
        assert_eq!(dictionary.validate(4), true);

        assert_eq!(dictionary.get_num_values(), 2921);

        assert_eq!(buffer.get_rpos(), 19 + 2921 * 8);
    }

    #[test]
    fn test_load_dictionary_page_with_filter_in_streaming_buffer() {
        let path = String::from("src/sample_files/lineitem_dictionary.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        let filter = IntegerRangeFilter::new(54914, 54930, true);

        for i in 0..16 {
            let buffer_size = 1 << i;
            let (dic, buffer): (Result<FixedLengthDictionaryWithFilter<i64>, _>, _) =
                load_parquet_first_dictionary_page_streaming_buffer(&file, &filter, buffer_size);
            assert!(dic.is_ok());
            let dictionary = dic.unwrap();

            assert_eq!(dictionary.get_type_size(), 8);

            assert_eq!(dictionary.find(0), 429);
            assert_eq!(dictionary.find(1), 54914);
            assert_eq!(dictionary.find(2), 54915);
            assert_eq!(dictionary.find(3), 54916);
            assert_eq!(dictionary.find(4), 54917);

            assert_eq!(dictionary.validate(0), false);
            assert_eq!(dictionary.validate(1), true);
            assert_eq!(dictionary.validate(2), true);
            assert_eq!(dictionary.validate(3), true);
            assert_eq!(dictionary.validate(4), true);

            assert_eq!(dictionary.get_num_values(), 2921);

            assert_eq!(buffer.get_rpos(), 19 + 2921 * 8);
        }
    }

    #[test]
    fn test_load_incorrect_dictionary_page() {
        let path = String::from("src/sample_files/lineitem.parquet");
        let filter = IntegerRangeFilter::new(54914, 54930, true);

        let (dic, _buffer): (Result<FixedLengthDictionaryWithFilter<i64>, _>, _) =
            load_parquet_first_dictionary_page(path, &filter);

        assert!(dic.is_err());
    }

    #[test]
    fn test_load_incorrect_dictionary_page_in_streaming_buffer() {
        let path = String::from("src/sample_files/lineitem.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        let filter = IntegerRangeFilter::new(54914, 54930, true);

        for i in 0..16 {
            let buffer_size = 1 << i;
            let (dic, _buffer): (Result<FixedLengthDictionaryWithFilter<i64>, _>, _) =
                load_parquet_first_dictionary_page_streaming_buffer(&file, &filter, buffer_size);
            assert!(dic.is_err());
        }
    }
}
