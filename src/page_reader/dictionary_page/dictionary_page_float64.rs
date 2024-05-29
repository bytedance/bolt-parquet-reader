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
use std::fmt::Formatter;
use std::mem;

use crate::metadata::parquet_metadata_thrift;
use crate::metadata::parquet_metadata_thrift::PageHeader;
use crate::page_reader::dictionary_page::dictionary_page_base::DictionaryPageNew;
use crate::utils::byte_buffer_base::{BufferEnum, ByteBufferBase};
use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
use crate::utils::exceptions::BoltReaderError;

pub struct DictionaryPageFloat64 {
    num_values: usize,
    sorted: bool,
    type_size: usize,
    #[allow(dead_code)]
    zero_copy: bool,
    #[allow(dead_code)]
    buffer_enum: BufferEnum,
    dictionary: Vec<f64>,
}

impl Drop for DictionaryPageFloat64 {
    fn drop(&mut self) {
        let data = mem::take(&mut self.dictionary);
        if self.is_zero_copied() {
            mem::forget(data);
        }
    }
}

#[allow(dead_code)]
impl std::fmt::Display for DictionaryPageFloat64 {
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

impl DictionaryPageNew for DictionaryPageFloat64 {
    #[inline(always)]
    fn validate(&self, _index: usize) -> bool {
        true
    }

    fn find_float64(&self, index: usize) -> f64 {
        self.dictionary[index]
    }

    fn get_num_values(&self) -> usize {
        self.num_values
    }

    #[inline(always)]
    fn get_type_size(&self) -> usize {
        self.type_size
    }

    fn is_zero_copied(&self) -> bool {
        self.zero_copy
    }
}

impl DictionaryPageFloat64 {
    pub fn new(
        page_header: &PageHeader,
        buffer: &mut dyn ByteBufferBase,
        type_size: usize,
        as_reference: bool,
        mut buffer_enum: BufferEnum,
    ) -> Result<DictionaryPageFloat64, BoltReaderError> {
        let buffer = if as_reference {
            buffer
        } else {
            &mut buffer_enum
        };

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

        let dictionary_size: usize = (num_values) * type_size;

        let mut zero_copy = false;
        let dictionary: Vec<f64> =
            if buffer.can_create_buffer_slice(buffer.get_rpos(), dictionary_size) {
                zero_copy = true;
                DirectByteBuffer::convert_byte_vec(
                    buffer.load_bytes_to_byte_vec(buffer.get_rpos(), dictionary_size)?,
                    type_size,
                )?
            } else {
                DirectByteBuffer::convert_byte_vec(
                    buffer.load_bytes_to_byte_vec_deep_copy(buffer.get_rpos(), dictionary_size)?,
                    type_size,
                )?
            };

        buffer.set_rpos(buffer.get_rpos() + dictionary_size);

        Ok(DictionaryPageFloat64 {
            num_values,
            sorted: header.is_sorted.unwrap_or(false),
            type_size,
            zero_copy,
            buffer_enum,
            dictionary,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use crate::metadata::page_header::read_page_header;
    use crate::page_reader::dictionary_page::dictionary_page_base::DictionaryPageNew;
    use crate::page_reader::dictionary_page::dictionary_page_float64::DictionaryPageFloat64;
    use crate::utils::byte_buffer_base::{BufferEnum, ByteBufferBase};
    use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
    use crate::utils::exceptions::BoltReaderError;
    use crate::utils::file_loader::{FileLoader, FileLoaderEnum};
    use crate::utils::file_streaming_byte_buffer::{FileStreamingBuffer, StreamingByteBuffer};
    use crate::utils::local_file_loader::LocalFileLoader;

    fn load_dictionary_page_float64<'a>(
        buf: &'a mut dyn ByteBufferBase,
    ) -> Result<DictionaryPageFloat64, BoltReaderError> {
        let res = read_page_header(buf);
        assert!(res.is_ok());
        let page_header = res.unwrap();

        DictionaryPageFloat64::new(
            &page_header,
            buf,
            8,
            true,
            BufferEnum::DirectByteBuffer(DirectByteBuffer::from_vec(Vec::new())),
        )
    }

    #[test]
    fn test_create_and_drop_dictionary_page() {
        let path = String::from("src/sample_files/rle_bp_double_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_dictionary_page_float64(&mut buf);

        assert!(res.is_ok());
        let dictionary_page_zero_copy = res.unwrap();

        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_dictionary_page_float64(&mut buf);

        assert!(res.is_ok());
        let dictionary_page_deep_copy = res.unwrap();

        assert_eq!(dictionary_page_zero_copy.is_zero_copied(), true);
        assert_eq!(dictionary_page_deep_copy.is_zero_copied(), false);

        // Both deep copy and zero dictionary page should be safely release at this point.
    }

    #[test]
    fn test_create_and_drop_nullable_dictionary_page() {
        let path = String::from("src/sample_files/rle_bp_double_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_dictionary_page_float64(&mut buf);

        assert!(res.is_ok());
        let dictionary_page_zero_copy = res.unwrap();

        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_dictionary_page_float64(&mut buf);

        assert!(res.is_ok());
        let dictionary_page_deep_copy = res.unwrap();

        assert_eq!(dictionary_page_zero_copy.is_zero_copied(), true);
        assert_eq!(dictionary_page_deep_copy.is_zero_copied(), false);

        // Both deep copy and zero dictionary page should be safely release at this point.
    }

    #[test]
    fn test_load_dictionary_page() {
        let path = String::from("src/sample_files/rle_bp_double_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_dictionary_page_float64(&mut buf);

        assert!(res.is_ok());
        let dictionary = res.unwrap();

        assert_eq!(dictionary.get_type_size(), 8);
        assert_eq!(dictionary.dictionary.len(), 1000);
    }

    #[test]
    fn test_load_dictionary_page_in_streaming_buffer() {
        let path = String::from("src/sample_files/rle_bp_double_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_dictionary_page_float64(&mut buf);

        assert!(res.is_ok());
        let dictionary = res.unwrap();

        assert_eq!(dictionary.get_type_size(), 8);
        assert_eq!(dictionary.dictionary.len(), 1000);
    }

    #[test]
    fn test_load_nullable_dictionary_page() {
        let path = String::from("src/sample_files/rle_bp_double_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_dictionary_page_float64(&mut buf);

        assert!(res.is_ok());
        let dictionary = res.unwrap();

        assert_eq!(dictionary.get_type_size(), 8);
        assert_eq!(dictionary.dictionary.len(), 800);
    }

    #[test]
    fn test_load_nullable_dictionary_page_in_streaming_buffer() {
        let path = String::from("src/sample_files/rle_bp_double_column_with_nulls.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let res = StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, 64);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let res = load_dictionary_page_float64(&mut buf);

        assert!(res.is_ok());
        let dictionary = res.unwrap();

        assert_eq!(dictionary.get_type_size(), 8);
        assert_eq!(dictionary.dictionary.len(), 800);
    }
}
