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
use crate::page_reader::dictionary_page::dictionary_page_base::DictionaryPage;
use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
use crate::utils::exceptions::BoltReaderError;

pub struct FixedLengthDictionary<T> {
    dictionary: Vec<T>,
    num_values: usize,
    sorted: bool,
    type_size: usize,
    #[allow(dead_code)]
    zero_copy: bool,
}

impl<T> Drop for FixedLengthDictionary<T> {
    fn drop(&mut self) {
        let data = mem::take(&mut self.dictionary);
        if self.is_zero_copied() {
            mem::forget(data);
        }
    }
}

#[allow(dead_code)]
impl<T: ToString> std::fmt::Display for FixedLengthDictionary<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut i = 0;
        let mut dict_str = String::new();
        while i < min(10, self.num_values) {
            dict_str += &(self.dictionary[i].to_string() + " ");
            i += 1;
        }

        writeln!(
            f,
            "Dictionary page: num_values {}, sorted {}\nDictionary: {}...",
            self.num_values, self.sorted, dict_str
        )
    }
}

impl<T> DictionaryPage<T> for FixedLengthDictionary<T> {
    #[inline(always)]
    fn validate(&self, _index: usize) -> bool {
        true
    }

    #[inline(always)]
    fn find(&self, index: usize) -> &T {
        &self.dictionary[index]
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

impl<T> FixedLengthDictionary<T> {
    pub fn new(
        page_header: &PageHeader,
        buffer: &mut dyn ByteBufferBase,
        type_size: usize,
    ) -> Result<FixedLengthDictionary<T>, BoltReaderError> {
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

        let dictionary_size: usize = (num_values) * type_size;

        let mut zero_copy = false;
        let dictionary: Vec<T> =
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

        Ok(FixedLengthDictionary {
            dictionary,
            num_values,
            sorted: header.is_sorted.unwrap_or(false),
            type_size,
            zero_copy,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::rc::Rc;

    use crate::metadata::page_header::read_page_header;
    use crate::page_reader::dictionary_page::fixed_length_dictionary_page::{
        DictionaryPage, FixedLengthDictionary,
    };
    use crate::utils::byte_buffer_base::ByteBufferBase;
    use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
    use crate::utils::exceptions::BoltReaderError;
    use crate::utils::file_loader::{FileLoader, FileLoaderEnum};
    use crate::utils::file_streaming_byte_buffer::{FileStreamingBuffer, StreamingByteBuffer};
    use crate::utils::local_file_loader::LocalFileLoader;

    fn load_parquet_first_dictionary_page<T>(
        path: &String,
    ) -> (
        Result<FixedLengthDictionary<T>, BoltReaderError>,
        DirectByteBuffer,
    ) {
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));

        let res = DirectByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4);

        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();

        (
            FixedLengthDictionary::new(&page_header, &mut buf, mem::size_of::<T>()),
            buf,
        )
    }

    fn load_parquet_first_dictionary_page_streaming_buffer<'a, T>(
        file: Rc<FileLoaderEnum>,
        buffer_size: usize,
    ) -> (
        Result<FixedLengthDictionary<T>, BoltReaderError>,
        StreamingByteBuffer,
    ) {
        let res =
            StreamingByteBuffer::from_file(file.clone(), 4, file.get_file_size() - 4, buffer_size);

        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();

        (
            FixedLengthDictionary::new(&page_header, &mut buf, mem::size_of::<T>()),
            buf,
        )
    }
    #[test]
    fn test_create_and_drop_dictionary_page() {
        let path = String::from("src/sample_files/lineitem_dictionary.parquet");
        let (dic, _buffer): (Result<FixedLengthDictionary<i64>, _>, _) =
            load_parquet_first_dictionary_page(&path);
        assert!(dic.is_ok());
        let dictionary_zero_copy = dic.unwrap();

        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));
        let (dic, _buffer): (Result<FixedLengthDictionary<i64>, _>, _) =
            load_parquet_first_dictionary_page_streaming_buffer(file.clone(), 16);
        assert!(dic.is_ok());
        let dictionary_deep_copy = dic.unwrap();

        assert_eq!(dictionary_zero_copy.is_zero_copied(), true);
        assert_eq!(dictionary_deep_copy.is_zero_copied(), false);

        // Both deep copy and zero dictionary page should be safely release at this point.
    }

    #[test]
    fn test_load_dictionary_page() {
        let path = String::from("src/sample_files/lineitem_dictionary.parquet");
        let (dic, buffer): (Result<FixedLengthDictionary<i64>, _>, _) =
            load_parquet_first_dictionary_page(&path);
        assert!(dic.is_ok());
        let dictionary = dic.unwrap();

        assert_eq!(dictionary.get_type_size(), 8);

        assert_eq!(*dictionary.find(0), 429);
        assert_eq!(*dictionary.find(1), 54914);
        assert_eq!(*dictionary.find(2), 54915);
        assert_eq!(*dictionary.find(3), 54916);
        assert_eq!(*dictionary.find(4), 54917);

        assert_eq!(dictionary.validate(0), true);
        assert_eq!(dictionary.validate(1), true);
        assert_eq!(dictionary.validate(2), true);
        assert_eq!(dictionary.validate(3), true);
        assert_eq!(dictionary.validate(4), true);

        assert_eq!(dictionary.get_num_values(), 2921);

        assert_eq!(buffer.get_rpos(), 19 + 2921 * 8);
    }

    #[test]
    fn test_load_dictionary_page_in_streaming_buffer() {
        let path = String::from("src/sample_files/lineitem_dictionary.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));

        for i in 0..16 {
            let buffer_size = 1 << i;
            let (dic, buffer): (Result<FixedLengthDictionary<i64>, _>, _) =
                load_parquet_first_dictionary_page_streaming_buffer(file.clone(), buffer_size);
            assert!(dic.is_ok());
            let dictionary = dic.unwrap();
            assert_eq!(dictionary.get_type_size(), 8);

            assert_eq!(*dictionary.find(0), 429);
            assert_eq!(*dictionary.find(1), 54914);
            assert_eq!(*dictionary.find(2), 54915);
            assert_eq!(*dictionary.find(3), 54916);
            assert_eq!(*dictionary.find(4), 54917);

            assert_eq!(dictionary.validate(0), true);
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
        let (dic, _buffer): (Result<FixedLengthDictionary<i64>, _>, _) =
            load_parquet_first_dictionary_page(&path);

        assert!(dic.is_err());
    }

    #[test]
    fn test_load_incorrect_dictionary_page_in_streaming_buffer() {
        let path = String::from("src/sample_files/lineitem.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = Rc::from(FileLoaderEnum::LocalFileLoader(res.unwrap()));

        for i in 0..16 {
            let buffer_size = 1 << i;
            let (dic, _buffer): (Result<FixedLengthDictionary<i64>, _>, _) =
                load_parquet_first_dictionary_page_streaming_buffer(file.clone(), buffer_size);
            assert!(dic.is_err());
        }
    }
}
