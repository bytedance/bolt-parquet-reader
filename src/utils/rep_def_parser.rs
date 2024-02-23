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

use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::encoding::rle_bp::RleBpDecoder;
use crate::utils::exceptions::BoltReaderError;

/// This implementation is based on Rust Arrow2 Parquet reader's RLE/BP Decoder
///
/// TODO: 1. Refactor the Repetition and Definition Level parser when the native RLE/BP Decoder is finished.
/// TODO: 2. Currently, it only supports primitive type columns. Add nested type support in the future.
pub struct RepDefParser {}

impl RepDefParser {
    pub fn parse_rep_def(
        buf: &mut dyn ByteBufferBase,
        num_values: usize,
        max_rep: u32,
        rep_rle_bp: bool,
        max_def: u32,
        def_rle_bp: bool,
    ) -> Result<(bool, Option<Vec<bool>>), BoltReaderError> {
        if Self::is_top_level(max_rep, max_def) {
            return Self::parse_top_level(
                buf, num_values, max_rep, rep_rle_bp, max_def, def_rle_bp,
            );
        }

        Err(BoltReaderError::NotYetImplementedError(String::from(
            "Not yet implemented: Parsing Rep Def for nested types",
        )))
    }

    #[inline(always)]
    pub fn is_top_level(max_rep: u32, max_def: u32) -> bool {
        max_def <= 1 && max_rep == 0
    }

    /// Top level represents the non-complex types. Repetition Level is not required.
    ///
    /// Return (has_null, Option<Validity Vec>)
    pub fn parse_top_level(
        buf: &mut dyn ByteBufferBase,
        num_values: usize,
        max_rep: u32,
        rep_rle_bp: bool,
        max_def: u32,
        def_rle_bp: bool,
    ) -> Result<(bool, Option<Vec<bool>>), BoltReaderError> {
        if unlikely(max_def > 1 || max_rep != 0) {
            return Err(BoltReaderError::RepDefError(format!(
                "Incorrect max definition and repetition level. max_def: {}, max_rep: {}",
                max_def, max_rep
            )));
        }

        if unlikely(!rep_rle_bp || !def_rle_bp) {
            return Err(BoltReaderError::RepDefError(String::from(
                "The Repetition and Definition Level data should be RLE/BP encoded",
            )));
        }

        let may_have_null = Self::top_level_may_have_null_fast_path(buf, max_def)?;
        if !may_have_null {
            return Ok((false, None));
        }

        let mut has_null = false;

        let result: Vec<bool> = Self::load_level(buf, max_def)?
            .iter()
            .take(num_values)
            .map(|&x| {
                has_null |= x != 1;
                x == 1
            })
            .collect();

        if !has_null {
            return Ok((false, None));
        }

        Ok((has_null, Option::Some(result)))
    }

    pub fn top_level_may_have_null_fast_path(
        buf: &mut dyn ByteBufferBase,
        max_value: u32,
    ) -> Result<bool, BoltReaderError> {
        let length = buf.read_u32()?;
        let (_, header_length, is_bit_packing) = RleBpDecoder::read_header(buf)?;
        let bit_width = RleBpDecoder::get_minimum_required_bits(max_value);

        let res = if !is_bit_packing && bit_width + header_length as u32 == length && bit_width == 1
        {
            buf.set_rpos(buf.get_rpos() + header_length);
            buf.read_u8()? != 1
        } else {
            buf.set_rpos(buf.get_rpos() - 4);
            return Ok(true);
        };

        buf.set_rpos(buf.get_rpos() - bit_width as usize - header_length - 4);
        Ok(res)
    }

    pub fn load_level(
        buf: &mut dyn ByteBufferBase,
        max_value: u32,
    ) -> Result<Vec<u32>, BoltReaderError> {
        let length = buf.read_u32()? as usize;
        let bit_width = RleBpDecoder::get_minimum_required_bits(max_value);

        let mut res: Vec<u32> = vec![];
        let mut bytes_read = 0;

        while bytes_read < length {
            let rpos = buf.get_rpos();
            res.append(&mut RleBpDecoder::decode(buf, bit_width as usize)?);
            bytes_read += buf.get_rpos() - rpos;
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {

    use crate::metadata::page_header::read_page_header;
    use crate::metadata::parquet_metadata_thrift::Encoding;
    use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
    use crate::utils::file_loader::LoadFile;
    use crate::utils::file_streaming_byte_buffer::{FileStreamingBuffer, StreamingByteBuffer};
    use crate::utils::local_file_loader::LocalFileLoader;
    use crate::utils::rep_def_parser::RepDefParser;

    #[test]
    fn test_top_level_no_null() {
        let vec = vec![0, 0, 0, 2, 8, 1];
        let mut buf = DirectByteBuffer::from_vec(vec);
        let result = RepDefParser::parse_rep_def(&mut buf, 4, 0, true, 1, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().0, false);
    }

    #[test]
    fn test_top_level_no_null_not_consecutive() {
        let vec = vec![0, 0, 0, 4, 8, 1, 12, 1];
        let mut buf = DirectByteBuffer::from_vec(vec);
        let result = RepDefParser::parse_rep_def(&mut buf, 10, 0, true, 1, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().0, false);
    }

    #[test]
    fn test_top_level_has_null() {
        let expected = vec![false, false, false, false, true, true];
        let vec = vec![0, 0, 0, 4, 8, 0, 4, 1];
        let mut buf = DirectByteBuffer::from_vec(vec);
        let result = RepDefParser::parse_rep_def(&mut buf, 6, 0, true, 1, true);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.0, true);
        assert!(result.1.is_some());
        assert_eq!(result.1.unwrap(), expected);
    }

    #[test]
    fn test_real_non_null_page_header_direct_buffer() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        let res = DirectByteBuffer::from_file(&file, 0, file.get_file_size());

        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();

        let data_page_header = page_header.data_page_header.unwrap();
        let rep_rle_bp = data_page_header.repetition_level_encoding == Encoding::RLE
            || data_page_header.repetition_level_encoding == Encoding::BIT_PACKED;

        let def_rle_bp = data_page_header.definition_level_encoding == Encoding::RLE
            || data_page_header.definition_level_encoding == Encoding::BIT_PACKED;
        let result = RepDefParser::parse_rep_def(
            &mut buf,
            data_page_header.num_values as usize,
            0,
            rep_rle_bp,
            1,
            def_rle_bp,
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.0, false);
        assert!(result.1.is_none());
    }

    #[test]
    fn test_real_non_null_page_header_streaming_buffer() {
        let path = String::from("src/sample_files/linitem_plain_data_page");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        let res = StreamingByteBuffer::from_file(&file, 0, file.get_file_size(), 5);

        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();

        let data_page_header = page_header.data_page_header.unwrap();
        let rep_rle_bp = data_page_header.repetition_level_encoding == Encoding::RLE
            || data_page_header.repetition_level_encoding == Encoding::BIT_PACKED;

        let def_rle_bp = data_page_header.definition_level_encoding == Encoding::RLE
            || data_page_header.definition_level_encoding == Encoding::BIT_PACKED;
        let result = RepDefParser::parse_rep_def(
            &mut buf,
            data_page_header.num_values as usize,
            0,
            rep_rle_bp,
            1,
            def_rle_bp,
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.0, false);
        assert!(result.1.is_none());
    }

    /// This page is manually crafted. Element 0-99: nonnull, Element 100-199: null
    /// Element 200-1200: depending on the index, null if index % 5 == 0 && index % 17 == 0
    #[test]
    fn test_real_nullable_page_header_direct_buffer() {
        let path = String::from("src/sample_files/data_page_with_nulls");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        let res = DirectByteBuffer::from_file(&file, 0, file.get_file_size());

        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();

        let data_page_header = page_header.data_page_header.unwrap();
        let rep_rle_bp = data_page_header.repetition_level_encoding == Encoding::RLE
            || data_page_header.repetition_level_encoding == Encoding::BIT_PACKED;

        let def_rle_bp = data_page_header.definition_level_encoding == Encoding::RLE
            || data_page_header.definition_level_encoding == Encoding::BIT_PACKED;
        let result = RepDefParser::parse_rep_def(
            &mut buf,
            data_page_header.num_values as usize,
            0,
            rep_rle_bp,
            1,
            def_rle_bp,
        );
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.0, true);
        assert!(result.1.is_some());
        let validity = result.1.unwrap();

        for i in 0..100 {
            assert_eq!(validity[i], true);
        }

        for i in 100..200 {
            assert_eq!(validity[i], false);
        }

        for i in 200..validity.len() {
            if i % 5 == 0 || i % 17 == 0 {
                assert_eq!(validity[i], false);
            } else {
                assert_eq!(validity[i], true);
            }
        }
    }

    #[test]
    fn test_real_nullable_page_header_streaming_buffer() {
        let path = String::from("src/sample_files/data_page_with_nulls");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        let res = StreamingByteBuffer::from_file(&file, 0, file.get_file_size(), 5);

        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
        let page_header = page_header.unwrap();

        let data_page_header = page_header.data_page_header.unwrap();
        let rep_rle_bp = data_page_header.repetition_level_encoding == Encoding::RLE
            || data_page_header.repetition_level_encoding == Encoding::BIT_PACKED;

        let def_rle_bp = data_page_header.definition_level_encoding == Encoding::RLE
            || data_page_header.definition_level_encoding == Encoding::BIT_PACKED;
        let result = RepDefParser::parse_rep_def(
            &mut buf,
            data_page_header.num_values as usize,
            0,
            rep_rle_bp,
            1,
            def_rle_bp,
        );
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.0, true);
        assert!(result.1.is_some());
        let validity = result.1.unwrap();

        for i in 0..100 {
            assert_eq!(validity[i], true);
        }

        for i in 100..200 {
            assert_eq!(validity[i], false);
        }

        for i in 200..validity.len() {
            if i % 5 == 0 || i % 17 == 0 {
                assert_eq!(validity[i], false);
            } else {
                assert_eq!(validity[i], true);
            }
        }
    }
}
