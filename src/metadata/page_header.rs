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

use thrift::protocol::{TCompactInputProtocol, TSerializable};

use crate::metadata::parquet_metadata_thrift::PageHeader;
use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::exceptions::BoltReaderError;

pub fn read_page_header(buffer: &mut dyn ByteBufferBase) -> Result<PageHeader, BoltReaderError> {
    let mut protocol = TCompactInputProtocol::new(buffer);

    let page_header = PageHeader::read_from_in_protocol(&mut protocol);
    match page_header {
        Ok(val) => Ok(val),
        Err(_err) => Err(BoltReaderError::MetadataError(String::from(
            "Unable to load page header",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::metadata::page_header::read_page_header;
    use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
    use crate::utils::file_streaming_byte_buffer::{FileStreamingBuffer, StreamingByteBuffer};
    use crate::utils::local_file_loader::LocalFileLoader;

    #[test]
    fn test_read_page_header() {
        let path = String::from("src/sample_files/lineitem.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();

        let res = DirectByteBuffer::from_file(&file, 4, 100);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());

        let res = StreamingByteBuffer::from_file(&file, 4, 100, 3);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());

        let res = StreamingByteBuffer::from_file(&file, 4, 100, 7);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_ok());
    }

    #[test]
    fn test_read_page_header_insufficient_length() {
        let path = String::from("src/sample_files/lineitem.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();

        let res = DirectByteBuffer::from_file(&file, 4, 50);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_err());

        let res = StreamingByteBuffer::from_file(&file, 4, 50, 3);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_err());

        let res = StreamingByteBuffer::from_file(&file, 4, 50, 7);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_err());
    }

    #[test]
    fn test_read_page_header_at_wrong_offset() {
        let path = String::from("src/sample_files/lineitem.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();

        let res = DirectByteBuffer::from_file(&file, 8, 100);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_err());

        let res = StreamingByteBuffer::from_file(&file, 8, 100, 3);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_err());

        let res = StreamingByteBuffer::from_file(&file, 8, 100, 7);
        assert!(res.is_ok());
        let mut buf = res.unwrap();
        let page_header = read_page_header(&mut buf);
        assert!(page_header.is_err());
    }
}
