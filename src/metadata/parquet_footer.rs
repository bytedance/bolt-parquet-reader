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

use thrift::protocol::{TCompactInputProtocol, TSerializable};

use crate::metadata::parquet_metadata_thrift::FileMetaData;
use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
use crate::utils::exceptions::BoltReaderError;
use crate::utils::file_loader::LoadFile;
use crate::utils::local_file_loader::LocalFileLoader;

const PARQUET_MAGIC_CODE: [u8; 4] = [b'P', b'A', b'R', b'1'];
const PARQUET_MAGIC_CODE_LENGTH: usize = 4;
const PARQUET_FOOTER_BUFFER_LENGTH: usize = 4;

// Currently, FileMetaDataLoader only has the LocalFileLoader member.
// TODO: Add cached file footer
pub struct FileMetaDataLoader {
    file: LocalFileLoader,
    footer_preload_size: usize,
    actual_footer_size: Option<usize>,
}

#[allow(dead_code)]
impl std::fmt::Display for FileMetaDataLoader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "File Metadata Loader, path: {}",
            self.file.get_file_path(),
        )
    }
}

impl FileMetaDataLoader {
    pub fn new(
        path: &String,
        footer_preload_size: usize,
    ) -> Result<FileMetaDataLoader, BoltReaderError> {
        let file = LocalFileLoader::new(path)?;
        FileMetaDataLoader::from(file, footer_preload_size)
    }

    pub fn from(
        file: LocalFileLoader,
        footer_preload_size: usize,
    ) -> Result<FileMetaDataLoader, BoltReaderError> {
        if footer_preload_size <= 8 {
            return Err(BoltReaderError::MetadataError(format!(
                "Footer Preload Size is {} bytes, should be large than 8 bytes.",
                footer_preload_size
            )));
        }

        Ok(FileMetaDataLoader {
            file,
            footer_preload_size,
            actual_footer_size: None,
        })
    }

    fn verify_footer_magic(
        &self,
        buffer: &DirectByteBuffer,
        offset: usize,
    ) -> Result<(), BoltReaderError> {
        let magic = buffer.create_buffer_slice(offset, 4)?;
        if magic != PARQUET_MAGIC_CODE {
            return Err(BoltReaderError::FileFormatError(String::from(
                "Not a Parquet File. Missing footer Magic code: PAR1",
            )));
        }
        Ok(())
    }

    fn read_footer_size(
        &self,
        buffer: &DirectByteBuffer,
        offset: usize,
    ) -> Result<usize, BoltReaderError> {
        let length_buffer: Result<[u8; 4], _> = buffer.create_buffer_slice(offset, 4)?.try_into();
        match length_buffer {
            Ok(val) => Ok(u32::from_le_bytes(val) as usize),
            Err(_err) => Err(BoltReaderError::MetadataError(String::from(
                "Unable to parse footer length",
            ))),
        }
    }

    fn translate_thrift_footer_metadata(
        &self,
        buf: &[u8],
    ) -> Result<FileMetaData, BoltReaderError> {
        let mut protocol = TCompactInputProtocol::new(buf);
        let t_file_metadata: FileMetaData = FileMetaData::read_from_in_protocol(&mut protocol)
            .map_err(|_err| {
                BoltReaderError::MetadataError(String::from(
                    "Unable to translate Thrift Parquet Footer",
                ))
            })?;
        Ok(t_file_metadata)
    }

    pub fn get_parquet_footer_size(&self) -> Result<usize, BoltReaderError> {
        match self.actual_footer_size {
            None => Err(BoltReaderError::MetadataError(String::from(
                "Parquet footer is not parsed yet. Please call load_parquet_footer() first",
            ))),
            Some(footer_size) => Ok(footer_size),
        }
    }

    pub fn load_parquet_footer(&mut self) -> Result<FileMetaData, BoltReaderError> {
        let size = self.file.get_file_size();
        let preload_size = min(size, self.footer_preload_size);

        let mut buffer =
            DirectByteBuffer::from_file(&self.file, size - preload_size, preload_size)?;

        self.verify_footer_magic(&buffer, preload_size - PARQUET_MAGIC_CODE_LENGTH)?;
        let actual_footer_size = self.read_footer_size(
            &buffer,
            preload_size - PARQUET_FOOTER_BUFFER_LENGTH - PARQUET_MAGIC_CODE_LENGTH,
        )?;

        if self.actual_footer_size.is_none() {
            self.actual_footer_size = Some(actual_footer_size)
        }

        let mut footer_offset = 0;
        if actual_footer_size
            > preload_size - PARQUET_FOOTER_BUFFER_LENGTH - PARQUET_MAGIC_CODE_LENGTH
        {
            buffer = DirectByteBuffer::from_file(
                &self.file,
                size - actual_footer_size
                    - PARQUET_FOOTER_BUFFER_LENGTH
                    - PARQUET_MAGIC_CODE_LENGTH,
                actual_footer_size,
            )?;
        } else {
            footer_offset = preload_size
                - actual_footer_size
                - PARQUET_FOOTER_BUFFER_LENGTH
                - PARQUET_MAGIC_CODE_LENGTH;
        }
        let footer_slice = buffer.create_buffer_slice(footer_offset, actual_footer_size)?;
        self.translate_thrift_footer_metadata(footer_slice)
    }
}

#[cfg(test)]
mod tests {
    use std::string::String;

    use crate::metadata::parquet_footer::FileMetaDataLoader;

    const DEFAULT_PARQUET_FOOTER_PRELOAD_SIZE: usize = 1 << 20;

    #[test]
    fn test_load_parquet_footer() {
        let file = String::from("src/sample_files/lineitem.parquet");
        let metadata_loader =
            FileMetaDataLoader::new(&String::from(&file), DEFAULT_PARQUET_FOOTER_PRELOAD_SIZE);
        assert!(metadata_loader.is_ok());
        let mut metadata_loader = metadata_loader.unwrap();
        let res = metadata_loader.load_parquet_footer();
        assert!(res.is_ok());

        let small_preload_metadata_loader = FileMetaDataLoader::new(&String::from(&file), 9);
        assert!(small_preload_metadata_loader.is_ok());
        let mut small_preload_metadata_loader = small_preload_metadata_loader.unwrap();
        let res = small_preload_metadata_loader.load_parquet_footer();
        assert!(res.is_ok());
    }

    #[test]
    fn test_too_small_preload_size() {
        let file = String::from("src/sample_files/lineitem.parquet");
        let metadata_loader = FileMetaDataLoader::new(&String::from(&file), 8);
        assert!(metadata_loader.is_err());
    }

    #[test]
    fn test_load_from_nonexistent_file() {
        let file = String::from("not_existing_file");

        let metadata_loader =
            FileMetaDataLoader::new(&String::from(file), DEFAULT_PARQUET_FOOTER_PRELOAD_SIZE);
        assert!(metadata_loader.is_err());
    }

    #[test]
    fn test_parquet_footer_size() {
        let file = String::from("src/sample_files/lineitem.parquet");
        let metadata_loader =
            FileMetaDataLoader::new(&String::from(&file), DEFAULT_PARQUET_FOOTER_PRELOAD_SIZE);
        assert!(metadata_loader.is_ok());

        let mut metadata_loader = metadata_loader.unwrap();
        let footer_size = metadata_loader.get_parquet_footer_size();
        assert!(footer_size.is_err());

        let res = metadata_loader.load_parquet_footer();
        assert!(res.is_ok());

        let footer_size = metadata_loader.get_parquet_footer_size();
        assert!(footer_size.is_ok());
        assert_eq!(footer_size.unwrap(), 11135);
    }
}
