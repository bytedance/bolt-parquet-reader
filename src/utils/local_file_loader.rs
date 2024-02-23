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
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use bytebuffer::Endian::LittleEndian;

use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
use crate::utils::exceptions::BoltReaderError;
use crate::utils::file_loader::LoadFile;

pub struct LocalFileLoader {
    path: String,
    size: usize,
}

#[allow(dead_code)]
impl std::fmt::Display for LocalFileLoader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Local File Loader, path: {}, size: {}",
            self.path, self.size
        )
    }
}

impl LoadFile for LocalFileLoader {
    fn get_file_path(&self) -> &String {
        &self.path
    }

    fn get_file_size(&self) -> usize {
        self.size
    }

    fn load_file_to_buffer(
        &self,
        offset: usize,
        length: usize,
    ) -> Result<DirectByteBuffer, BoltReaderError> {
        if offset + length > self.size {
            return Err(BoltReaderError::InternalError(format!(
                "Reading range exceeds the file size: {} bytes. \nReading range [{}, {}) ",
                self.size,
                offset,
                offset + length
            )));
        }
        let mut file = File::open(self.path.clone())?;
        file.seek(SeekFrom::Start(offset as u64))?;
        let mut v = DirectByteBuffer::allocate_vec_for_buffer(length)?;
        file.read_exact(&mut v)?;
        let mut buffer = DirectByteBuffer::from_vec(v);
        buffer.set_endian(LittleEndian);

        Ok(buffer)
    }
}

#[allow(dead_code)]
impl LocalFileLoader {
    pub fn new(path: &String) -> Result<LocalFileLoader, BoltReaderError> {
        let file = File::open(path)?;
        Ok(LocalFileLoader {
            path: path.clone(),
            size: file.metadata()?.len() as usize,
        })
    }
}

#[cfg(test)]
mod tests {
    extern crate bytebuffer;

    use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
    use crate::utils::file_loader::LoadFile;
    use crate::utils::local_file_loader::LocalFileLoader;

    #[test]
    fn test_create_local_file_loader() {
        let path = String::from("not_existing_file");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_err());

        let path = String::from("src/sample_files/lineitem.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        assert_eq!(file.get_file_size(), 2065982);
        assert_eq!(file.get_file_path(), "src/sample_files/lineitem.parquet");

        let res = DirectByteBuffer::from_file(&file, 0, 4);
        assert!(res.is_ok());
        let buffer = res.unwrap();
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.as_bytes(), vec![80, 65, 82, 49]);

        let res = DirectByteBuffer::from_file(&file, file.get_file_size() - 4, 4);
        assert!(res.is_ok());
        let buffer = res.unwrap();
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.as_bytes(), vec![80, 65, 82, 49]);
    }

    #[test]
    fn test_loading_local_file_loader() {
        let path = String::from("src/sample_files/lineitem.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        let res = DirectByteBuffer::from_file(&file, 0, 4);

        assert!(res.is_ok());
        let buffer = res.unwrap();
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.as_bytes(), vec![80, 65, 82, 49]);

        let res = DirectByteBuffer::from_file(&file, file.get_file_size() - 4, 4);
        assert!(res.is_ok());
        let buffer = res.unwrap();
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.as_bytes(), vec![80, 65, 82, 49]);

        let res = DirectByteBuffer::from_file(&file, file.get_file_size(), 1);
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "Internal Error: Reading range exceeds the file size: 2065982 bytes. \nReading range [2065982, 2065983) \n");
    }
}
