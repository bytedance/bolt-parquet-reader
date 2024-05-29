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

use crate::utils::direct_byte_buffer::DirectByteBuffer;
use crate::utils::exceptions::BoltReaderError;
use crate::utils::local_file_loader::LocalFileLoader;

pub enum FileLoaderEnum {
    LocalFileLoader(LocalFileLoader),
}

pub trait FileLoader {
    fn get_file_path(&self) -> &String;

    fn get_file_size(&self) -> usize;

    fn load_file_to_raw_buffer(
        &self,
        offset: usize,
        length: usize,
    ) -> Result<Vec<u8>, BoltReaderError>;

    fn load_file_to_buffer(
        &self,
        offset: usize,
        length: usize,
    ) -> Result<DirectByteBuffer, BoltReaderError>;
}

impl FileLoader for FileLoaderEnum {
    fn get_file_path(&self) -> &String {
        match self {
            FileLoaderEnum::LocalFileLoader(loader) => loader.get_file_path(),
        }
    }

    fn get_file_size(&self) -> usize {
        match self {
            FileLoaderEnum::LocalFileLoader(loader) => loader.get_file_size(),
        }
    }

    fn load_file_to_raw_buffer(
        &self,
        offset: usize,
        length: usize,
    ) -> Result<Vec<u8>, BoltReaderError> {
        match self {
            FileLoaderEnum::LocalFileLoader(loader) => {
                loader.load_file_to_raw_buffer(offset, length)
            }
        }
    }

    fn load_file_to_buffer(
        &self,
        offset: usize,
        length: usize,
    ) -> Result<DirectByteBuffer, BoltReaderError> {
        match self {
            FileLoaderEnum::LocalFileLoader(loader) => loader.load_file_to_buffer(offset, length),
        }
    }
}
