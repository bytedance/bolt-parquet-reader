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

use std::mem;

use crate::convert_generic_vec;
use crate::utils::direct_byte_buffer::{ByteBufferSlice, DirectByteBuffer};
use crate::utils::exceptions::BoltReaderError;

pub trait ByteBufferBase: std::io::Read {
    fn can_create_buffer_slice(&self, start: usize, len: usize) -> bool;

    fn create_buffer_slice(
        &self,
        start: usize,
        len: usize,
    ) -> Result<ByteBufferSlice<'_>, BoltReaderError>;

    fn get_direct_byte_buffer(&mut self) -> &mut DirectByteBuffer;

    fn get_rpos(&self) -> usize;

    fn set_rpos(&mut self, pos: usize);

    fn read_u8(&mut self) -> Result<u8, BoltReaderError>;

    fn read_u32(&mut self) -> Result<u32, BoltReaderError>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn load_bytes_to_byte_vec(
        &self,
        start: usize,
        length: usize,
    ) -> Result<Vec<u8>, BoltReaderError> {
        let slice = self.create_buffer_slice(start, length)?;
        let data = unsafe { convert_generic_vec!(slice, mem::size_of::<u8>(), u8) };

        Ok(data)
    }

    fn load_bytes_to_byte_vec_deep_copy(
        &mut self,
        start: usize,
        length: usize,
    ) -> Result<Vec<u8>, BoltReaderError>;
}
