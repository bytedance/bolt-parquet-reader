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

use std::io::Read;
use std::mem;

use crate::convert_generic_vec;
use crate::utils::direct_byte_buffer::{ByteBufferSlice, DirectByteBuffer};
use crate::utils::exceptions::BoltReaderError;
use crate::utils::file_streaming_byte_buffer::StreamingByteBuffer;

pub enum BufferEnum<'a> {
    DirectByteBuffer(DirectByteBuffer),
    StreamingByteBuffer(StreamingByteBuffer<'a>),
}

impl Read for BufferEnum<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            BufferEnum::DirectByteBuffer(buffer) => buffer.read(buf),
            BufferEnum::StreamingByteBuffer(buffer) => buffer.read(buf),
        }
    }
}

impl ByteBufferBase for BufferEnum<'_> {
    fn can_create_buffer_slice(&self, start: usize, len: usize) -> bool {
        match self {
            BufferEnum::DirectByteBuffer(buffer) => buffer.can_create_buffer_slice(start, len),
            BufferEnum::StreamingByteBuffer(buffer) => buffer.can_create_buffer_slice(start, len),
        }
    }

    fn create_buffer_slice(
        &self,
        start: usize,
        len: usize,
    ) -> Result<ByteBufferSlice<'_>, BoltReaderError> {
        match self {
            BufferEnum::DirectByteBuffer(buffer) => buffer.create_buffer_slice(start, len),
            BufferEnum::StreamingByteBuffer(buffer) => buffer.create_buffer_slice(start, len),
        }
    }

    fn get_direct_byte_buffer(&mut self) -> &mut DirectByteBuffer {
        match self {
            BufferEnum::DirectByteBuffer(buffer) => buffer.get_direct_byte_buffer(),
            BufferEnum::StreamingByteBuffer(buffer) => buffer.get_direct_byte_buffer(),
        }
    }

    fn get_rpos(&self) -> usize {
        match self {
            BufferEnum::DirectByteBuffer(buffer) => buffer.get_rpos(),
            BufferEnum::StreamingByteBuffer(buffer) => buffer.get_rpos(),
        }
    }

    fn set_rpos(&mut self, pos: usize) {
        match self {
            BufferEnum::DirectByteBuffer(buffer) => buffer.set_rpos(pos),
            BufferEnum::StreamingByteBuffer(buffer) => buffer.set_rpos(pos),
        }
    }

    fn read_u8(&mut self) -> Result<u8, BoltReaderError> {
        match self {
            BufferEnum::DirectByteBuffer(buffer) => ByteBufferBase::read_u8(buffer),
            BufferEnum::StreamingByteBuffer(buffer) => buffer.read_u8(),
        }
    }

    fn read_u32(&mut self) -> Result<u32, BoltReaderError> {
        match self {
            BufferEnum::DirectByteBuffer(buffer) => ByteBufferBase::read_u32(buffer),
            BufferEnum::StreamingByteBuffer(buffer) => buffer.read_u32(),
        }
    }

    fn len(&self) -> usize {
        match self {
            BufferEnum::DirectByteBuffer(buffer) => buffer.len(),
            BufferEnum::StreamingByteBuffer(buffer) => buffer.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            BufferEnum::DirectByteBuffer(buffer) => buffer.is_empty(),
            BufferEnum::StreamingByteBuffer(buffer) => buffer.is_empty(),
        }
    }

    fn load_bytes_to_byte_vec(
        &self,
        start: usize,
        length: usize,
    ) -> Result<Vec<u8>, BoltReaderError> {
        match self {
            BufferEnum::DirectByteBuffer(buffer) => buffer.load_bytes_to_byte_vec(start, length),
            BufferEnum::StreamingByteBuffer(buffer) => buffer.load_bytes_to_byte_vec(start, length),
        }
    }

    fn load_bytes_to_byte_vec_deep_copy(
        &mut self,
        start: usize,
        length: usize,
    ) -> Result<Vec<u8>, BoltReaderError> {
        match self {
            BufferEnum::DirectByteBuffer(buffer) => {
                buffer.load_bytes_to_byte_vec_deep_copy(start, length)
            }
            BufferEnum::StreamingByteBuffer(buffer) => {
                buffer.load_bytes_to_byte_vec_deep_copy(start, length)
            }
        }
    }
}

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
