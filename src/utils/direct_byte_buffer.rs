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

use bytebuffer::ByteBuffer;

use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::exceptions::BoltReaderError;
use crate::utils::file_loader::LoadFile;

// Currently, we use 1GB as the max capacity.
// todo: Create config module to handle the default const values.
const MAX_CAPACITY: usize = 1 << 30;

pub type DirectByteBuffer = ByteBuffer;
pub type ByteBufferSlice<'a> = &'a [u8];

pub trait Buffer {
    fn allocate_vec_for_buffer(capacity: usize) -> Result<Vec<u8>, BoltReaderError>;

    fn from_file(
        source: &dyn LoadFile,
        offset: usize,
        length: usize,
    ) -> Result<DirectByteBuffer, BoltReaderError>;

    fn convert_byte_vec<T>(input: Vec<u8>, type_size: usize) -> Result<Vec<T>, BoltReaderError> {
        if input.len() % type_size != 0 {
            return Err(BoltReaderError::InternalError(format!(
                "Can not convert Vec. Byte vec size: {}, type size: {}.",
                input.len(),
                type_size
            )));
        }

        let vec: Vec<T> = unsafe {
            let ratio = type_size / mem::size_of::<u8>();
            let length = input.len() / ratio;
            let ptr = input.as_ptr() as *mut T;

            Vec::from_raw_parts(ptr, length, length)
        };
        mem::forget(input);

        Ok(vec)
    }
}

impl Buffer for DirectByteBuffer {
    fn allocate_vec_for_buffer(capacity: usize) -> Result<Vec<u8>, BoltReaderError> {
        if capacity > MAX_CAPACITY {
            return Err(BoltReaderError::InsufficientMemoryError(format!(
                "Allocated capacity is over the max direct buffer capacity: {} bytes",
                MAX_CAPACITY
            )));
        }

        Ok(vec![0; capacity])
    }

    fn from_file(
        source: &dyn LoadFile,
        offset: usize,
        length: usize,
    ) -> Result<DirectByteBuffer, BoltReaderError> {
        source.load_file_to_buffer(offset, length)
    }
}

impl ByteBufferBase for DirectByteBuffer {
    #[inline(always)]
    fn can_create_buffer_slice(&self, _start: usize, _len: usize) -> bool {
        true
    }

    fn create_buffer_slice(
        &self,
        start: usize,
        len: usize,
    ) -> Result<ByteBufferSlice<'_>, BoltReaderError> {
        if len == 0 {
            return Err(BoltReaderError::InternalError(String::from(
                "Slice size should not be 0.",
            )));
        }
        let end = start + len;
        if start >= self.len() || end > self.len() {
            return Err(BoltReaderError::InternalError(format!(
                "Slice out of DirectByteBuffer size: {} bytes; Slice: [{}, {})",
                self.len(),
                start,
                end
            )));
        }
        Ok(&self.as_bytes()[start..end])
    }

    fn get_direct_byte_buffer(&mut self) -> &mut DirectByteBuffer {
        self
    }

    #[inline(always)]
    fn get_rpos(&self) -> usize {
        self.get_rpos()
    }

    #[inline(always)]
    fn set_rpos(&mut self, pos: usize) {
        self.set_rpos(pos);
    }

    #[inline(always)]
    fn read_u8(&mut self) -> Result<u8, BoltReaderError> {
        match self.read_u8() {
            Ok(res) => Ok(res),
            Err(_) => Err(BoltReaderError::InternalError(String::from(
                "Can not read_u8()",
            ))),
        }
    }

    #[inline(always)]
    fn read_u32(&mut self) -> Result<u32, BoltReaderError> {
        match self.read_u32() {
            Ok(res) => Ok(res),
            Err(_) => Err(BoltReaderError::InternalError(String::from(
                "Can not read_u32()",
            ))),
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn load_bytes_to_byte_vec_deep_copy(
        &mut self,
        start: usize,
        length: usize,
    ) -> Result<Vec<u8>, BoltReaderError> {
        let slice = self.create_buffer_slice(start, length)?;

        Ok(Vec::from(slice))
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::direct_byte_buffer::*;

    #[test]
    fn test_allocate_byte_buffer() {
        let res = DirectByteBuffer::allocate_vec_for_buffer(10);
        assert!(res.is_ok());
        let mut vec = res.unwrap();
        vec[1] = 1;
        let mut buffer = DirectByteBuffer::from_vec(vec);
        assert_eq!(buffer.len(), 10);

        assert_eq!(buffer.read_u8().unwrap(), 0);
        assert_eq!(buffer.read_u8().unwrap(), 1);

        let res = DirectByteBuffer::allocate_vec_for_buffer(MAX_CAPACITY + 1);
        assert!(res.is_err());
    }

    #[test]
    fn test_direct_buffer_slice() {
        let res = DirectByteBuffer::allocate_vec_for_buffer(10);
        assert!(res.is_ok());
        let mut vec = res.unwrap();
        vec[1] = 1;
        vec[2] = 2;
        vec[3] = 3;
        vec[4] = 4;
        vec[5] = 5;
        vec[6] = 6;
        vec[7] = 7;
        vec[8] = 8;
        vec[9] = 9;
        let buffer = DirectByteBuffer::from_vec(vec);

        let res1 = buffer.create_buffer_slice(1, 2);
        assert!(res1.is_ok());
        let slice1 = res1.unwrap();
        assert_eq!(slice1[0], 1);
        assert_eq!(slice1[1], 2);

        let res2 = buffer.create_buffer_slice(8, 2);
        assert!(res2.is_ok());
        let slice2 = res2.unwrap();
        assert_eq!(slice2[0], 8);
        assert_eq!(slice2[1], 9);

        let res3 = buffer.create_buffer_slice(0, 100);
        assert!(res3.is_err());
    }

    #[test]
    fn test_create_vec_from_zero_copy() {
        let res = DirectByteBuffer::allocate_vec_for_buffer(10);
        assert!(res.is_ok());
        let mut vec = res.unwrap();
        vec[1] = 1;
        vec[2] = 2;
        vec[3] = 3;
        vec[4] = 4;
        vec[5] = 5;
        vec[6] = 6;
        vec[7] = 7;
        vec[8] = 8;
        vec[9] = 9;
        let buffer = DirectByteBuffer::from_vec(vec);

        let byte_vec_res = buffer.load_bytes_to_byte_vec(0, 8);
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let res1 = DirectByteBuffer::convert_byte_vec(byte_vec, 4);
        assert!(res1.is_ok());
        let vec1: Vec<i32> = res1.unwrap();

        assert_eq!(vec1, Vec::from([50462976, 117835012]));
        assert_eq!(buffer.as_bytes().as_ptr(), vec1.as_ptr() as *mut u8);

        let byte_vec_res = buffer.load_bytes_to_byte_vec(0, 8);
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let res2 = DirectByteBuffer::convert_byte_vec(byte_vec, 8);
        assert!(res2.is_ok());
        let vec2: Vec<i64> = res2.unwrap();

        assert_eq!(vec2, Vec::from([506097522914230528]));
        assert_eq!(buffer.as_bytes().as_ptr(), vec2.as_ptr() as *mut u8);

        mem::forget(vec1);
        mem::forget(vec2);
    }

    #[test]
    fn test_create_vec_from_deep_copy() {
        let res = DirectByteBuffer::allocate_vec_for_buffer(10);
        assert!(res.is_ok());
        let mut vec = res.unwrap();
        vec[1] = 1;
        vec[2] = 2;
        vec[3] = 3;
        vec[4] = 4;
        vec[5] = 5;
        vec[6] = 6;
        vec[7] = 7;
        vec[8] = 8;
        vec[9] = 9;
        let mut buffer = DirectByteBuffer::from_vec(vec);

        let byte_vec_res = buffer.load_bytes_to_byte_vec_deep_copy(0, 8);
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let res1 = DirectByteBuffer::convert_byte_vec(byte_vec, 4);
        assert!(res1.is_ok());
        let vec1: Vec<i32> = res1.unwrap();

        assert_eq!(vec1, Vec::from([50462976, 117835012]));
        assert_ne!(buffer.as_bytes().as_ptr(), vec1.as_ptr() as *mut u8);

        let byte_vec_res = buffer.load_bytes_to_byte_vec_deep_copy(0, 8);
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let res2 = DirectByteBuffer::convert_byte_vec(byte_vec, 8);
        assert!(res2.is_ok());
        let vec2: Vec<i64> = res2.unwrap();

        assert_eq!(vec2, Vec::from([506097522914230528]));
        assert_ne!(buffer.as_bytes().as_ptr(), vec2.as_ptr() as *mut u8);
    }
}
