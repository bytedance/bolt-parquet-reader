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
use std::rc::Rc;

use byteorder::{ByteOrder, LittleEndian};

use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::exceptions::BoltReaderError;
use crate::utils::file_loader::FileLoader;

// Currently, we use 1GB as the max capacity.
// todo: Create config module to handle the default const values.
const MAX_CAPACITY: usize = 1 << 30;

pub struct SharedMemoryBuffer {
    buffer: Rc<Vec<u8>>,
    // The offset of the whole buffer from the source file
    external_offset: usize,
    // The offset within the buffer
    buffer_offset: usize,
    buffer_size: usize,
    rpos: usize,
}

impl Read for SharedMemoryBuffer {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read_len = std::cmp::min(self.len() - self.rpos, buf.len());
        let range = self.rpos + self.buffer_offset..self.rpos + self.buffer_offset + read_len;
        for (i, val) in self.buffer[range].iter().enumerate() {
            buf[i] = *val;
        }
        self.rpos += read_len;
        Ok(read_len)
    }
}

impl ByteBufferBase for SharedMemoryBuffer {
    fn can_create_buffer_slice(&self, _start: usize, _len: usize) -> bool {
        true
    }

    fn create_buffer_slice(
        &self,
        start: usize,
        len: usize,
    ) -> Result<crate::utils::direct_byte_buffer::ByteBufferSlice<'_>, BoltReaderError> {
        if len == 0 {
            return Err(BoltReaderError::InternalError(String::from(
                "Slice size should not be 0.",
            )));
        }
        let end = start + len;
        if start >= self.len() || end > self.len() {
            return Err(BoltReaderError::InternalError(format!(
                "Slice out of SharedMemoryBuffer size: {} bytes; Slice: [{}, {})",
                self.len(),
                start,
                end
            )));
        }
        Ok(&self.buffer[start + self.buffer_offset..end + self.buffer_offset])
    }

    fn get_rpos(&self) -> usize {
        self.rpos
    }

    fn set_rpos(&mut self, pos: usize) {
        self.rpos = pos;
    }

    fn read_u8(&mut self) -> Result<u8, BoltReaderError> {
        if self.rpos >= self.len() {
            return Err(BoltReaderError::BufferError(String::from(
                "Shared Memroy Buffer: Can not read_u8()",
            )));
        }
        let to_read = self.rpos;
        self.rpos += 1;

        Ok(self.buffer[self.buffer_offset + to_read])
    }

    fn read_u32(&mut self) -> Result<u32, BoltReaderError> {
        if self.rpos + 4 > self.len() {
            return Err(BoltReaderError::BufferError(String::from(
                "Shared Memroy Buffer: Can not read_u32()",
            )));
        }
        let range = self.buffer_offset + self.rpos..self.buffer_offset + self.rpos + 4;
        self.rpos += 4;

        Ok(LittleEndian::read_u32(&self.buffer[range]))
    }

    fn len(&self) -> usize {
        self.buffer_size
    }

    fn is_empty(&self) -> bool {
        self.buffer_size == 0
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

impl SharedMemoryBuffer {
    pub fn new(
        buffer: Rc<Vec<u8>>,
        external_offset: usize,
        buffer_offset: usize,
        buffer_size: usize,
        rpos: usize,
    ) -> Result<SharedMemoryBuffer, BoltReaderError> {
        if buffer_size > MAX_CAPACITY {
            return Err(BoltReaderError::BufferError(format!(                "Shared Memory Buffer: Allocated capacity is over the max direct buffer capacity: {} bytes",
                                                                            MAX_CAPACITY)));
        }

        Ok(SharedMemoryBuffer {
            buffer,
            external_offset,
            buffer_offset,
            buffer_size,
            rpos,
        })
    }

    pub fn from_file(
        source: &dyn FileLoader,
        offset: usize,
        length: usize,
    ) -> Result<SharedMemoryBuffer, BoltReaderError> {
        let raw_buffer = source.load_file_to_raw_buffer(offset, length)?;

        Ok(SharedMemoryBuffer {
            buffer: Rc::from(raw_buffer),
            external_offset: offset,
            buffer_offset: 0,
            buffer_size: length,
            rpos: 0,
        })
    }

    pub fn from_shared_memory_buffer(
        other_buffer: &SharedMemoryBuffer,
        buffer_offset: usize,
        buffer_size: usize,
    ) -> Result<SharedMemoryBuffer, BoltReaderError> {
        SharedMemoryBuffer::new(
            Rc::clone(&other_buffer.buffer),
            other_buffer.external_offset,
            buffer_offset,
            buffer_size,
            0,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::rc::Rc;

    use crate::utils::byte_buffer_base::ByteBufferBase;
    use crate::utils::direct_byte_buffer::*;
    use crate::utils::shared_memory_buffer::{SharedMemoryBuffer, MAX_CAPACITY};

    #[test]
    fn test_allocate_byte_buffer() {
        let res = DirectByteBuffer::allocate_vec_for_buffer(10);
        assert!(res.is_ok());
        let mut vec = res.unwrap();
        vec[1] = 1;
        let vec = Rc::new(vec);

        let buffer = SharedMemoryBuffer::new(vec, 0, 0, 10, 0);
        assert!(buffer.is_ok());

        let mut buffer = buffer.unwrap();
        assert_eq!(buffer.len(), 10);

        assert_eq!(buffer.read_u8().unwrap(), 0);
        assert_eq!(buffer.read_u8().unwrap(), 1);

        let res = DirectByteBuffer::allocate_vec_for_buffer(MAX_CAPACITY + 1);
        assert!(res.is_err());
    }

    #[test]
    fn test_create_multiple_buffers() {
        let res = DirectByteBuffer::allocate_vec_for_buffer(10);
        assert!(res.is_ok());
        let mut vec = res.unwrap();
        vec[1] = 1;
        let vec = Rc::new(vec);

        let res = SharedMemoryBuffer::new(vec, 0, 0, 10, 0);
        assert!(res.is_ok());
        let root_buffer = res.unwrap();

        let res = SharedMemoryBuffer::from_shared_memory_buffer(&root_buffer, 1, 5);
        let mut buffer = res.unwrap();
        assert_eq!(buffer.read_u8().unwrap(), 1);
        assert_eq!(buffer.read_u8().unwrap(), 0);
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
        let vec = Rc::new(vec);

        let buffer = SharedMemoryBuffer::new(vec, 0, 0, 10, 0);
        assert!(buffer.is_ok());
        let buffer = buffer.unwrap();

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
        let vec = Rc::new(vec);

        let buffer = SharedMemoryBuffer::new(vec, 0, 0, 10, 0);
        assert!(buffer.is_ok());
        let buffer = buffer.unwrap();

        let byte_vec_res = buffer.load_bytes_to_byte_vec(0, 8);
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let res1 = DirectByteBuffer::convert_byte_vec(byte_vec, 4);
        assert!(res1.is_ok());
        let vec1: Vec<i32> = res1.unwrap();

        assert_eq!(vec1, Vec::from([50462976, 117835012]));
        assert_eq!(buffer.buffer.as_ptr(), vec1.as_ptr() as *mut u8);

        let byte_vec_res = buffer.load_bytes_to_byte_vec(0, 8);
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let res2 = DirectByteBuffer::convert_byte_vec(byte_vec, 8);
        assert!(res2.is_ok());
        let vec2: Vec<i64> = res2.unwrap();

        assert_eq!(vec2, Vec::from([506097522914230528]));
        assert_eq!(buffer.buffer.as_ptr(), vec2.as_ptr() as *mut u8);

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
        let vec = Rc::new(vec);

        let buffer = SharedMemoryBuffer::new(vec, 0, 0, 10, 0);
        assert!(buffer.is_ok());
        let mut buffer = buffer.unwrap();

        let byte_vec_res = buffer.load_bytes_to_byte_vec_deep_copy(0, 8);
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let res1 = DirectByteBuffer::convert_byte_vec(byte_vec, 4);
        assert!(res1.is_ok());
        let vec1: Vec<i32> = res1.unwrap();

        assert_eq!(vec1, Vec::from([50462976, 117835012]));
        assert_ne!(buffer.buffer.as_ptr(), vec1.as_ptr() as *mut u8);

        let byte_vec_res = buffer.load_bytes_to_byte_vec_deep_copy(0, 8);
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let res2 = DirectByteBuffer::convert_byte_vec(byte_vec, 8);
        assert!(res2.is_ok());
        let vec2: Vec<i64> = res2.unwrap();

        assert_eq!(vec2, Vec::from([506097522914230528]));
        assert_ne!(buffer.buffer.as_ptr(), vec2.as_ptr() as *mut u8);
    }
}
