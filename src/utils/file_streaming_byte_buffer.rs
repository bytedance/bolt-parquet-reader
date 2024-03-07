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
use std::fs::File;
use std::intrinsics::unlikely;
use std::io::{Read, Seek, SeekFrom};
use std::mem;

use crate::convert_generic_vec;
use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::direct_byte_buffer::{Buffer, ByteBufferSlice, DirectByteBuffer};
use crate::utils::exceptions::BoltReaderError;
use crate::utils::file_loader::LoadFile;

pub struct StreamingByteBuffer<'a> {
    buffer: DirectByteBuffer,
    source: &'a dyn LoadFile,
    direct_buffer_size: usize,
    buffer_offset: usize,
    file_offset: usize,
    total_size: usize,
    buffer_size: usize,
}

pub trait FileStreamingBuffer {
    fn from_file(
        source: &dyn LoadFile,
        offset: usize,
        length: usize,
        buffer_size: usize,
    ) -> Result<StreamingByteBuffer, BoltReaderError>;

    fn update_buffer(&mut self, new_buffer: DirectByteBuffer);

    fn get_source(&self) -> &dyn LoadFile;

    fn get_file_offset(&self) -> usize;

    fn get_total_size(&self) -> usize;

    fn reload(&mut self, start: usize) -> Result<(), BoltReaderError>;
}

impl FileStreamingBuffer for StreamingByteBuffer<'_> {
    fn from_file(
        source: &dyn LoadFile,
        file_offset: usize,
        length: usize,
        buffer_size: usize,
    ) -> Result<StreamingByteBuffer, BoltReaderError> {
        let buffer = DirectByteBuffer::from_file(
            source,
            file_offset,
            min(buffer_size, min(length, source.get_file_size())),
        )?;

        let direct_buffer_size = buffer.len();
        Ok(StreamingByteBuffer {
            buffer,
            source,
            direct_buffer_size,
            buffer_offset: 0,
            file_offset,
            total_size: min(length, source.get_file_size()) - file_offset,
            buffer_size,
        })
    }

    #[inline(always)]
    fn update_buffer(&mut self, new_buffer: DirectByteBuffer) {
        self.buffer = new_buffer;
    }

    #[inline(always)]
    fn get_source(&self) -> &dyn LoadFile {
        self.source
    }

    #[inline(always)]
    fn get_file_offset(&self) -> usize {
        self.file_offset
    }

    #[inline(always)]
    fn get_total_size(&self) -> usize {
        self.total_size
    }

    #[inline(always)]
    fn reload(&mut self, start: usize) -> Result<(), BoltReaderError> {
        let to_load = min(self.get_total_size() - start, self.buffer_size);

        self.buffer.set_rpos(0);
        self.direct_buffer_size = to_load;

        if to_load == 0 {
            return Ok(());
        }

        self.buffer_offset = start;

        let offset = start + self.file_offset;
        let path = self.source.get_file_path();
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset as u64))?;
        let slice = self.buffer.create_buffer_slice(0, to_load)?;
        let mut vec = unsafe { convert_generic_vec!(slice, mem::size_of::<u8>(), u8) };
        file.read_exact(&mut vec)?;

        mem::forget(vec);

        Ok(())
    }
}

impl Read for StreamingByteBuffer<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read_len = std::cmp::min(
            self.total_size - self.buffer_offset - self.buffer.get_rpos(),
            buf.len(),
        );
        let mut remaining = read_len;

        let mut reading_offset = 0;
        while remaining > 0 {
            if self.buffer.get_rpos() == self.direct_buffer_size {
                let _ = self.reload(self.buffer.get_rpos() + self.buffer_offset);
            }
            let to_read = min(
                remaining,
                min(self.direct_buffer_size - self.buffer.get_rpos(), read_len),
            );
            for i in 0..to_read {
                buf[i + reading_offset] = self.buffer.read_u8()?;
            }

            reading_offset += to_read;

            remaining -= to_read;
        }
        Ok(read_len)
    }
}

impl ByteBufferBase for StreamingByteBuffer<'_> {
    #[inline(always)]
    fn can_create_buffer_slice(&self, start: usize, len: usize) -> bool {
        start >= self.buffer_offset && start + len <= self.buffer_offset + self.direct_buffer_size
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
        if start < self.buffer_offset || end > self.buffer_offset + self.direct_buffer_size {
            return Err(BoltReaderError::InternalError(format!(
                "Slice out of DirectByteBuffer, file file_offset: {} size: {} bytes; Slice: [{}, {})",
                self.file_offset,
                self.direct_buffer_size,
                start,
                end
            )));
        }

        Ok(&self.buffer.as_bytes()[start - self.buffer_offset..end - self.buffer_offset])
    }

    fn get_direct_byte_buffer(&mut self) -> &mut DirectByteBuffer {
        let _ = self.reload(self.buffer.get_rpos());
        &mut self.buffer
    }

    #[inline(always)]
    fn get_rpos(&self) -> usize {
        self.buffer_offset + self.buffer.get_rpos()
    }

    #[inline(always)]
    fn set_rpos(&mut self, pos: usize) {
        let rpos = min(pos, self.total_size);
        if rpos >= self.buffer_offset && pos < self.buffer_offset + self.direct_buffer_size {
            self.buffer.set_rpos(pos - self.buffer_offset);
        } else {
            self.reload(rpos).expect("Set rpos out of boundary");
        }
    }

    #[inline(always)]
    fn read_u8(&mut self) -> Result<u8, BoltReaderError> {
        if unlikely(self.direct_buffer_size - self.buffer.get_rpos() < 1) {
            self.reload(self.buffer.get_rpos() + self.buffer_offset)?;
        }
        match self.buffer.read_u8() {
            Ok(res) => Ok(res),
            Err(_) => Err(BoltReaderError::InternalError(String::from(
                "Can not read_u8()",
            ))),
        }
    }

    #[inline(always)]
    fn read_u32(&mut self) -> Result<u32, BoltReaderError> {
        if unlikely(self.direct_buffer_size - self.buffer.get_rpos() < 4) {
            self.reload(self.buffer.get_rpos() + self.buffer_offset)?;
        }
        match self.buffer.read_u32() {
            Ok(res) => Ok(res),
            Err(_) => Err(BoltReaderError::InternalError(String::from(
                "Can not read_u32()",
            ))),
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.total_size
    }

    fn is_empty(&self) -> bool {
        self.total_size == 0
    }

    fn load_bytes_to_byte_vec_deep_copy(
        &mut self,
        start: usize,
        length: usize,
    ) -> Result<Vec<u8>, BoltReaderError> {
        if length == 0 {
            return Ok(Vec::new());
        }

        if start + length > self.total_size {
            return Err(BoltReaderError::InternalError(format!(
                "load bytes out of Streaming Buffer total size: {} bytes; copy start: {}, length: {}",
                self.total_size,
                start,
                length
            )));
        }

        if start < self.buffer_offset
            || start + length > self.buffer_offset + self.direct_buffer_size
        {
            self.reload(start)?;
        } else {
            self.buffer.set_rpos(start % self.buffer_size);
        }

        let mut begin = start;

        let mut remaining = length;
        let mut vec: Vec<u8> = Vec::with_capacity(length);
        while remaining > 0 {
            let to_load = min(remaining, self.direct_buffer_size - self.buffer.get_rpos());
            let slice = self.create_buffer_slice(begin, to_load)?;

            vec.extend_from_slice(slice);
            self.buffer.set_rpos(self.buffer.get_rpos() + to_load);
            if self.buffer.get_rpos() == self.direct_buffer_size {
                self.reload(self.buffer.get_rpos() + self.buffer_offset)?;
            }
            begin += to_load;
            remaining -= to_load;
        }

        let data: Vec<u8> = unsafe {
            let length = vec.len();
            let ptr = vec.as_ptr() as *mut u8;

            Vec::from_raw_parts(ptr, length, length)
        };

        self.set_rpos(start);
        mem::forget(vec);

        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{Read, Write};
    use std::{fs, mem};

    use crate::utils::byte_buffer_base::ByteBufferBase;
    use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
    use crate::utils::file_loader::LoadFile;
    use crate::utils::file_streaming_byte_buffer::{FileStreamingBuffer, StreamingByteBuffer};
    use crate::utils::local_file_loader::LocalFileLoader;

    // 4KB
    const DEFAULT_STREAMING_BUFFER_LOADING_SIZE: usize = 1 << 12;

    const DEFAULT_TESTING_FILE_SIZE: usize = 1 << 16;

    fn file_exists(path: &str) -> bool {
        if let Ok(metadata) = fs::metadata(path) {
            metadata.is_file()
        } else {
            false
        }
    }

    fn create_testing_file(path: &String) {
        if file_exists(path) {
            remove_testing_file(path);
        }

        let mut file = File::create(path).unwrap();
        let vec: Vec<u8> = (0..DEFAULT_TESTING_FILE_SIZE)
            .map(|x| (x % 100) as u8)
            .collect();
        let _ = file.write_all(&vec);
    }

    fn remove_testing_file(path: &String) {
        if !file_exists(path) {
            return;
        }

        fs::remove_file(path).unwrap_or_else(|why| {
            println!("! {:?}", why.kind());
        });
    }

    #[test]
    fn test_create_streaming_byte_buffer() {
        let path = String::from("src/sample_files/test_file_1");
        create_testing_file(&path);

        let file_loader = LocalFileLoader::new(&path).unwrap();
        let file_offset = 4;
        let streaming_buffer = StreamingByteBuffer::from_file(
            &file_loader,
            file_offset,
            file_loader.get_file_size(),
            DEFAULT_STREAMING_BUFFER_LOADING_SIZE,
        )
        .unwrap();

        assert_eq!(streaming_buffer.file_offset, file_offset);
        assert_eq!(streaming_buffer.total_size, file_loader.get_file_size() - 4);

        remove_testing_file(&path);
    }

    #[test]
    fn test_create_buffer_slice() {
        let path = String::from("src/sample_files/test_file_2");
        create_testing_file(&path);

        let file_loader = LocalFileLoader::new(&path).unwrap();
        let file_offset = 4;
        let streaming_buffer = StreamingByteBuffer::from_file(
            &file_loader,
            file_offset,
            file_loader.get_file_size(),
            DEFAULT_STREAMING_BUFFER_LOADING_SIZE,
        )
        .unwrap();

        let slice_offset = 2;

        for slice_length in 1..DEFAULT_STREAMING_BUFFER_LOADING_SIZE - slice_offset {
            assert!(streaming_buffer.can_create_buffer_slice(slice_offset, slice_length));
            let res = streaming_buffer.create_buffer_slice(slice_offset, slice_length);
            assert!(res.is_ok());
            let res = res.unwrap();

            let vec: Vec<u8> = ((file_offset + slice_offset)
                ..(file_offset + slice_offset + slice_length))
                .map(|x| (x % 100) as u8)
                .collect();

            assert_eq!(res, vec);
        }

        let too_large_slice_length = DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 10;

        assert!(!streaming_buffer.can_create_buffer_slice(slice_offset, too_large_slice_length));
        let res = streaming_buffer.create_buffer_slice(slice_offset, too_large_slice_length);
        assert!(res.is_err());
        remove_testing_file(&path);
    }

    #[test]
    fn test_create_vec_from_zero_copy() {
        let path = String::from("src/sample_files/test_file_3");
        create_testing_file(&path);

        let file_loader = LocalFileLoader::new(&path).unwrap();
        let file_offset = 4;
        let streaming_buffer = StreamingByteBuffer::from_file(
            &file_loader,
            file_offset,
            file_loader.get_file_size(),
            DEFAULT_STREAMING_BUFFER_LOADING_SIZE,
        )
        .unwrap();

        let slice_offset = 2;
        let slice_length = 8;

        assert!(streaming_buffer.can_create_buffer_slice(slice_offset, slice_length));
        let byte_vec_res = streaming_buffer.load_bytes_to_byte_vec(0, 8);
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let res1 = DirectByteBuffer::convert_byte_vec(byte_vec, 4);
        assert!(res1.is_ok());
        let vec1: Vec<i32> = res1.unwrap();

        assert_eq!(vec1, Vec::from([117835012, 185207048]));
        assert_eq!(
            streaming_buffer.buffer.as_bytes().as_ptr(),
            vec1.as_ptr() as *mut u8
        );

        assert!(streaming_buffer.can_create_buffer_slice(slice_offset, slice_length));
        let byte_vec_res = streaming_buffer.load_bytes_to_byte_vec(0, 8);
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let res2 = DirectByteBuffer::convert_byte_vec(byte_vec, 8);
        assert!(res2.is_ok());
        let vec2: Vec<i64> = res2.unwrap();

        assert_eq!(vec2, Vec::from([795458214266537220]));
        assert_eq!(
            streaming_buffer.buffer.as_bytes().as_ptr(),
            vec2.as_ptr() as *mut u8
        );

        mem::forget(vec1);
        mem::forget(vec2);

        assert!(streaming_buffer.can_create_buffer_slice(slice_offset, slice_length));
        let byte_vec_res =
            streaming_buffer.load_bytes_to_byte_vec(0, DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 16);
        assert!(byte_vec_res.is_err());

        remove_testing_file(&path);
    }

    #[test]
    fn test_create_vec_from_deep_copy() {
        let path = String::from("src/sample_files/test_file_4");
        create_testing_file(&path);

        let file_loader = LocalFileLoader::new(&path).unwrap();
        let file_offset = 4;
        let mut streaming_buffer = StreamingByteBuffer::from_file(
            &file_loader,
            file_offset,
            file_loader.get_file_size(),
            DEFAULT_STREAMING_BUFFER_LOADING_SIZE,
        )
        .unwrap();

        let slice_offset = 0;
        let slice_length = 8;
        assert!(streaming_buffer.can_create_buffer_slice(slice_offset, slice_length));

        let byte_vec_res = streaming_buffer.load_bytes_to_byte_vec_deep_copy(slice_offset, 8);
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let res1 = DirectByteBuffer::convert_byte_vec(byte_vec, 4);
        assert!(res1.is_ok());
        let vec1: Vec<i32> = res1.unwrap();

        assert_eq!(vec1, Vec::from([117835012, 185207048]));
        assert_ne!(
            streaming_buffer.buffer.as_bytes().as_ptr(),
            vec1.as_ptr() as *mut u8
        );

        let byte_vec_res = streaming_buffer.load_bytes_to_byte_vec_deep_copy(slice_offset, 8);
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let res2 = DirectByteBuffer::convert_byte_vec(byte_vec, 8);
        assert!(res2.is_ok());
        let vec2: Vec<i64> = res2.unwrap();

        assert_eq!(vec2, Vec::from([795458214266537220]));
        assert_ne!(
            streaming_buffer.buffer.as_bytes().as_ptr(),
            vec2.as_ptr() as *mut u8
        );

        let byte_vec_res = streaming_buffer.load_bytes_to_byte_vec_deep_copy(
            slice_offset,
            DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 16,
        );
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let large_res = DirectByteBuffer::convert_byte_vec(byte_vec, 8);
        assert!(large_res.is_ok());
        let large_vec: Vec<i64> = large_res.unwrap();

        let vec_u8: Vec<u8> = ((file_offset + slice_offset)
            ..(file_offset + slice_offset + DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 16))
            .map(|x| (x % 100) as u8)
            .collect();
        let vec_i64 = unsafe {
            let length = vec_u8.len() / 8;
            let ptr = vec_u8.as_ptr() as *mut i64;

            Vec::from_raw_parts(ptr, length, length)
        };

        assert_eq!(large_vec, vec_i64);
        mem::forget(vec_u8);

        let slice_offset = DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 16;

        let byte_vec_res = streaming_buffer.load_bytes_to_byte_vec_deep_copy(
            slice_offset,
            DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 16,
        );
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let large_res = DirectByteBuffer::convert_byte_vec(byte_vec, 8);
        assert!(large_res.is_ok());
        let large_vec: Vec<i64> = large_res.unwrap();

        let vec_u8: Vec<u8> = ((file_offset + slice_offset)
            ..(file_offset + slice_offset + DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 16))
            .map(|x| (x % 100) as u8)
            .collect();
        let vec_i64 = unsafe {
            let length = vec_u8.len() / 8;
            let ptr = vec_u8.as_ptr() as *mut i64;

            Vec::from_raw_parts(ptr, length, length)
        };

        mem::forget(vec_u8);
        assert_eq!(large_vec, vec_i64);

        let slice_offset = 16;

        let byte_vec_res = streaming_buffer.load_bytes_to_byte_vec_deep_copy(
            slice_offset,
            DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 16,
        );
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let large_res = DirectByteBuffer::convert_byte_vec(byte_vec, 8);
        assert!(large_res.is_ok());
        let large_vec: Vec<i64> = large_res.unwrap();

        let vec_u8: Vec<u8> = ((file_offset + slice_offset)
            ..(file_offset + slice_offset + DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 16))
            .map(|x| (x % 100) as u8)
            .collect();
        let vec_i64 = unsafe {
            let length = vec_u8.len() / 8;
            let ptr = vec_u8.as_ptr() as *mut i64;

            Vec::from_raw_parts(ptr, length, length)
        };

        mem::forget(vec_u8);
        assert_eq!(large_vec, vec_i64);

        let slice_offset = DEFAULT_STREAMING_BUFFER_LOADING_SIZE * 2 + 16;

        let byte_vec_res = streaming_buffer.load_bytes_to_byte_vec_deep_copy(
            slice_offset,
            DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 16,
        );
        assert!(byte_vec_res.is_ok());
        let byte_vec = byte_vec_res.unwrap();
        let large_res = DirectByteBuffer::convert_byte_vec(byte_vec, 8);
        assert!(large_res.is_ok());
        let large_vec: Vec<i64> = large_res.unwrap();

        let vec_u8: Vec<u8> = ((file_offset + slice_offset)
            ..(file_offset + slice_offset + DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 16))
            .map(|x| (x % 100) as u8)
            .collect();
        let vec_i64 = unsafe {
            let length = vec_u8.len() / 8;
            let ptr = vec_u8.as_ptr() as *mut i64;

            Vec::from_raw_parts(ptr, length, length)
        };

        mem::forget(vec_u8);
        assert_eq!(large_vec, vec_i64);

        remove_testing_file(&path);
    }

    #[test]
    fn test_create_vec_from_deep_copy_random() {
        let path = String::from("src/sample_files/test_file_5");
        create_testing_file(&path);

        let file_loader = LocalFileLoader::new(&path).unwrap();
        let file_offset = 4;
        let mut streaming_buffer = StreamingByteBuffer::from_file(
            &file_loader,
            file_offset,
            file_loader.get_file_size(),
            DEFAULT_STREAMING_BUFFER_LOADING_SIZE,
        )
        .unwrap();

        for slice_offset in
            (0..streaming_buffer.total_size - DEFAULT_STREAMING_BUFFER_LOADING_SIZE - 16)
                .step_by(1024)
        {
            let byte_vec_res = streaming_buffer.load_bytes_to_byte_vec_deep_copy(
                slice_offset,
                DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 16,
            );
            assert!(byte_vec_res.is_ok());
            let byte_vec = byte_vec_res.unwrap();
            let large_res = DirectByteBuffer::convert_byte_vec(byte_vec, 8);
            assert!(large_res.is_ok());
            let large_vec: Vec<i64> = large_res.unwrap();

            let vec_u8: Vec<u8> = ((file_offset + slice_offset)
                ..(file_offset + slice_offset + DEFAULT_STREAMING_BUFFER_LOADING_SIZE + 16))
                .map(|x| (x % 100) as u8)
                .collect();
            let vec_i64 = unsafe {
                let length = vec_u8.len() / 8;
                let ptr = vec_u8.as_ptr() as *mut i64;

                Vec::from_raw_parts(ptr, length, length)
            };

            mem::forget(vec_u8);
            assert_eq!(large_vec, vec_i64);
        }

        remove_testing_file(&path);
    }

    #[test]
    fn test_read() {
        let path = String::from("src/sample_files/test_file_6");
        create_testing_file(&path);

        let file_loader = LocalFileLoader::new(&path).unwrap();
        let file_offset = 4;
        let mut streaming_buffer = StreamingByteBuffer::from_file(
            &file_loader,
            file_offset,
            file_loader.get_file_size(),
            5,
        )
        .unwrap();

        let vec_u8: Vec<u8> = (0..streaming_buffer.total_size)
            .map(|x| ((x + file_offset) % 100) as u8)
            .collect();

        for step in 1..9 {
            let mut buf: Vec<u8> = vec![0; step];
            let mut cnt = 0;
            while streaming_buffer.buffer_offset + streaming_buffer.buffer.get_rpos()
                < streaming_buffer.total_size
            {
                streaming_buffer.read(&mut buf).unwrap();
                for i in 0..step {
                    assert_eq!(buf[i], vec_u8[cnt + i]);
                }
                cnt += step;
            }
        }
        remove_testing_file(&path);
    }
}
