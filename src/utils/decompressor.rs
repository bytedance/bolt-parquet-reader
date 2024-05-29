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

use bytebuffer::Endian::LittleEndian;
use flate2::read::GzDecoder;

use crate::utils::byte_buffer_base::{BufferEnum, ByteBufferBase};
use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
use crate::utils::exceptions::BoltReaderError;

pub enum DecompressorEnum {
    GzipDecompressor(GzipDecompressor),
}

pub trait Decompress {
    fn decompress(
        &self,
        buffer: &mut dyn ByteBufferBase,
        compressed_size: usize,
    ) -> Result<BufferEnum, BoltReaderError>;
}

pub struct GzipDecompressor {
    //     We don't need any struct members here. We just need the interface
}

impl GzipDecompressor {
    pub fn new() -> GzipDecompressor {
        GzipDecompressor {}
    }
}

impl Default for GzipDecompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl Decompress for DecompressorEnum {
    fn decompress(
        &self,
        buffer: &mut dyn ByteBufferBase,
        compressed_size: usize,
    ) -> Result<BufferEnum, BoltReaderError> {
        match self {
            DecompressorEnum::GzipDecompressor(decompressor) => {
                decompressor.decompress(buffer, compressed_size)
            }
        }
    }
}

impl Decompress for GzipDecompressor {
    fn decompress(
        &self,
        buffer: &mut dyn ByteBufferBase,
        compressed_size: usize,
    ) -> Result<BufferEnum, BoltReaderError> {
        let zero_copy;
        let data: Vec<u8> = if buffer.can_create_buffer_slice(buffer.get_rpos(), compressed_size) {
            zero_copy = true;
            DirectByteBuffer::convert_byte_vec(
                buffer.load_bytes_to_byte_vec(buffer.get_rpos(), compressed_size)?,
                1,
            )?
        } else {
            zero_copy = false;
            DirectByteBuffer::convert_byte_vec(
                buffer.load_bytes_to_byte_vec_deep_copy(buffer.get_rpos(), compressed_size)?,
                1,
            )?
        };

        let data_slice = data.as_slice();

        let mut decompressor = GzDecoder::new(data_slice);
        let mut decompressed_data = Vec::new();
        decompressor.read_to_end(&mut decompressed_data)?;

        if zero_copy {
            mem::forget(data);
        }

        let mut decompressed_buffer = DirectByteBuffer::from_vec(decompressed_data);
        decompressed_buffer.set_endian(LittleEndian);

        Ok(BufferEnum::DirectByteBuffer(decompressed_buffer))
    }
}
