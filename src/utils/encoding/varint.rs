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

use std::intrinsics::unlikely;

use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::direct_byte_buffer::DirectByteBuffer;
use crate::utils::exceptions::BoltReaderError;

const MAX_VARINT_LENGTH_32_BIT: usize = 5;
const MAX_VARINT_LENGTH_64_BIT: usize = 10;

#[inline(always)]
pub fn get_max_varint_length_32_bit() -> usize {
    MAX_VARINT_LENGTH_32_BIT
}

#[inline(always)]
pub fn get_max_varint_length_64_bit() -> usize {
    MAX_VARINT_LENGTH_64_BIT
}

#[inline(always)]
pub fn encode_varint(mut val: u64) -> DirectByteBuffer {
    let mut vec = Vec::new();
    while val >= 128 {
        vec.push((0x80 | (val & 0x7f)) as u8);
        val >>= 7;
    }
    vec.push(val as u8);
    DirectByteBuffer::from_vec(vec)
}

#[inline(always)]
pub fn decode_varint(buffer: &mut dyn ByteBufferBase) -> Result<u64, BoltReaderError> {
    let mut val: u64 = 0;
    let mut begin = 0;
    let end = buffer.len();

    let mut shift = 0;
    let mut cur = buffer.read_u8()? as i8;
    while begin < end && cur < 0 {
        val |= ((cur & 0x7f) as u64) << shift;
        cur = buffer.read_u8()? as i8;
        shift += 7;
        begin += 1;

        if unlikely(begin >= MAX_VARINT_LENGTH_64_BIT) {
            return Err(BoltReaderError::VarintDecodingError(String::from(
                "Varint buffer contains too many bytes",
            )));
        }
    }

    if begin == end {
        return Err(BoltReaderError::VarintDecodingError(String::from(
            "Varint buffer contains too few bytes",
        )));
    }
    val |= (cur as u64) << shift;

    Ok(val)
}

#[cfg(test)]
mod tests {
    use crate::utils::byte_buffer_base::ByteBufferBase;
    use crate::utils::direct_byte_buffer::DirectByteBuffer;
    use crate::utils::encoding::varint::{decode_varint, encode_varint};

    #[test]
    fn test_varint_e2e_encoding_decoding() {
        let mut num = 1;
        while num < 1 << 16 {
            let mut shift = 0;
            while shift < 47 {
                let input = num << shift;
                let mut buf = encode_varint(input);
                let res = decode_varint(&mut buf);
                assert!(res.is_ok());
                let res = res.unwrap();
                assert_eq!(res, input);
                shift += 1;
            }
            num += 1;
        }
    }

    #[test]
    fn test_extra_bytes_in_decode_input() {
        let buf = encode_varint(1000000);
        assert_eq!(buf.len(), 3);
        let mut vec = buf.into_vec();
        vec.push(0);
        vec.push(1);
        vec.push(2);
        vec.push(3);
        let mut long_buf = DirectByteBuffer::from_vec(vec);
        assert_eq!(long_buf.len(), 7);
        let res = decode_varint(&mut long_buf);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 1000000);
    }

    #[test]
    fn test_max() {
        let mut buf = encode_varint(u64::MAX);
        let res = decode_varint(&mut buf);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), u64::MAX);
    }

    #[test]
    fn test_min() {
        let mut buf = encode_varint(0);
        let res = decode_varint(&mut buf);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
    }

    #[test]
    fn test_decode_input_with_too_many_bytes() {
        let buf = encode_varint(u64::MAX);
        assert_eq!(buf.len(), 10);
        let mut vec = buf.into_vec();
        vec[9] = vec[8];
        vec.push(0);
        vec.push(1);
        vec.push(2);
        vec.push(3);
        let mut incorrect_buf = DirectByteBuffer::from_vec(vec);
        assert_eq!(incorrect_buf.len(), 14);
        let res = decode_varint(&mut incorrect_buf);
        assert!(res.is_err());
    }

    #[test]
    fn test_decode_input_with_too_few_bytes() {
        let buf = encode_varint(1000000);
        assert_eq!(buf.len(), 3);
        let mut incorrect_buf =
            DirectByteBuffer::from_bytes(buf.create_buffer_slice(0, 2).unwrap());
        assert_eq!(incorrect_buf.len(), 2);
        let res = decode_varint(&mut incorrect_buf);
        assert!(res.is_err());
    }
}
