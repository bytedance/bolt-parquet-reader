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

use parquet2::encoding::hybrid_rle::HybridRleDecoder;

use crate::utils::byte_buffer_base::ByteBufferBase;
use crate::utils::direct_byte_buffer::{Buffer, DirectByteBuffer};
use crate::utils::encoding::varint::decode_varint;
use crate::utils::exceptions::BoltReaderError;

/// This RLE/BP Decoder is currently based on Rust Arrow2 Parquet.
/// TODO: Implement a native RLE/BP Decoder, supporting filter push down.

pub struct RleBpDecoder {}

impl RleBpDecoder {
    #[inline(always)]
    #[allow(dead_code)]
    pub fn ceil_dividing(value: usize, divisor: usize) -> usize {
        value / divisor + ((value % divisor != 0) as usize)
    }

    #[inline(always)]
    pub fn get_minimum_required_bits(value: u32) -> u32 {
        32 - value.leading_zeros()
    }

    #[allow(dead_code)]
    pub fn read_header(
        buffer: &mut dyn ByteBufferBase,
    ) -> Result<(usize, usize, bool), BoltReaderError> {
        let rpos = buffer.get_rpos();
        let res = decode_varint(buffer)? as u32;
        let is_bit_packing = res & 1 == 1;
        let num_values = if is_bit_packing {
            (res >> 1) * 8
        } else {
            res >> 1
        };
        let header_length = buffer.get_rpos() - rpos;
        buffer.set_rpos(rpos);
        Ok((num_values as usize, header_length, is_bit_packing))
    }

    #[allow(dead_code)]
    pub fn decode(
        buffer: &mut dyn ByteBufferBase,
        bit_width: usize,
    ) -> Result<Vec<u32>, BoltReaderError> {
        let (num_values, header_length, is_bit_packing) = RleBpDecoder::read_header(buffer)?;
        let length = if is_bit_packing {
            header_length + num_values * bit_width / 8
        } else {
            header_length + RleBpDecoder::ceil_dividing(bit_width, 8)
        };

        let zero_copy;

        let encoded_data: Vec<u8> = if buffer.can_create_buffer_slice(buffer.get_rpos(), length) {
            zero_copy = true;
            DirectByteBuffer::convert_byte_vec(
                buffer.load_bytes_to_byte_vec(buffer.get_rpos(), length)?,
                mem::size_of::<u8>(),
            )?
        } else {
            zero_copy = false;
            DirectByteBuffer::convert_byte_vec(
                buffer.load_bytes_to_byte_vec_deep_copy(buffer.get_rpos(), length)?,
                mem::size_of::<u8>(),
            )?
        };

        let decoder = HybridRleDecoder::try_new(&encoded_data, bit_width as u32, num_values)?;

        let result = decoder.collect::<Result<Vec<_>, _>>()?;

        if zero_copy {
            mem::forget(encoded_data);
        }

        buffer.set_rpos(buffer.get_rpos() + length);

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use parquet2::encoding::hybrid_rle::encode_u32;
    use rand::{thread_rng, Rng};

    use crate::utils::direct_byte_buffer::DirectByteBuffer;
    use crate::utils::encoding::rle_bp::RleBpDecoder;
    use crate::utils::encoding::varint::encode_varint;
    use crate::utils::exceptions::BoltReaderError;

    fn encode_rle(
        value: u32,
        num_values: usize,
        bit_width: usize,
    ) -> Result<DirectByteBuffer, BoltReaderError> {
        let mut buffer = encode_varint((num_values << 1) as u64);
        let mut result: Vec<u8> = vec![0; RleBpDecoder::ceil_dividing(bit_width, 8)];
        for i in 0..result.len() {
            result[i] = ((value >> ((i as u8) * 8)) & 0xFF) as u8
        }
        buffer.write(&result)?;

        Ok(buffer)
    }

    #[test]
    fn test_decoding_bit_packing() {
        let mut vec = vec![];
        let bit_width = 10;

        let data = (0..1000).collect::<Vec<_>>();

        // This API is only able to encode Bit-Packing
        encode_u32(&mut vec, data.iter().cloned(), bit_width as u32).unwrap();

        let mut buf = DirectByteBuffer::from_vec(vec);
        let result = RleBpDecoder::decode(&mut buf, bit_width).unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_decoding_bit_packing_random() {
        let factor = 100;
        let mut rng = thread_rng();
        for i in 1..factor {
            let mut data: Vec<u32> = (0..i * 8).map(|_| rng.gen()).collect();
            data.iter_mut().for_each(|x| *x %= 1 << 20);
            let minimum_bit_width = data
                .iter()
                .map(|v| RleBpDecoder::get_minimum_required_bits(*v))
                .max()
                .unwrap();

            for bit_width in minimum_bit_width..32 {
                let mut vec = vec![];
                encode_u32(&mut vec, data.iter().cloned(), bit_width).unwrap();
                let mut buf = DirectByteBuffer::from_vec(vec);
                let result = RleBpDecoder::decode(&mut buf, bit_width as usize);

                assert!(result.is_ok());
                assert_eq!(data, result.unwrap());
            }
        }
    }

    #[test]
    fn test_decoding_incorrect_bit_packing() {
        let mut vec = vec![];
        let bit_width = 10;

        let data = (0..1000).collect::<Vec<_>>();

        // This API is only able to encode Bit-Packing
        encode_u32(&mut vec, data.iter().cloned(), bit_width as u32).unwrap();

        // Pop one element from the vec and make it invalid
        vec.pop();

        let mut buf = DirectByteBuffer::from_vec(vec);
        let result = RleBpDecoder::decode(&mut buf, bit_width);

        assert!(result.is_err());
    }

    #[test]
    fn test_decoding_run_length_encoding() {
        let vec = vec![8, 2, 0];

        let bit_width = 10;
        let mut buf = DirectByteBuffer::from_vec(vec);
        let result = RleBpDecoder::decode(&mut buf, bit_width);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), &[2, 2, 2, 2]);

        let vec = vec![8, 2];

        let bit_width = 4;
        let mut buf = DirectByteBuffer::from_vec(vec);
        let result = RleBpDecoder::decode(&mut buf, bit_width);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), &[2, 2, 2, 2]);
    }

    #[test]
    fn test_decoding_run_length_encoding_random() {
        let num_tests = 100;
        let max_num_values = 100;
        let mut rng = thread_rng();

        for _ in 0..num_tests {
            let mut value = rng.gen();
            value = value % (1 << 20);
            for num_values in 1..max_num_values {
                for bit_width in RleBpDecoder::get_minimum_required_bits(value)..32 {
                    let res = encode_rle(value, num_values, bit_width as usize);
                    assert!(res.is_ok());
                    let mut buf = res.unwrap();
                    let result = RleBpDecoder::decode(&mut buf, bit_width as usize).unwrap();
                    assert_eq!(result, vec![value; num_values]);
                }
            }
        }
    }

    #[test]
    fn test_decoding_incorrect_run_length_encoding() {
        let vec = vec![8, 2];

        let bit_width = 10;
        let mut buf = DirectByteBuffer::from_vec(vec);
        let result = RleBpDecoder::decode(&mut buf, bit_width);

        assert!(result.is_err());
    }
}
