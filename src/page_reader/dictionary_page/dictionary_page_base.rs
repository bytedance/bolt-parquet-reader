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

use crate::bridge::byte_array_bridge::ByteArray;
use crate::page_reader::dictionary_page::dictionary_page_byte_array::DictionaryPageByteArray;
use crate::page_reader::dictionary_page::dictionary_page_float32::DictionaryPageFloat32;
use crate::page_reader::dictionary_page::dictionary_page_float32_with_filters::DictionaryPageWithFilterFloat32;
use crate::page_reader::dictionary_page::dictionary_page_float64::DictionaryPageFloat64;
use crate::page_reader::dictionary_page::dictionary_page_float64_with_filters::DictionaryPageWithFilterFloat64;
use crate::page_reader::dictionary_page::dictionary_page_int32::DictionaryPageInt32;
use crate::page_reader::dictionary_page::dictionary_page_int32_with_filters::DictionaryPageWithFilterInt32;
use crate::page_reader::dictionary_page::dictionary_page_int64::DictionaryPageInt64;
use crate::page_reader::dictionary_page::dictionary_page_int64_with_filters::DictionaryPageWithFilterInt64;

pub enum DictionaryPageEnum {
    DictionaryPageInt32(DictionaryPageInt32),
    DictionaryPageFloat32(DictionaryPageFloat32),
    DictionaryPageInt64(DictionaryPageInt64),
    DictionaryPageFloat64(DictionaryPageFloat64),
    DictionaryPageByteArray(DictionaryPageByteArray),
    DictionaryPageWithFilterInt32(DictionaryPageWithFilterInt32),
    DictionaryPageWithFilterFloat32(DictionaryPageWithFilterFloat32),
    DictionaryPageWithFilterInt64(DictionaryPageWithFilterInt64),
    DictionaryPageWithFilterFloat64(DictionaryPageWithFilterFloat64),
}

pub trait DictionaryPageNew {
    fn validate(&self, index: usize) -> bool;

    fn find_int32(&self, _index: usize) -> i32 {
        i32::default()
    }

    fn find_float32(&self, _index: usize) -> f32 {
        f32::default()
    }

    fn find_int64(&self, _index: usize) -> i64 {
        i64::default()
    }

    fn find_float64(&self, _index: usize) -> f64 {
        f64::default()
    }

    fn find_byte_array(&self, _index: usize) -> ByteArray {
        ByteArray::default()
    }

    fn get_num_values(&self) -> usize;

    fn get_type_size(&self) -> usize;

    fn is_zero_copied(&self) -> bool;
}

impl DictionaryPageNew for DictionaryPageEnum {
    fn validate(&self, index: usize) -> bool {
        match self {
            DictionaryPageEnum::DictionaryPageInt32(dictionary_page) => {
                dictionary_page.validate(index)
            }
            DictionaryPageEnum::DictionaryPageFloat32(dictionary_page) => {
                dictionary_page.validate(index)
            }
            DictionaryPageEnum::DictionaryPageInt64(dictionary_page) => {
                dictionary_page.validate(index)
            }
            DictionaryPageEnum::DictionaryPageFloat64(dictionary_page) => {
                dictionary_page.validate(index)
            }
            DictionaryPageEnum::DictionaryPageByteArray(dictionary_page) => {
                dictionary_page.validate(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt32(dictionary_page) => {
                dictionary_page.validate(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat32(dictionary_page) => {
                dictionary_page.validate(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt64(dictionary_page) => {
                dictionary_page.validate(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat64(dictionary_page) => {
                dictionary_page.validate(index)
            }
        }
    }

    fn find_int32(&self, index: usize) -> i32 {
        match self {
            DictionaryPageEnum::DictionaryPageInt32(dictionary_page) => {
                dictionary_page.find_int32(index)
            }
            DictionaryPageEnum::DictionaryPageFloat32(dictionary_page) => {
                dictionary_page.find_int32(index)
            }
            DictionaryPageEnum::DictionaryPageInt64(dictionary_page) => {
                dictionary_page.find_int32(index)
            }
            DictionaryPageEnum::DictionaryPageFloat64(dictionary_page) => {
                dictionary_page.find_int32(index)
            }
            DictionaryPageEnum::DictionaryPageByteArray(dictionary_page) => {
                dictionary_page.find_int32(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt32(dictionary_page) => {
                dictionary_page.find_int32(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat32(dictionary_page) => {
                dictionary_page.find_int32(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt64(dictionary_page) => {
                dictionary_page.find_int32(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat64(dictionary_page) => {
                dictionary_page.find_int32(index)
            }
        }
    }

    fn find_float32(&self, index: usize) -> f32 {
        match self {
            DictionaryPageEnum::DictionaryPageInt32(dictionary_page) => {
                dictionary_page.find_float32(index)
            }
            DictionaryPageEnum::DictionaryPageFloat32(dictionary_page) => {
                dictionary_page.find_float32(index)
            }
            DictionaryPageEnum::DictionaryPageInt64(dictionary_page) => {
                dictionary_page.find_float32(index)
            }
            DictionaryPageEnum::DictionaryPageFloat64(dictionary_page) => {
                dictionary_page.find_float32(index)
            }
            DictionaryPageEnum::DictionaryPageByteArray(dictionary_page) => {
                dictionary_page.find_float32(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt32(dictionary_page) => {
                dictionary_page.find_float32(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat32(dictionary_page) => {
                dictionary_page.find_float32(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt64(dictionary_page) => {
                dictionary_page.find_float32(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat64(dictionary_page) => {
                dictionary_page.find_float32(index)
            }
        }
    }

    fn find_int64(&self, index: usize) -> i64 {
        match self {
            DictionaryPageEnum::DictionaryPageInt32(dictionary_page) => {
                dictionary_page.find_int64(index)
            }
            DictionaryPageEnum::DictionaryPageFloat32(dictionary_page) => {
                dictionary_page.find_int64(index)
            }
            DictionaryPageEnum::DictionaryPageInt64(dictionary_page) => {
                dictionary_page.find_int64(index)
            }
            DictionaryPageEnum::DictionaryPageFloat64(dictionary_page) => {
                dictionary_page.find_int64(index)
            }
            DictionaryPageEnum::DictionaryPageByteArray(dictionary_page) => {
                dictionary_page.find_int64(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt32(dictionary_page) => {
                dictionary_page.find_int64(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat32(dictionary_page) => {
                dictionary_page.find_int64(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt64(dictionary_page) => {
                dictionary_page.find_int64(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat64(dictionary_page) => {
                dictionary_page.find_int64(index)
            }
        }
    }

    fn find_float64(&self, index: usize) -> f64 {
        match self {
            DictionaryPageEnum::DictionaryPageInt32(dictionary_page) => {
                dictionary_page.find_float64(index)
            }
            DictionaryPageEnum::DictionaryPageFloat32(dictionary_page) => {
                dictionary_page.find_float64(index)
            }
            DictionaryPageEnum::DictionaryPageInt64(dictionary_page) => {
                dictionary_page.find_float64(index)
            }
            DictionaryPageEnum::DictionaryPageFloat64(dictionary_page) => {
                dictionary_page.find_float64(index)
            }
            DictionaryPageEnum::DictionaryPageByteArray(dictionary_page) => {
                dictionary_page.find_float64(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt32(dictionary_page) => {
                dictionary_page.find_float64(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat32(dictionary_page) => {
                dictionary_page.find_float64(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt64(dictionary_page) => {
                dictionary_page.find_float64(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat64(dictionary_page) => {
                dictionary_page.find_float64(index)
            }
        }
    }

    fn find_byte_array(&self, index: usize) -> ByteArray {
        match self {
            DictionaryPageEnum::DictionaryPageInt32(dictionary_page) => {
                dictionary_page.find_byte_array(index)
            }
            DictionaryPageEnum::DictionaryPageFloat32(dictionary_page) => {
                dictionary_page.find_byte_array(index)
            }
            DictionaryPageEnum::DictionaryPageInt64(dictionary_page) => {
                dictionary_page.find_byte_array(index)
            }
            DictionaryPageEnum::DictionaryPageFloat64(dictionary_page) => {
                dictionary_page.find_byte_array(index)
            }
            DictionaryPageEnum::DictionaryPageByteArray(dictionary_page) => {
                dictionary_page.find_byte_array(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt32(dictionary_page) => {
                dictionary_page.find_byte_array(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat32(dictionary_page) => {
                dictionary_page.find_byte_array(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt64(dictionary_page) => {
                dictionary_page.find_byte_array(index)
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat64(dictionary_page) => {
                dictionary_page.find_byte_array(index)
            }
        }
    }

    fn get_num_values(&self) -> usize {
        match self {
            DictionaryPageEnum::DictionaryPageInt32(dictionary_page) => {
                dictionary_page.get_num_values()
            }
            DictionaryPageEnum::DictionaryPageFloat32(dictionary_page) => {
                dictionary_page.get_num_values()
            }
            DictionaryPageEnum::DictionaryPageInt64(dictionary_page) => {
                dictionary_page.get_num_values()
            }
            DictionaryPageEnum::DictionaryPageFloat64(dictionary_page) => {
                dictionary_page.get_num_values()
            }
            DictionaryPageEnum::DictionaryPageByteArray(dictionary_page) => {
                dictionary_page.get_num_values()
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt32(dictionary_page) => {
                dictionary_page.get_num_values()
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat32(dictionary_page) => {
                dictionary_page.get_num_values()
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt64(dictionary_page) => {
                dictionary_page.get_num_values()
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat64(dictionary_page) => {
                dictionary_page.get_num_values()
            }
        }
    }

    fn get_type_size(&self) -> usize {
        match self {
            DictionaryPageEnum::DictionaryPageInt32(dictionary_page) => {
                dictionary_page.get_type_size()
            }
            DictionaryPageEnum::DictionaryPageFloat32(dictionary_page) => {
                dictionary_page.get_type_size()
            }
            DictionaryPageEnum::DictionaryPageInt64(dictionary_page) => {
                dictionary_page.get_type_size()
            }
            DictionaryPageEnum::DictionaryPageFloat64(dictionary_page) => {
                dictionary_page.get_type_size()
            }
            DictionaryPageEnum::DictionaryPageByteArray(dictionary_page) => {
                dictionary_page.get_type_size()
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt32(dictionary_page) => {
                dictionary_page.get_type_size()
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat32(dictionary_page) => {
                dictionary_page.get_type_size()
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt64(dictionary_page) => {
                dictionary_page.get_type_size()
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat64(dictionary_page) => {
                dictionary_page.get_type_size()
            }
        }
    }

    fn is_zero_copied(&self) -> bool {
        match self {
            DictionaryPageEnum::DictionaryPageInt32(dictionary_page) => {
                dictionary_page.is_zero_copied()
            }
            DictionaryPageEnum::DictionaryPageFloat32(dictionary_page) => {
                dictionary_page.is_zero_copied()
            }
            DictionaryPageEnum::DictionaryPageInt64(dictionary_page) => {
                dictionary_page.is_zero_copied()
            }
            DictionaryPageEnum::DictionaryPageFloat64(dictionary_page) => {
                dictionary_page.is_zero_copied()
            }
            DictionaryPageEnum::DictionaryPageByteArray(dictionary_page) => {
                dictionary_page.is_zero_copied()
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt32(dictionary_page) => {
                dictionary_page.is_zero_copied()
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat32(dictionary_page) => {
                dictionary_page.is_zero_copied()
            }
            DictionaryPageEnum::DictionaryPageWithFilterInt64(dictionary_page) => {
                dictionary_page.is_zero_copied()
            }
            DictionaryPageEnum::DictionaryPageWithFilterFloat64(dictionary_page) => {
                dictionary_page.is_zero_copied()
            }
        }
    }
}

pub trait DictionaryPage<T> {
    fn validate(&self, index: usize) -> bool;

    fn find(&self, index: usize) -> &T;

    fn get_num_values(&self) -> usize;

    fn get_type_size(&self) -> usize;

    fn is_zero_copied(&self) -> bool;
}
