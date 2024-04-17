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

    fn get_num_values(&self) -> usize;

    fn get_type_size(&self) -> usize;

    fn is_zero_copied(&self) -> bool;
}

pub trait DictionaryPage<T> {
    fn validate(&self, index: usize) -> bool;

    fn find(&self, index: usize) -> &T;

    fn get_num_values(&self) -> usize;

    fn get_type_size(&self) -> usize;

    fn is_zero_copied(&self) -> bool;
}
