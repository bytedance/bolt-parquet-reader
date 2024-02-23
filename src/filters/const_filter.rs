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

use std::fmt::Formatter;
use std::simd::{Mask, Simd};

use crate::filters::filter::*;

pub struct ConstFilter {
    filter_type: FilterType,
    value: bool,
}

#[allow(dead_code)]
impl std::fmt::Display for ConstFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Const Filter, value: {}", self.value)
    }
}

#[allow(dead_code)]
impl ConstFilter {
    pub fn new(value: bool) -> ConstFilter {
        ConstFilter {
            filter_type: FilterType::ConstFilter,
            value,
        }
    }

    #[inline(always)]
    fn get_filter_type(&self) -> &FilterType {
        &self.filter_type
    }

    #[inline(always)]
    fn check_null(&self) -> bool {
        self.value
    }

    #[inline(always)]
    fn check_non_null(&self) -> bool {
        self.value
    }

    #[inline(always)]
    fn check_i32(&self, _: i32) -> bool {
        self.value
    }

    #[inline(always)]
    fn check_i64(&self, _: i64) -> bool {
        self.value
    }

    #[inline(always)]
    fn check_i128(&self, _: i128) -> bool {
        self.value
    }

    #[inline(always)]
    fn check_f64(&self, _: f64) -> bool {
        self.value
    }

    #[inline(always)]
    fn check_f32(&self, _: f32) -> bool {
        self.value
    }

    #[inline(always)]
    fn check_bytes(&self, _: Vec<u8>) -> bool {
        self.value
    }

    #[inline(always)]
    fn check_values_i64x8(&self, _: Simd<i64, 8>) -> Mask<i64, 8> {
        Mask::splat(self.value)
    }

    #[inline(always)]
    fn check_values_i64x4(&self, _: Simd<i64, 4>) -> Mask<i64, 4> {
        Mask::splat(self.value)
    }

    #[inline(always)]
    fn check_values_i32x16(&self, _: Simd<i32, 16>) -> Mask<i32, 16> {
        Mask::splat(self.value)
    }

    #[inline(always)]
    fn check_values_i32x8(&self, _: Simd<i32, 8>) -> Mask<i32, 8> {
        Mask::splat(self.value)
    }

    #[inline(always)]
    fn check_values_i32x4(&self, _: Simd<i32, 4>) -> Mask<i32, 4> {
        Mask::splat(self.value)
    }

    #[inline(always)]
    fn check_values_f64x8(&self, _: Simd<f64, 8>) -> Mask<i64, 8> {
        Mask::splat(self.value)
    }

    #[inline(always)]
    fn check_values_f64x4(&self, _: Simd<f64, 4>) -> Mask<i64, 4> {
        Mask::splat(self.value)
    }

    #[inline(always)]
    fn check_values_f32x16(&self, _: Simd<f32, 16>) -> Mask<i32, 16> {
        Mask::splat(self.value)
    }

    #[inline(always)]
    fn check_values_f32x8(&self, _: Simd<f32, 8>) -> Mask<i32, 8> {
        Mask::splat(self.value)
    }

    #[inline(always)]
    fn check_values_f32x4(&self, _: Simd<f32, 4>) -> Mask<i32, 4> {
        Mask::splat(self.value)
    }
}

#[cfg(test)]
mod tests {
    use std::simd::Simd;

    use crate::filters::const_filter;

    #[test]
    fn test_always_false_filter() {
        let filter = const_filter::ConstFilter::new(false);

        assert_eq!(
            filter.to_string(),
            String::from("Const Filter, value: false\n")
        );
        assert_eq!(filter.check_null(), false);
        assert_eq!(filter.check_non_null(), false);
        assert_eq!(filter.check_i32(0), false);
        assert_eq!(filter.check_i64(0), false);
        assert_eq!(filter.check_i128(0), false);
        assert_eq!(filter.check_f64(0.0), false);
        assert_eq!(filter.check_f32(0.0), false);
        assert_eq!(filter.check_bytes(Vec::new()), false);

        let simd_results_4 = [false; 4];
        let simd_results_8 = [false; 8];
        let simd_results_16 = [false; 16];

        assert_eq!(
            filter.check_values_i64x8(Simd::splat(0)).to_array(),
            simd_results_8
        );
        assert_eq!(
            filter.check_values_i64x4(Simd::splat(0)).to_array(),
            simd_results_4
        );
        assert_eq!(
            filter.check_values_i32x16(Simd::splat(0)).to_array(),
            simd_results_16
        );
        assert_eq!(
            filter.check_values_i32x8(Simd::splat(0)).to_array(),
            simd_results_8
        );
        assert_eq!(
            filter.check_values_i32x4(Simd::splat(0)).to_array(),
            simd_results_4
        );

        assert_eq!(
            filter.check_values_f64x8(Simd::splat(0.0)).to_array(),
            simd_results_8
        );
        assert_eq!(
            filter.check_values_f64x4(Simd::splat(0.0)).to_array(),
            simd_results_4
        );
        assert_eq!(
            filter.check_values_f32x16(Simd::splat(0.0)).to_array(),
            simd_results_16
        );
        assert_eq!(
            filter.check_values_f32x8(Simd::splat(0.0)).to_array(),
            simd_results_8
        );
        assert_eq!(
            filter.check_values_f32x4(Simd::splat(0.0)).to_array(),
            simd_results_4
        );
    }

    #[test]
    fn test_always_true_filter() {
        let filter = const_filter::ConstFilter::new(true);
        assert_eq!(
            filter.to_string(),
            String::from("Const Filter, value: true\n")
        );
        assert_eq!(filter.check_null(), true);
        assert_eq!(filter.check_non_null(), true);
        assert_eq!(filter.check_i32(0), true);
        assert_eq!(filter.check_i64(0), true);
        assert_eq!(filter.check_i128(0), true);
        assert_eq!(filter.check_f64(0.0), true);
        assert_eq!(filter.check_f32(0.0), true);
        assert_eq!(filter.check_bytes(Vec::new()), true);

        let simd_results_4 = [true; 4];
        let simd_results_8 = [true; 8];
        let simd_results_16 = [true; 16];

        assert_eq!(
            filter.check_values_i64x8(Simd::splat(0)).to_array(),
            simd_results_8
        );
        assert_eq!(
            filter.check_values_i64x4(Simd::splat(0)).to_array(),
            simd_results_4
        );
        assert_eq!(
            filter.check_values_i32x16(Simd::splat(0)).to_array(),
            simd_results_16
        );
        assert_eq!(
            filter.check_values_i32x8(Simd::splat(0)).to_array(),
            simd_results_8
        );
        assert_eq!(
            filter.check_values_i32x4(Simd::splat(0)).to_array(),
            simd_results_4
        );

        assert_eq!(
            filter.check_values_f64x8(Simd::splat(0.0)).to_array(),
            simd_results_8
        );
        assert_eq!(
            filter.check_values_f64x4(Simd::splat(0.0)).to_array(),
            simd_results_4
        );
        assert_eq!(
            filter.check_values_f32x16(Simd::splat(0.0)).to_array(),
            simd_results_16
        );
        assert_eq!(
            filter.check_values_f32x8(Simd::splat(0.0)).to_array(),
            simd_results_8
        );
        assert_eq!(
            filter.check_values_f32x4(Simd::splat(0.0)).to_array(),
            simd_results_4
        );
    }
}
