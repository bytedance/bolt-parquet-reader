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

use std::simd::{Mask, Simd};

use crate::filters::filter::FilterBasic;

pub trait FixedLengthRangeFilter: FilterBasic + std::fmt::Display {
    #[inline(always)]
    fn get_null_allowed(&self) -> bool {
        false
    }

    #[inline(always)]
    fn check_bool(&self, _: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_null(&self, _: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_i32(&self, _: i32) -> bool {
        false
    }

    #[inline(always)]
    fn check_i64(&self, _: i64) -> bool {
        false
    }

    #[inline(always)]
    fn check_i128(&self, _: i128) -> bool {
        false
    }

    #[inline(always)]
    fn check_f64(&self, _: f64) -> bool {
        false
    }

    #[inline(always)]
    fn check_f32(&self, _: f32) -> bool {
        false
    }

    #[inline(always)]
    fn check_bool_with_validity(&self, _: bool, _: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_i32_with_validity(&self, _: i32, _: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_i64_with_validity(&self, _: i64, _: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_i128_with_validity(&self, _: i128, _: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_f64_with_validity(&self, _: f64, _: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_f32_with_validity(&self, _: f32, _: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_range_i32(&self, _min: i32, _max: i32, _has_null: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_range_i64(&self, _min: i64, _max: i64, _has_null: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_range_i128(&self, _min: i128, _max: i128, _has_null: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_range_f32(&self, _min: f32, _max: f32, _has_null: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_range_f64(&self, _min: f64, _max: f64, _has_null: bool) -> bool {
        false
    }

    #[inline(always)]
    fn check_values_i64x8(&self, _: Simd<i64, 8>) -> Mask<i64, 8> {
        Mask::splat(false)
    }

    #[inline(always)]
    fn check_values_i64x4(&self, _: Simd<i64, 4>) -> Mask<i64, 4> {
        Mask::splat(false)
    }

    #[inline(always)]
    fn check_values_i32x16(&self, _: Simd<i32, 16>) -> Mask<i32, 16> {
        Mask::splat(false)
    }

    #[inline(always)]
    fn check_values_i32x8(&self, _: Simd<i32, 8>) -> Mask<i32, 8> {
        Mask::splat(false)
    }

    #[inline(always)]
    fn check_values_i32x4(&self, _: Simd<i32, 4>) -> Mask<i32, 4> {
        Mask::splat(false)
    }

    #[inline(always)]
    fn check_values_f64x8(&self, _: Simd<f64, 8>) -> Mask<i64, 8> {
        Mask::splat(false)
    }

    #[inline(always)]
    fn check_values_f64x4(&self, _: Simd<f64, 4>) -> Mask<i64, 4> {
        Mask::splat(false)
    }

    #[inline(always)]
    fn check_values_f32x16(&self, _: Simd<f32, 16>) -> Mask<i32, 16> {
        Mask::splat(false)
    }

    #[inline(always)]
    fn check_values_f32x8(&self, _: Simd<f32, 8>) -> Mask<i32, 8> {
        Mask::splat(false)
    }

    #[inline(always)]
    fn check_values_f32x4(&self, _: Simd<f32, 4>) -> Mask<i32, 4> {
        Mask::splat(false)
    }
}

#[cfg(test)]
mod tests {
    use crate::filters::filter::FilterType::{FloatPointRange, IntegerRange};
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
    use crate::filters::float_point_range_filter::FloatPointRangeFilter;
    use crate::filters::integer_range_filter::IntegerRangeFilter;

    fn get_integer_filter() -> Box<dyn FixedLengthRangeFilter> {
        Box::new(IntegerRangeFilter::new(1, 2, true))
    }

    fn get_float_point_filter() -> Box<dyn FixedLengthRangeFilter> {
        Box::new(FloatPointRangeFilter::new(
            1.0, 2.0, true, true, false, false, true,
        ))
    }

    #[test]
    fn test_integer_filter() {
        let filter = get_integer_filter();
        assert_eq!(*filter.get_filter_type(), IntegerRange);
        assert_eq!(filter.get_null_allowed(), true);
        assert_eq!(filter.check_i64(0), false);
        assert_eq!(filter.check_i64(1), true);
        assert_eq!(filter.check_f32(1.0), false);
        assert_eq!(
            filter.to_string(),
            String::from("Integer Filter, upper: 2, lower: 1, Null Allowed: true\n")
        );
    }

    #[test]
    fn test_float_point_filter() {
        let filter = get_float_point_filter();
        assert_eq!(*filter.get_filter_type(), FloatPointRange);
        assert_eq!(filter.get_null_allowed(), true);
        assert_eq!(filter.check_f64(1.0), false);
        assert_eq!(filter.check_f64(1.5), true);
        assert_eq!(filter.check_f64(2.0), false);
        assert_eq!(filter.check_f32(1.0), false);
        assert_eq!(filter.check_f32(1.5), true);
        assert_eq!(filter.check_f32(2.0), false);

        assert_eq!(filter.check_i32(1), false);
        assert_eq!(filter.check_i32(2), false);

        assert_eq!(
            filter.to_string(),
            String::from("Float Point Filter, upper: 2, lower: 1, lower bounded: true, upper bounded: true, lower included: false, upper included: false, Null Allowed: true\n")
        );
    }
}
