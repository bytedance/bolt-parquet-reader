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

use std::cmp::{max, min};
use std::fmt::Formatter;
use std::ops::BitAnd;
use std::simd::cmp::SimdPartialOrd;
use std::simd::{Mask, Simd};

use crate::filters::filter::*;
use crate::filters::fixed_length_filter::FixedLengthRangeFilter;

pub struct IntegerRangeFilter {
    filter_type: FilterType,
    lower_i128: i128,
    upper_i128: i128,
    lower_i64: i64,
    upper_i64: i64,
    always_false_i64: bool,
    lower_i32: i32,
    upper_i32: i32,
    always_false_i32: bool,
    null_allowed: bool,
}

#[allow(dead_code)]
impl std::fmt::Display for IntegerRangeFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Integer Filter, upper: {}, lower: {}, Null Allowed: {}",
            self.upper_i128, self.lower_i128, self.null_allowed,
        )
    }
}

impl FixedLengthRangeFilter for IntegerRangeFilter {
    #[inline(always)]
    fn get_null_allowed(&self) -> bool {
        self.null_allowed
    }

    #[inline(always)]
    fn check_i32(&self, value: i32) -> bool {
        !self.always_false_i32 && value >= self.lower_i32 && value <= self.upper_i32
    }

    #[inline(always)]
    fn check_i64(&self, value: i64) -> bool {
        !self.always_false_i64 && value >= self.lower_i64 && value <= self.upper_i64
    }

    #[inline(always)]
    fn check_i128(&self, value: i128) -> bool {
        value >= self.lower_i128 && value <= self.upper_i128
    }

    #[inline(always)]
    fn check_range_i32(&self, min: i32, max: i32, has_null: bool) -> bool {
        if has_null && self.null_allowed {
            return true;
        }
        !(min > self.upper_i32 || max < self.lower_i32)
    }

    #[inline(always)]
    fn check_range_i64(&self, min: i64, max: i64, has_null: bool) -> bool {
        if has_null && self.null_allowed {
            return true;
        }
        !(min > self.upper_i64 || max < self.lower_i64)
    }

    #[inline(always)]
    fn check_range_i128(&self, min: i128, max: i128, has_null: bool) -> bool {
        if has_null && self.null_allowed {
            return true;
        }
        !(min > self.upper_i128 || max < self.lower_i128)
    }

    #[inline(always)]
    fn check_values_i64x8(&self, values: Simd<i64, 8>) -> Mask<i64, 8> {
        let low = values.simd_ge(Simd::splat(self.lower_i64));
        let up = values.simd_le(Simd::splat(self.upper_i64));
        low.bitand(up).bitand(Mask::splat(!self.always_false_i64))
    }

    #[inline(always)]
    fn check_values_i64x4(&self, values: Simd<i64, 4>) -> Mask<i64, 4> {
        let low = values.simd_ge(Simd::splat(self.lower_i64));
        let up = values.simd_le(Simd::splat(self.upper_i64));
        low.bitand(up).bitand(Mask::splat(!self.always_false_i64))
    }

    #[inline(always)]
    fn check_values_i32x16(&self, values: Simd<i32, 16>) -> Mask<i32, 16> {
        let low = values.simd_ge(Simd::splat(self.lower_i32));
        let up = values.simd_le(Simd::splat(self.upper_i32));
        low.bitand(up).bitand(Mask::splat(!self.always_false_i32))
    }

    #[inline(always)]
    fn check_values_i32x8(&self, values: Simd<i32, 8>) -> Mask<i32, 8> {
        let low = values.simd_ge(Simd::splat(self.lower_i32));
        let up = values.simd_le(Simd::splat(self.upper_i32));
        low.bitand(up).bitand(Mask::splat(!self.always_false_i32))
    }

    #[inline(always)]
    fn check_values_i32x4(&self, values: Simd<i32, 4>) -> Mask<i32, 4> {
        let low = values.simd_ge(Simd::splat(self.lower_i32));
        let up = values.simd_le(Simd::splat(self.upper_i32));
        low.bitand(up).bitand(Mask::splat(!self.always_false_i32))
    }
}

impl FilterBasic for IntegerRangeFilter {
    fn get_filter_type(&self) -> &FilterType {
        &self.filter_type
    }
}

#[allow(dead_code)]
impl IntegerRangeFilter {
    pub fn new(lower: i128, upper: i128, null_allowed: bool) -> IntegerRangeFilter {
        IntegerRangeFilter {
            filter_type: FilterType::IntegerRange,
            lower_i128: lower,
            upper_i128: upper,
            lower_i64: (max(lower, i64::MIN as i128) as i64),
            upper_i64: (min(upper, i64::MAX as i128) as i64),
            always_false_i64: (lower > i64::MAX as i128 || upper < i64::MIN as i128),
            lower_i32: (max(lower, i32::MIN as i128) as i32),
            upper_i32: (min(upper, i32::MAX as i128) as i32),
            always_false_i32: (lower > i32::MAX as i128 || upper < i32::MIN as i128),
            null_allowed,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::simd::{Mask, Simd};

    use crate::filters::filter::FilterBasic;
    use crate::filters::filter::FilterType::IntegerRange;
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
    use crate::filters::integer_range_filter;
    use crate::filters::integer_range_filter::IntegerRangeFilter;

    #[test]
    fn test_create_integer_filter() {
        let filter = IntegerRangeFilter::new(1, 2, true);

        assert_eq!(*filter.get_filter_type(), IntegerRange);
        assert_eq!(filter.get_null_allowed(), true);
        assert_eq!(
            filter.to_string(),
            String::from("Integer Filter, upper: 2, lower: 1, Null Allowed: true\n")
        );
    }

    #[test]
    fn test_integer_filter_i128() {
        let filter =
            integer_range_filter::IntegerRangeFilter::new(i128::MAX / 2, i128::MAX - 1, true);

        assert_eq!(filter.check_i128(i128::MAX / 2 + 1), true);
        assert_eq!(filter.check_i64(i64::MAX / 2 + 1), false);
        assert_eq!(filter.check_i32(i32::MAX / 2 + 1), false);
    }

    #[test]
    fn test_integer_filter_i64() {
        let filter = integer_range_filter::IntegerRangeFilter::new(
            (i64::MAX / 2) as i128,
            (i64::MAX - 1) as i128,
            true,
        );

        assert_eq!(filter.check_i128(i128::MAX / 2 + 1), false);
        assert_eq!(filter.check_i64(i64::MAX / 2 + 1), true);
        assert_eq!(filter.check_i32(i32::MAX / 2 + 1), false);
    }

    #[test]
    fn test_integer_filter_i32() {
        let filter = integer_range_filter::IntegerRangeFilter::new(
            (i32::MAX / 2) as i128,
            (i32::MAX - 1) as i128,
            true,
        );

        assert_eq!(filter.check_i128(i128::MAX / 2 + 1), false);
        assert_eq!(filter.check_i64(i64::MAX / 2 + 1), false);
        assert_eq!(filter.check_i32(i32::MAX / 2 + 1), true);
    }

    #[test]
    fn test_equal() {
        let filter = integer_range_filter::IntegerRangeFilter::new(1, 1, true);

        assert_eq!(filter.check_i128(1), true);
        assert_eq!(filter.check_i64(1), true);
        assert_eq!(filter.check_i32(1), true);
    }

    #[test]
    fn test_simd_i64() {
        let filter = integer_range_filter::IntegerRangeFilter::new(
            (i64::MAX / 2) as i128,
            (i64::MAX - 1) as i128,
            true,
        );
        let mid = i64::MAX / 2;
        let arr = [
            mid - 3,
            mid - 2,
            mid - 1,
            mid,
            mid + 1,
            mid + 2,
            mid + 3,
            mid + 4,
        ];

        assert_eq!(
            Mask::from_array([false, false, false, true, true, true, true, true]),
            filter.check_values_i64x8(Simd::from_slice(&arr[0..]))
        );
        assert_eq!(
            Mask::from_array([false, false, false, true]),
            filter.check_values_i64x4(Simd::from_slice(&arr[0..]))
        );
        assert_eq!(
            Mask::from_array([true, true, true, true]),
            filter.check_values_i64x4(Simd::from_slice(&arr[4..]))
        );
    }

    #[test]
    fn test_simd_i32() {
        let filter = integer_range_filter::IntegerRangeFilter::new(
            (i32::MAX / 2) as i128,
            (i32::MAX - 1) as i128,
            true,
        );
        let mid = i32::MAX / 2;
        let arr = [
            mid - 7,
            mid - 6,
            mid - 5,
            mid - 4,
            mid - 3,
            mid - 2,
            mid - 1,
            mid,
            mid + 1,
            mid + 2,
            mid + 3,
            mid + 4,
            mid + 5,
            mid + 6,
            mid + 7,
            mid + 8,
        ];

        assert_eq!(
            filter
                .check_values_i32x16(Simd::from_slice(&arr[0..]))
                .to_array(),
            [
                false, false, false, false, false, false, false, true, true, true, true, true,
                true, true, true, true
            ]
        );
        assert_eq!(
            filter
                .check_values_i32x8(Simd::from_slice(&arr[0..]))
                .to_array(),
            [false, false, false, false, false, false, false, true]
        );
        assert_eq!(
            filter
                .check_values_i32x8(Simd::from_slice(&arr[8..]))
                .to_array(),
            [true, true, true, true, true, true, true, true]
        );

        assert_eq!(
            filter
                .check_values_i32x4(Simd::from_slice(&arr[3..]))
                .to_array(),
            [false, false, false, false]
        );
        assert_eq!(
            filter
                .check_values_i32x4(Simd::from_slice(&arr[9..]))
                .to_array(),
            [true, true, true, true]
        );
    }

    #[test]
    fn test_range_i128() {
        let filter = integer_range_filter::IntegerRangeFilter::new(
            (i128::MAX / 2) as i128,
            (i128::MAX - 1) as i128,
            true,
        );
        let lower = i128::MAX / 2 - 1;
        let upper = i128::MAX / 2;

        assert_eq!(filter.check_range_i128(lower, upper, true), true);
        assert_eq!(filter.check_range_i128(lower - 1, upper + 1, true), true);
        assert_eq!(filter.check_range_i128(lower, upper, false), true);
        assert_eq!(filter.check_range_i128(lower - 2, lower - 1, false), false);
    }

    #[test]
    fn test_range_i64() {
        let filter = integer_range_filter::IntegerRangeFilter::new(
            (i64::MAX / 2) as i128,
            (i64::MAX - 1) as i128,
            true,
        );
        let lower = i64::MAX / 2 - 1;
        let upper = i64::MAX / 2;

        assert_eq!(filter.check_range_i64(lower, upper, true), true);
        assert_eq!(filter.check_range_i64(lower - 1, upper + 1, true), true);
        assert_eq!(filter.check_range_i64(lower, upper, false), true);
        assert_eq!(filter.check_range_i64(lower - 2, lower - 1, false), false);
    }

    #[test]
    fn test_range_i32() {
        let filter = integer_range_filter::IntegerRangeFilter::new(
            (i32::MAX / 2) as i128,
            (i32::MAX - 1) as i128,
            true,
        );
        let lower = i32::MAX / 2 - 1;
        let upper = i32::MAX / 2;

        assert_eq!(filter.check_range_i32(lower, upper, true), true);
        assert_eq!(filter.check_range_i32(lower - 1, upper + 1, true), true);
        assert_eq!(filter.check_range_i32(lower, upper, false), true);
        assert_eq!(filter.check_range_i32(lower - 2, lower - 1, false), false);
    }

    #[test]
    fn test_float() {
        let filter = integer_range_filter::IntegerRangeFilter::new(1, 1, true);

        assert_eq!(filter.check_f32(1.0), false);
        assert_eq!(filter.check_f64(1.0), false);
    }

    #[test]
    fn test_float_range() {
        let filter = integer_range_filter::IntegerRangeFilter::new(1, 1, true);

        assert_eq!(filter.check_range_f32(1.0, 2.0, true), false);
        assert_eq!(filter.check_range_f64(1.0, 2.0, true), false);
    }
}
