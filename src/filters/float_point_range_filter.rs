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
use std::ops::{BitAnd, BitOr};
use std::simd::{Mask, Simd, SimdPartialEq, SimdPartialOrd};

use crate::filters::filter::*;
use crate::filters::fixed_length_filter::FixedLengthRangeFilter;

pub struct FloatPointRangeFilter {
    filter_type: FilterType,
    lower_f64: f64,
    upper_f64: f64,
    lower_f32: f32,
    upper_f32: f32,
    lower_bounded: bool,
    upper_bounded: bool,
    lower_included: bool,
    upper_included: bool,
    null_allowed: bool,
}

#[allow(dead_code)]
impl std::fmt::Display for FloatPointRangeFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Float Point Filter, upper: {}, lower: {}, lower bounded: {}, upper bounded: {}, lower included: {}, upper included: {}, Null Allowed: {}",
            self.upper_f64, self.lower_f64, self.lower_bounded, self.upper_bounded, self. lower_included, self.upper_included, self.null_allowed,
        )
    }
}

impl FixedLengthRangeFilter for FloatPointRangeFilter {
    #[inline(always)]
    fn get_null_allowed(&self) -> bool {
        self.null_allowed
    }

    #[inline(always)]
    fn check_f64(&self, val: f64) -> bool {
        ((!self.upper_bounded)
            || (val < self.upper_f64)
            || (self.upper_included && val == self.upper_f64))
            && ((!self.lower_bounded)
                || (val > self.lower_f64)
                || (self.lower_included && val == self.lower_f64))
    }

    #[inline(always)]
    fn check_f32(&self, val: f32) -> bool {
        ((!self.upper_bounded)
            || (val < self.upper_f32)
            || (self.upper_included && val == self.upper_f32))
            && ((!self.lower_bounded)
                || (val > self.lower_f32)
                || (self.lower_included && val == self.lower_f32))
    }

    #[inline(always)]
    fn check_range_f32(&self, min: f32, max: f32, has_null: bool) -> bool {
        if has_null && self.null_allowed {
            return true;
        }

        !((self.upper_bounded
            && (min > self.upper_f32 || (!self.upper_included && min == self.upper_f32)))
            || (self.lower_bounded
                && (max < self.lower_f32 || (!self.lower_included && max == self.lower_f32))))
    }

    #[inline(always)]
    fn check_range_f64(&self, min: f64, max: f64, has_null: bool) -> bool {
        if has_null && self.null_allowed {
            return true;
        }

        !((self.upper_bounded
            && (min > self.upper_f64 || (!self.upper_included && min == self.upper_f64)))
            || (self.lower_bounded
                && (max < self.lower_f64 || (!self.lower_included && max == self.lower_f64))))
    }

    #[inline(always)]
    fn check_values_f64x8(&self, values: Simd<f64, 8>) -> Mask<i64, 8> {
        Mask::splat(!self.upper_bounded)
            .bitor(values.simd_lt(Simd::splat(self.upper_f64)))
            .bitor(
                Mask::splat(self.upper_included)
                    .bitand(values.simd_eq(Simd::splat(self.upper_f64))),
            )
            .bitand(
                Mask::splat(!self.lower_bounded)
                    .bitor(values.simd_gt(Simd::splat(self.lower_f64)))
                    .bitor(
                        Mask::splat(self.lower_included)
                            .bitand(values.simd_eq(Simd::splat(self.lower_f64))),
                    ),
            )
    }

    #[inline(always)]
    fn check_values_f64x4(&self, values: Simd<f64, 4>) -> Mask<i64, 4> {
        Mask::splat(!self.upper_bounded)
            .bitor(values.simd_lt(Simd::splat(self.upper_f64)))
            .bitor(
                Mask::splat(self.upper_included)
                    .bitand(values.simd_eq(Simd::splat(self.upper_f64))),
            )
            .bitand(
                Mask::splat(!self.lower_bounded)
                    .bitor(values.simd_gt(Simd::splat(self.lower_f64)))
                    .bitor(
                        Mask::splat(self.lower_included)
                            .bitand(values.simd_eq(Simd::splat(self.lower_f64))),
                    ),
            )
    }

    #[inline(always)]
    fn check_values_f32x16(&self, values: Simd<f32, 16>) -> Mask<i32, 16> {
        Mask::splat(!self.upper_bounded)
            .bitor(values.simd_lt(Simd::splat(self.upper_f32)))
            .bitor(
                Mask::splat(self.upper_included)
                    .bitand(values.simd_eq(Simd::splat(self.upper_f32))),
            )
            .bitand(
                Mask::splat(!self.lower_bounded)
                    .bitor(values.simd_gt(Simd::splat(self.lower_f32)))
                    .bitor(
                        Mask::splat(self.lower_included)
                            .bitand(values.simd_eq(Simd::splat(self.lower_f32))),
                    ),
            )
    }

    #[inline(always)]
    fn check_values_f32x8(&self, values: Simd<f32, 8>) -> Mask<i32, 8> {
        Mask::splat(!self.upper_bounded)
            .bitor(values.simd_lt(Simd::splat(self.upper_f32)))
            .bitor(
                Mask::splat(self.upper_included)
                    .bitand(values.simd_eq(Simd::splat(self.upper_f32))),
            )
            .bitand(
                Mask::splat(!self.lower_bounded)
                    .bitor(values.simd_gt(Simd::splat(self.lower_f32)))
                    .bitor(
                        Mask::splat(self.lower_included)
                            .bitand(values.simd_eq(Simd::splat(self.lower_f32))),
                    ),
            )
    }

    #[inline(always)]
    fn check_values_f32x4(&self, values: Simd<f32, 4>) -> Mask<i32, 4> {
        Mask::splat(!self.upper_bounded)
            .bitor(values.simd_lt(Simd::splat(self.upper_f32)))
            .bitor(
                Mask::splat(self.upper_included)
                    .bitand(values.simd_eq(Simd::splat(self.upper_f32))),
            )
            .bitand(
                Mask::splat(!self.lower_bounded)
                    .bitor(values.simd_gt(Simd::splat(self.lower_f32)))
                    .bitor(
                        Mask::splat(self.lower_included)
                            .bitand(values.simd_eq(Simd::splat(self.lower_f32))),
                    ),
            )
    }
}

impl FilterBasic for FloatPointRangeFilter {
    fn get_filter_type(&self) -> &FilterType {
        &self.filter_type
    }
}

#[allow(dead_code)]
impl FloatPointRangeFilter {
    pub fn new(
        lower: f64,
        upper: f64,
        lower_bounded: bool,
        upper_bounded: bool,
        lower_included: bool,
        upper_included: bool,
        null_allowed: bool,
    ) -> FloatPointRangeFilter {
        FloatPointRangeFilter {
            filter_type: FilterType::FloatPointRange,
            lower_f64: lower,
            upper_f64: upper,
            lower_f32: lower as f32,
            upper_f32: upper as f32,
            lower_bounded,
            upper_bounded,
            lower_included,
            upper_included,
            null_allowed,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::simd::{Mask, Simd};

    use crate::filters::filter::FilterBasic;
    use crate::filters::filter::FilterType::FloatPointRange;
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
    use crate::filters::float_point_range_filter::FloatPointRangeFilter;

    #[test]
    fn test_create_float_point_range_filter() {
        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, false, false, true, true);

        assert_eq!(*filter.get_filter_type(), FloatPointRange);
        assert_eq!(filter.get_null_allowed(), true);
        assert_eq!(
            filter.to_string(),
            String::from("Float Point Filter, upper: 2, lower: 1, lower bounded: true, upper bounded: false, lower included: false, upper included: true, Null Allowed: true\n")
        );
    }

    #[test]
    fn test_float_point_filter_f64_bounds() {
        let filter = FloatPointRangeFilter::new(1.0, 2.0, false, false, false, false, false);

        assert_eq!(filter.check_f64(0.0), true);
        assert_eq!(filter.check_f64(0.5), true);
        assert_eq!(filter.check_f64(1.0), true);
        assert_eq!(filter.check_f64(1.5), true);
        assert_eq!(filter.check_f64(2.0), true);
        assert_eq!(filter.check_f64(2.5), true);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, false, false, false, false);

        assert_eq!(filter.check_f64(0.0), false);
        assert_eq!(filter.check_f64(0.5), false);
        assert_eq!(filter.check_f64(1.0), false);
        assert_eq!(filter.check_f64(1.5), true);
        assert_eq!(filter.check_f64(2.0), true);
        assert_eq!(filter.check_f64(2.5), true);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, false, true, false, false, false);

        assert_eq!(filter.check_f64(0.0), true);
        assert_eq!(filter.check_f64(0.5), true);
        assert_eq!(filter.check_f64(1.0), true);
        assert_eq!(filter.check_f64(1.5), true);
        assert_eq!(filter.check_f64(2.0), false);
        assert_eq!(filter.check_f64(2.5), false);
    }

    #[test]
    fn test_float_point_filter_f32_bounds() {
        let filter = FloatPointRangeFilter::new(1.0, 2.0, false, false, false, false, false);

        assert_eq!(filter.check_f32(0.0), true);
        assert_eq!(filter.check_f32(0.5), true);
        assert_eq!(filter.check_f32(1.0), true);
        assert_eq!(filter.check_f32(1.5), true);
        assert_eq!(filter.check_f32(2.0), true);
        assert_eq!(filter.check_f32(2.5), true);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, false, false, false, false);

        assert_eq!(filter.check_f32(0.0), false);
        assert_eq!(filter.check_f32(0.5), false);
        assert_eq!(filter.check_f32(1.0), false);
        assert_eq!(filter.check_f32(1.5), true);
        assert_eq!(filter.check_f32(2.0), true);
        assert_eq!(filter.check_f32(2.5), true);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, false, true, false, false, false);

        assert_eq!(filter.check_f32(0.0), true);
        assert_eq!(filter.check_f32(0.5), true);
        assert_eq!(filter.check_f32(1.0), true);
        assert_eq!(filter.check_f32(1.5), true);
        assert_eq!(filter.check_f32(2.0), false);
        assert_eq!(filter.check_f32(2.5), false);
    }

    #[test]
    fn test_float_point_filter_f64_includes() {
        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, false, true, false, false);

        assert_eq!(filter.check_f64(0.0), false);
        assert_eq!(filter.check_f64(0.5), false);
        assert_eq!(filter.check_f64(1.0), true);
        assert_eq!(filter.check_f64(1.5), true);
        assert_eq!(filter.check_f64(2.0), true);
        assert_eq!(filter.check_f64(2.5), true);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, true, false, false);

        assert_eq!(filter.check_f64(0.0), false);
        assert_eq!(filter.check_f64(0.5), false);
        assert_eq!(filter.check_f64(1.0), true);
        assert_eq!(filter.check_f64(1.5), true);
        assert_eq!(filter.check_f64(2.0), false);
        assert_eq!(filter.check_f64(2.5), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, false, true, false, true, false);

        assert_eq!(filter.check_f64(0.0), true);
        assert_eq!(filter.check_f64(0.5), true);
        assert_eq!(filter.check_f64(1.0), true);
        assert_eq!(filter.check_f64(1.5), true);
        assert_eq!(filter.check_f64(2.0), true);
        assert_eq!(filter.check_f64(2.5), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, false, true, false);

        assert_eq!(filter.check_f64(0.0), false);
        assert_eq!(filter.check_f64(0.5), false);
        assert_eq!(filter.check_f64(1.0), false);
        assert_eq!(filter.check_f64(1.5), true);
        assert_eq!(filter.check_f64(2.0), true);
        assert_eq!(filter.check_f64(2.5), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, true, true, false);

        assert_eq!(filter.check_f64(0.0), false);
        assert_eq!(filter.check_f64(0.5), false);
        assert_eq!(filter.check_f64(1.0), true);
        assert_eq!(filter.check_f64(1.5), true);
        assert_eq!(filter.check_f64(2.0), true);
        assert_eq!(filter.check_f64(2.5), false);
    }

    #[test]
    fn test_float_point_filter_f32_includes() {
        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, false, true, false, false);

        assert_eq!(filter.check_f32(0.0), false);
        assert_eq!(filter.check_f32(0.5), false);
        assert_eq!(filter.check_f32(1.0), true);
        assert_eq!(filter.check_f32(1.5), true);
        assert_eq!(filter.check_f32(2.0), true);
        assert_eq!(filter.check_f32(2.5), true);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, true, false, false);

        assert_eq!(filter.check_f32(0.0), false);
        assert_eq!(filter.check_f32(0.5), false);
        assert_eq!(filter.check_f32(1.0), true);
        assert_eq!(filter.check_f32(1.5), true);
        assert_eq!(filter.check_f32(2.0), false);
        assert_eq!(filter.check_f32(2.5), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, false, true, false, true, false);

        assert_eq!(filter.check_f32(0.0), true);
        assert_eq!(filter.check_f32(0.5), true);
        assert_eq!(filter.check_f32(1.0), true);
        assert_eq!(filter.check_f32(1.5), true);
        assert_eq!(filter.check_f32(2.0), true);
        assert_eq!(filter.check_f32(2.5), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, false, true, false);

        assert_eq!(filter.check_f32(0.0), false);
        assert_eq!(filter.check_f32(0.5), false);
        assert_eq!(filter.check_f32(1.0), false);
        assert_eq!(filter.check_f32(1.5), true);
        assert_eq!(filter.check_f32(2.0), true);
        assert_eq!(filter.check_f32(2.5), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, true, true, false);

        assert_eq!(filter.check_f32(0.0), false);
        assert_eq!(filter.check_f32(0.5), false);
        assert_eq!(filter.check_f32(1.0), true);
        assert_eq!(filter.check_f32(1.5), true);
        assert_eq!(filter.check_f32(2.0), true);
        assert_eq!(filter.check_f32(2.5), false);
    }

    #[test]
    fn test_float_point_filter_f64_range() {
        let filter = FloatPointRangeFilter::new(1.0, 2.0, false, false, false, false, false);

        assert_eq!(filter.check_range_f64(0.0, 0.5, true), true);
        assert_eq!(filter.check_range_f64(0.5, 1.0, true), true);
        assert_eq!(filter.check_range_f64(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f64(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f64(2.0, 2.5, true), true);
        assert_eq!(filter.check_range_f64(2.5, 3.0, true), true);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, false, false, false, false);

        assert_eq!(filter.check_range_f64(0.0, 0.5, true), false);
        assert_eq!(filter.check_range_f64(0.5, 1.0, true), false);
        assert_eq!(filter.check_range_f64(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f64(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f64(2.0, 2.5, true), true);
        assert_eq!(filter.check_range_f64(2.5, 3.0, true), true);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, false, true, false, false);

        assert_eq!(filter.check_range_f64(0.0, 0.5, true), false);
        assert_eq!(filter.check_range_f64(0.5, 1.0, true), true);
        assert_eq!(filter.check_range_f64(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f64(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f64(2.0, 2.5, true), true);
        assert_eq!(filter.check_range_f64(2.5, 3.0, true), true);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, false, true, false, false, false);

        assert_eq!(filter.check_range_f64(0.0, 0.5, true), true);
        assert_eq!(filter.check_range_f64(0.5, 1.0, true), true);
        assert_eq!(filter.check_range_f64(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f64(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f64(2.0, 2.5, true), false);
        assert_eq!(filter.check_range_f64(2.5, 3.0, true), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, false, true, false, true, false);

        assert_eq!(filter.check_range_f64(0.0, 0.5, true), true);
        assert_eq!(filter.check_range_f64(0.5, 1.0, true), true);
        assert_eq!(filter.check_range_f64(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f64(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f64(2.0, 2.5, true), true);
        assert_eq!(filter.check_range_f64(2.5, 3.0, true), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, false, false, false);

        assert_eq!(filter.check_range_f64(0.0, 0.5, true), false);
        assert_eq!(filter.check_range_f64(0.5, 1.0, true), false);
        assert_eq!(filter.check_range_f64(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f64(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f64(2.0, 2.5, true), false);
        assert_eq!(filter.check_range_f64(2.5, 3.0, true), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, true, false, false);

        assert_eq!(filter.check_range_f64(0.0, 0.5, true), false);
        assert_eq!(filter.check_range_f64(0.5, 1.0, true), true);
        assert_eq!(filter.check_range_f64(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f64(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f64(2.0, 2.5, true), false);
        assert_eq!(filter.check_range_f64(2.5, 3.0, true), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, false, true, false);

        assert_eq!(filter.check_range_f64(0.0, 0.5, true), false);
        assert_eq!(filter.check_range_f64(0.5, 1.0, true), false);
        assert_eq!(filter.check_range_f64(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f64(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f64(2.0, 2.5, true), true);
        assert_eq!(filter.check_range_f64(2.5, 3.0, true), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, true, true, false);

        assert_eq!(filter.check_range_f64(0.0, 0.5, true), false);
        assert_eq!(filter.check_range_f64(0.5, 1.0, true), true);
        assert_eq!(filter.check_range_f64(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f64(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f64(2.0, 2.5, true), true);
        assert_eq!(filter.check_range_f64(2.5, 3.0, true), false);
    }

    #[test]
    fn test_float_point_filter_f32_range() {
        let filter = FloatPointRangeFilter::new(1.0, 2.0, false, false, false, false, false);

        assert_eq!(filter.check_range_f32(0.0, 0.5, true), true);
        assert_eq!(filter.check_range_f32(0.5, 1.0, true), true);
        assert_eq!(filter.check_range_f32(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f32(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f32(2.0, 2.5, true), true);
        assert_eq!(filter.check_range_f32(2.5, 3.0, true), true);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, false, false, false, false);

        assert_eq!(filter.check_range_f32(0.0, 0.5, true), false);
        assert_eq!(filter.check_range_f32(0.5, 1.0, true), false);
        assert_eq!(filter.check_range_f32(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f32(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f32(2.0, 2.5, true), true);
        assert_eq!(filter.check_range_f32(2.5, 3.0, true), true);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, false, true, false, false);

        assert_eq!(filter.check_range_f32(0.0, 0.5, true), false);
        assert_eq!(filter.check_range_f32(0.5, 1.0, true), true);
        assert_eq!(filter.check_range_f32(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f32(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f32(2.0, 2.5, true), true);
        assert_eq!(filter.check_range_f32(2.5, 3.0, true), true);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, false, true, false, false, false);

        assert_eq!(filter.check_range_f32(0.0, 0.5, true), true);
        assert_eq!(filter.check_range_f32(0.5, 1.0, true), true);
        assert_eq!(filter.check_range_f32(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f32(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f32(2.0, 2.5, true), false);
        assert_eq!(filter.check_range_f32(2.5, 3.0, true), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, false, true, false, true, false);

        assert_eq!(filter.check_range_f32(0.0, 0.5, true), true);
        assert_eq!(filter.check_range_f32(0.5, 1.0, true), true);
        assert_eq!(filter.check_range_f32(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f32(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f32(2.0, 2.5, true), true);
        assert_eq!(filter.check_range_f32(2.5, 3.0, true), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, false, false, false);

        assert_eq!(filter.check_range_f32(0.0, 0.5, true), false);
        assert_eq!(filter.check_range_f32(0.5, 1.0, true), false);
        assert_eq!(filter.check_range_f32(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f32(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f32(2.0, 2.5, true), false);
        assert_eq!(filter.check_range_f32(2.5, 3.0, true), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, true, false, false);

        assert_eq!(filter.check_range_f32(0.0, 0.5, true), false);
        assert_eq!(filter.check_range_f32(0.5, 1.0, true), true);
        assert_eq!(filter.check_range_f32(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f32(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f32(2.0, 2.5, true), false);
        assert_eq!(filter.check_range_f32(2.5, 3.0, true), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, false, true, false);

        assert_eq!(filter.check_range_f32(0.0, 0.5, true), false);
        assert_eq!(filter.check_range_f32(0.5, 1.0, true), false);
        assert_eq!(filter.check_range_f32(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f32(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f32(2.0, 2.5, true), true);
        assert_eq!(filter.check_range_f32(2.5, 3.0, true), false);

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, true, true, false);

        assert_eq!(filter.check_range_f32(0.0, 0.5, true), false);
        assert_eq!(filter.check_range_f32(0.5, 1.0, true), true);
        assert_eq!(filter.check_range_f32(1.0, 1.5, true), true);
        assert_eq!(filter.check_range_f32(1.5, 2.0, true), true);
        assert_eq!(filter.check_range_f32(2.0, 2.5, true), true);
        assert_eq!(filter.check_range_f32(2.5, 3.0, true), false);
    }

    #[test]
    fn test_float_point_simd_f64() {
        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, true, true, false);

        let arr = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5];
        assert_eq!(
            filter.check_values_f64x8(Simd::from_slice(&arr[0..])),
            Mask::from_array([false, false, true, true, true, false, false, false])
        );

        assert_eq!(
            filter.check_values_f64x4(Simd::from_slice(&arr[0..])),
            Mask::from_array([false, false, true, true])
        );

        assert_eq!(
            filter.check_values_f64x4(Simd::from_slice(&arr[4..])),
            Mask::from_array([true, false, false, false])
        );

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, false, false, false);

        let arr = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5];
        assert_eq!(
            filter.check_values_f64x8(Simd::from_slice(&arr[0..])),
            Mask::from_array([false, false, false, true, false, false, false, false])
        );

        assert_eq!(
            filter.check_values_f64x4(Simd::from_slice(&arr[0..])),
            Mask::from_array([false, false, false, true])
        );

        assert_eq!(
            filter.check_values_f64x4(Simd::from_slice(&arr[4..])),
            Mask::from_array([false, false, false, false])
        );

        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, false, true, false, false);

        let arr = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5];
        assert_eq!(
            filter.check_values_f64x8(Simd::from_slice(&arr[0..])),
            Mask::from_array([false, false, true, true, true, true, true, true])
        );

        assert_eq!(
            filter.check_values_f64x4(Simd::from_slice(&arr[0..])),
            Mask::from_array([false, false, true, true])
        );

        assert_eq!(
            filter.check_values_f64x4(Simd::from_slice(&arr[4..])),
            Mask::from_array([true, true, true, true])
        );
    }

    #[test]
    fn test_float_point_simd_f32() {
        let filter = FloatPointRangeFilter::new(1.0, 2.0, true, true, true, true, false);

        let arr = [
            -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0,
            3.25,
        ];
        assert_eq!(
            filter.check_values_f32x16(Simd::from_slice(&arr[0..])),
            Mask::from_array([
                false, false, false, false, false, false, true, true, true, true, true, false,
                false, false, false, false
            ])
        );

        assert_eq!(
            filter.check_values_f32x8(Simd::from_slice(&arr[0..])),
            Mask::from_array([false, false, false, false, false, false, true, true])
        );

        assert_eq!(
            filter.check_values_f32x8(Simd::from_slice(&arr[8..])),
            Mask::from_array([true, true, true, false, false, false, false, false])
        );

        assert_eq!(
            filter.check_values_f32x4(Simd::from_slice(&arr[0..])),
            Mask::from_array([false, false, false, false])
        );

        assert_eq!(
            filter.check_values_f32x4(Simd::from_slice(&arr[4..])),
            Mask::from_array([false, false, true, true])
        );

        assert_eq!(
            filter.check_values_f32x4(Simd::from_slice(&arr[8..])),
            Mask::from_array([true, true, true, false])
        );

        assert_eq!(
            filter.check_values_f32x4(Simd::from_slice(&arr[12..])),
            Mask::from_array([false, false, false, false])
        );
    }
}
