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

#[cfg(test)]
pub mod test_utils {
    use crate::bridge::result_bridge::ResultBridge;
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
    use crate::utils::row_range_set::RowRangeSet;

    pub fn verify_boolean_non_null_result(
        result_row_range_set: &RowRangeSet,
        boolean_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = boolean_bridge
                    .get_bool_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();

                assert_eq!(validity, true);
                if i % 4 == 0 {
                    assert_eq!(value, true);
                } else {
                    assert_eq!(value, false);
                }

                if let Some(filter) = filter {
                    assert!(filter.check_bool(value));
                }
            }
        }
    }

    pub fn verify_boolean_nullable_result(
        result_row_range_set: &RowRangeSet,
        boolean_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = boolean_bridge
                    .get_bool_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();

                if i % 5 == 0 || i % 17 == 0 {
                    assert_eq!(validity, false);
                } else {
                    assert_eq!(validity, true);
                    if i % 4 == 0 {
                        assert_eq!(value, true);
                    } else {
                        assert_eq!(value, false);
                    }
                }
                if let Some(filter) = filter {
                    assert!(filter.check_bool_with_validity(value, validity));
                }
            }
        }
    }

    pub fn verify_plain_float32_non_null_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_float32_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();

                assert_eq!(validity, true);
                assert_eq!(i as f32, value);

                if let Some(filter) = filter {
                    assert!(filter.check_f32_with_validity(value, validity));
                }
            }
        }
    }

    pub fn verify_plain_float32_nullable_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_float32_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();

                if i % 5 == 0 || i % 17 == 0 {
                    assert_eq!(validity, false);
                } else {
                    assert_eq!(validity, true);
                    assert_eq!(i as f32, value);
                }
                if let Some(filter) = filter {
                    assert!(filter.check_f32(value));
                }
            }
        }
    }

    pub fn verify_plain_float64_non_null_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_float64_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();

                assert_eq!(validity, true);
                assert_eq!(i as f64, value);

                if let Some(filter) = filter {
                    assert!(filter.check_f64_with_validity(value, validity));
                }
            }
        }
    }

    pub fn verify_plain_float64_nullable_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_float64_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();

                if i % 5 == 0 || i % 17 == 0 {
                    assert_eq!(validity, false);
                } else {
                    assert_eq!(validity, true);
                    assert_eq!(i as f64, value);
                }
                if let Some(filter) = filter {
                    assert!(filter.check_f64(value));
                }
            }
        }
    }

    pub fn verify_plain_int32_non_null_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_int32_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();

                assert_eq!(validity, true);
                assert_eq!(i as i32, value);

                if let Some(filter) = filter {
                    assert!(filter.check_i32_with_validity(value, validity));
                }
            }
        }
    }

    pub fn verify_plain_int32_nullable_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_int32_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();

                if i % 5 == 0 || i % 17 == 0 {
                    assert_eq!(validity, false);
                } else {
                    assert_eq!(validity, true);
                    assert_eq!(i as i32, value);
                }
                if let Some(filter) = filter {
                    assert!(filter.check_i32(value));
                }
            }
        }
    }

    pub fn verify_plain_int64_non_null_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_int64_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();

                assert_eq!(validity, true);
                assert_eq!(i as i64, value);

                if let Some(filter) = filter {
                    assert!(filter.check_i64(value));
                }
            }
        }
    }

    pub fn verify_plain_int64_nullable_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_int64_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();

                if i % 5 == 0 || i % 17 == 0 {
                    assert_eq!(validity, false);
                } else {
                    assert_eq!(validity, true);
                    assert_eq!(i as i64, value);
                }
                if let Some(filter) = filter {
                    assert!(filter.check_i64_with_validity(value, validity));
                }
            }
        }
    }

    pub fn verify_rle_bp_float32_non_null_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                assert_eq!(
                    raw_bridge
                        .get_float32_validity_and_value(offset, i, &result_row_range_set)
                        .unwrap(),
                    (true, (i % 1000) as f32)
                );
                if let Some(filter) = filter {
                    assert!(filter.check_f32((i % 1000) as f32));
                }
            }
        }
    }

    pub fn verify_rle_bp_float32_nullable_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_float32_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();
                if i % 5 == 0 || i % 17 == 0 {
                    assert_eq!(validity, false);
                } else {
                    assert_eq!((validity, value), (true, (i % 1000) as f32));
                }
                if let Some(filter) = filter {
                    assert!(filter.check_f32_with_validity(value, validity));
                }
            }
        }
    }

    pub fn verify_rle_bp_float64_non_null_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                assert_eq!(
                    raw_bridge
                        .get_float64_validity_and_value(offset, i, &result_row_range_set)
                        .unwrap(),
                    (true, (i % 1000) as f64)
                );
                if let Some(filter) = filter {
                    assert!(filter.check_f64((i % 1000) as f64));
                }
            }
        }
    }

    pub fn verify_rle_bp_float64_nullable_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_float64_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();
                if i % 5 == 0 || i % 17 == 0 {
                    assert_eq!(validity, false);
                } else {
                    assert_eq!((validity, value), (true, (i % 1000) as f64));
                }
                if let Some(filter) = filter {
                    assert!(filter.check_f64_with_validity(value, validity));
                }
            }
        }
    }

    pub fn verify_rle_bp_int32_non_null_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                assert_eq!(
                    raw_bridge
                        .get_int32_validity_and_value(offset, i, &result_row_range_set)
                        .unwrap(),
                    (true, (i % 1000) as i32)
                );
                if let Some(filter) = filter {
                    assert!(filter.check_i32((i % 1000) as i32));
                }
            }
        }
    }

    pub fn verify_rle_bp_int32_nullable_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_int32_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();
                if i % 5 == 0 || i % 17 == 0 {
                    assert_eq!(validity, false);
                } else {
                    assert_eq!((validity, value), (true, (i % 1000) as i32));
                }
                if let Some(filter) = filter {
                    assert!(filter.check_i32_with_validity(value, validity));
                }
            }
        }
    }

    pub fn verify_rle_bp_int64_non_null_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                assert_eq!(
                    raw_bridge
                        .get_int64_validity_and_value(offset, i, &result_row_range_set)
                        .unwrap(),
                    (true, (i % 1000) as i64)
                );
                if let Some(filter) = filter {
                    assert!(filter.check_i64((i % 1000) as i64));
                }
            }
        }
    }

    pub fn verify_rle_bp_int64_nullable_result(
        result_row_range_set: &RowRangeSet,
        raw_bridge: &dyn ResultBridge,
        filter: Option<&dyn FixedLengthRangeFilter>,
    ) {
        let offset = result_row_range_set.get_offset();
        for row_range in result_row_range_set.get_row_ranges() {
            for i in row_range.begin..row_range.end {
                let (validity, value) = raw_bridge
                    .get_int64_validity_and_value(offset, i, &result_row_range_set)
                    .unwrap();
                if i % 5 == 0 || i % 17 == 0 {
                    assert_eq!(validity, false);
                } else {
                    assert_eq!((validity, value), (true, (i % 1000) as i64));
                }
                if let Some(filter) = filter {
                    assert!(filter.check_i64_with_validity(value, validity));
                }
            }
        }
    }
}
