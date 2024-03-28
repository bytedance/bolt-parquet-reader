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

use crate::filters::filter::{FilterBasic, FilterType};
use crate::filters::fixed_length_filter::FixedLengthRangeFilter;

pub struct BooleanFilter {
    filter_type: FilterType,
    value: bool,
    null_allowed: bool,
}

#[allow(dead_code)]
impl std::fmt::Display for BooleanFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Boolean Filter, value: {}, Null Allowed: {}",
            self.value, self.null_allowed,
        )
    }
}

impl FixedLengthRangeFilter for BooleanFilter {
    #[inline(always)]
    fn get_null_allowed(&self) -> bool {
        self.null_allowed
    }

    #[inline(always)]
    fn check_null(&self, validity: bool) -> bool {
        validity || self.null_allowed
    }

    #[inline(always)]
    fn check_bool(&self, value: bool) -> bool {
        value == self.value
    }

    #[inline(always)]
    fn check_bool_with_validity(&self, value: bool, validity: bool) -> bool {
        (validity && self.check_bool(value)) || (self.null_allowed && !validity)
    }
}

impl FilterBasic for BooleanFilter {
    fn get_filter_type(&self) -> &FilterType {
        &self.filter_type
    }
}

#[allow(dead_code)]
impl BooleanFilter {
    pub fn new(value: bool, null_allowed: bool) -> BooleanFilter {
        BooleanFilter {
            filter_type: FilterType::Boolean,
            value,
            null_allowed,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::filters::boolean_filter::BooleanFilter;
    use crate::filters::filter::FilterBasic;
    use crate::filters::filter::FilterType::Boolean;
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;

    #[test]
    fn test_create_boolean_filter() {
        let filter = BooleanFilter::new(true, true);

        assert_eq!(*filter.get_filter_type(), Boolean);
        assert_eq!(filter.get_null_allowed(), true);
        assert_eq!(
            filter.to_string(),
            String::from("Boolean Filter, value: true, Null Allowed: true\n")
        );
    }

    #[test]
    fn test_null() {
        let nullable_filter = BooleanFilter::new(true, true);

        assert_eq!(nullable_filter.check_null(true), true);
        assert_eq!(nullable_filter.check_null(false), true);
        let non_null_filter = BooleanFilter::new(true, false);
        assert_eq!(non_null_filter.check_null(true), true);
        assert_eq!(non_null_filter.check_null(false), false);
    }

    #[test]
    fn test_bool_values() {
        let true_filter = BooleanFilter::new(true, false);
        assert_eq!(true_filter.check_bool(true), true);
        assert_eq!(true_filter.check_bool(false), false);

        let false_filter = BooleanFilter::new(false, false);
        assert_eq!(false_filter.check_bool(true), false);
        assert_eq!(false_filter.check_bool(false), true);
    }

    #[test]
    fn test_nullable_true_values() {
        let nullable_filter = BooleanFilter::new(true, true);
        assert_eq!(nullable_filter.check_bool_with_validity(true, true), true);
        assert_eq!(nullable_filter.check_bool_with_validity(true, false), true);
        assert_eq!(nullable_filter.check_bool_with_validity(false, true), false);
        assert_eq!(nullable_filter.check_bool_with_validity(false, false), true);

        let non_null_filter = BooleanFilter::new(true, false);
        assert_eq!(non_null_filter.check_bool_with_validity(true, true), true);
        assert_eq!(non_null_filter.check_bool_with_validity(true, false), false);
        assert_eq!(non_null_filter.check_bool_with_validity(false, true), false);
        assert_eq!(
            non_null_filter.check_bool_with_validity(false, false),
            false
        );
    }

    #[test]
    fn test_nullable_false_values() {
        let nullable_filter = BooleanFilter::new(false, true);
        assert_eq!(nullable_filter.check_bool_with_validity(true, true), false);
        assert_eq!(nullable_filter.check_bool_with_validity(true, false), true);
        assert_eq!(nullable_filter.check_bool_with_validity(false, true), true);
        assert_eq!(nullable_filter.check_bool_with_validity(false, false), true);

        let non_null_filter = BooleanFilter::new(false, false);
        assert_eq!(non_null_filter.check_bool_with_validity(true, true), false);
        assert_eq!(non_null_filter.check_bool_with_validity(true, false), false);
        assert_eq!(non_null_filter.check_bool_with_validity(false, true), true);
        assert_eq!(
            non_null_filter.check_bool_with_validity(false, false),
            false
        );
    }
}
