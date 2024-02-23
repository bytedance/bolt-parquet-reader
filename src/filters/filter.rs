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

use std::fmt::{Debug, Formatter};

pub enum FilterType {
    ConstFilter,
    IntegerRange,
    FloatPointRange,
}

impl Debug for FilterType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterType::ConstFilter => {
                writeln!(f, "ConstFilter")
            }
            FilterType::IntegerRange => {
                writeln!(f, "IntegerRange")
            }
            FilterType::FloatPointRange => {
                writeln!(f, "FloatPointRange")
            }
        }
    }
}

impl PartialEq for FilterType {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (self::FilterType::ConstFilter, self::FilterType::ConstFilter)
                | (
                    self::FilterType::IntegerRange,
                    self::FilterType::IntegerRange
                )
                | (
                    self::FilterType::FloatPointRange,
                    self::FilterType::FloatPointRange
                )
        )
    }
}

pub trait FilterBasic {
    fn get_filter_type(&self) -> &FilterType;
}

#[cfg(test)]
mod tests {
    use crate::filters::filter::FilterType;

    #[test]
    fn test_filter_type_compare() {
        assert_eq!(FilterType::ConstFilter, FilterType::ConstFilter);
        assert_eq!(FilterType::IntegerRange, FilterType::IntegerRange);
        assert_eq!(FilterType::FloatPointRange, FilterType::FloatPointRange);

        assert_ne!(FilterType::ConstFilter, FilterType::IntegerRange);
        assert_ne!(FilterType::IntegerRange, FilterType::FloatPointRange);
        assert_ne!(FilterType::FloatPointRange, FilterType::ConstFilter);
    }
}
