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
use std::intrinsics::unlikely;

use crate::utils::exceptions::BoltReaderError;

pub struct RowRange {
    pub begin: usize,
    pub end: usize,
}

#[allow(dead_code)]
impl std::fmt::Display for RowRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RowRange: [{}, {})", self.begin, self.end)
    }
}

impl RowRange {
    pub fn new(begin: usize, end: usize) -> RowRange {
        RowRange { begin, end }
    }

    #[inline(always)]
    pub fn get_length(&self) -> usize {
        self.end - self.begin
    }

    /// Get the truncated RowRange based on the input absolute begin and end indexes.
    ///
    /// # Arguments
    ///
    /// * `row_range_offset` - The offset for the self RowRange
    /// * `to_read_begin` - The absolute begin
    /// * `to_read_end` - The absolute end
    ///
    ///  For example
    ///  self: [1,10)
    ///  row_range_offset: 5
    ///  to_read_begin: 7
    ///  to_read_end: 10
    ///  Result: [2,5)
    pub fn get_covered_range(
        &self,
        row_range_offset: usize,
        to_read_begin: usize,
        to_read_end: usize,
    ) -> Result<Option<RowRange>, BoltReaderError> {
        let begin = self.begin + row_range_offset;
        let end = self.end + row_range_offset;

        if unlikely(to_read_begin >= to_read_end) {
            return Err(BoltReaderError::InternalError(format!(
                "Range processing error. Input begin {}, end {}",
                to_read_begin, to_read_end
            )));
        }

        if end <= to_read_begin || begin >= to_read_end {
            return Ok(None);
        }

        Ok(Some(RowRange::new(
            max(begin, to_read_begin) - row_range_offset,
            min(end, to_read_end) - row_range_offset,
        )))
    }

    /// Get the left remaining RowRange based on the input absolute begin and end indexes.
    ///
    /// # Arguments
    ///
    /// * `row_range_offset` - The offset for the self RowRange
    /// * `to_read_begin` - The absolute begin
    /// * `to_read_end` - The absolute end
    ///
    ///  For example
    ///  self: [1,10)
    ///  row_range_offset: 5
    ///  to_read_begin: 7
    ///  to_read_end: 10
    ///  Result: [1,2)
    #[allow(dead_code)]
    pub fn get_left_remaining_range(
        &self,
        row_range_offset: usize,
        to_read_begin: usize,
        to_read_end: usize,
    ) -> Result<Option<RowRange>, BoltReaderError> {
        let begin = self.begin + row_range_offset;
        let end = self.end + row_range_offset;

        if unlikely(to_read_begin >= to_read_end) {
            return Err(BoltReaderError::InternalError(format!(
                "Range processing error. Input begin {}, end {}",
                to_read_begin, to_read_end
            )));
        }

        if to_read_begin <= begin {
            return Ok(None);
        }

        Ok(Some(RowRange::new(
            begin - row_range_offset,
            min(end, to_read_begin) - row_range_offset,
        )))
    }

    /// Get the right remaining RowRange based on the input absolute begin and end indexes.
    ///
    /// # Arguments
    ///
    /// * `row_range_offset` - The offset for the self RowRange
    /// * `to_read_begin` - The absolute begin
    /// * `to_read_end` - The absolute end
    ///
    ///  For example
    ///  self: [1,10)
    ///  row_range_offset: 5
    ///  to_read_begin: 7
    ///  to_read_end: 10
    ///  Result: [5,10)
    pub fn get_right_remaining_range(
        &self,
        row_range_offset: usize,
        to_read_begin: usize,
        to_read_end: usize,
    ) -> Result<Option<RowRange>, BoltReaderError> {
        let begin = self.begin + row_range_offset;
        let end = self.end + row_range_offset;

        if unlikely(to_read_begin >= to_read_end) {
            return Err(BoltReaderError::InternalError(format!(
                "Range processing error. Input begin {}, end {}",
                to_read_begin, to_read_end
            )));
        }

        if to_read_end >= end {
            return Ok(None);
        }

        Ok(Some(RowRange::new(
            max(begin, to_read_end) - row_range_offset,
            end - row_range_offset,
        )))
    }
}

pub struct RowRangeSet {
    offset: usize,
    row_ranges: Vec<RowRange>,
}

#[allow(dead_code)]
impl std::fmt::Display for RowRangeSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let row_range_str = self
            .row_ranges
            .iter()
            .take(10)
            .map(RowRange::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        writeln!(
            f,
            "RowRangeSet Offset: {}, Ranges: {} ... (showing only the first 10 ranges)",
            self.offset, row_range_str,
        )
    }
}

impl RowRangeSet {
    pub fn new(offset: usize) -> RowRangeSet {
        RowRangeSet {
            offset,
            row_ranges: Default::default(),
        }
    }

    pub fn get_offset(&self) -> usize {
        self.offset
    }

    pub fn get_row_ranges(&self) -> &Vec<RowRange> {
        &self.row_ranges
    }

    pub fn add_row_ranges(&mut self, begin: usize, end: usize) {
        self.row_ranges.push(RowRange::new(begin, end));
    }
}

pub struct RowRangeSetGenerator<'a> {
    first_true: Option<usize>,
    row_range_set: &'a mut RowRangeSet,
}

impl<'a> RowRangeSetGenerator<'a> {
    pub fn new(row_range_set: &'a mut RowRangeSet) -> RowRangeSetGenerator {
        RowRangeSetGenerator {
            first_true: None,
            row_range_set,
        }
    }

    #[inline(always)]
    pub fn update(&mut self, index: usize, value: bool) {
        if value != self.first_true.is_some() {
            match self.first_true {
                None => self.first_true = Option::from(index),
                Some(first) => {
                    self.row_range_set.add_row_ranges(first, index);
                    self.first_true = None;
                }
            }
        }
    }

    pub fn finish(&mut self, last: usize) {
        if self.first_true.is_some() {
            self.row_range_set
                .add_row_ranges(self.first_true.unwrap(), last);
            self.first_true = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::row_range_set::{RowRange, RowRangeSet, RowRangeSetGenerator};

    #[test]
    fn test_create_row_range_set() {
        let mut row_range_set = RowRangeSet::new(3);
        row_range_set.add_row_ranges(0, 10);
        assert_eq!(row_range_set.get_offset(), 3);
        assert_eq!(row_range_set.get_row_ranges().len(), 1);
        assert_eq!(row_range_set.get_row_ranges()[0].begin, 0);
        assert_eq!(row_range_set.get_row_ranges()[0].end, 10);

        row_range_set.add_row_ranges(15, 20);
        assert_eq!(row_range_set.get_offset(), 3);
        assert_eq!(row_range_set.get_row_ranges().len(), 2);
        assert_eq!(row_range_set.get_row_ranges()[1].begin, 15);
        assert_eq!(row_range_set.get_row_ranges()[1].end, 20);
    }

    #[test]
    fn test_row_range_set_generator() {
        let offset = 3;

        let mut row_range_set = RowRangeSet::new(offset);
        let mut generator = RowRangeSetGenerator::new(&mut row_range_set);

        let vec = Vec::from([false, true, false, false, true, true, false, true, true]);
        for i in 0..vec.len() {
            generator.update(i, vec[i]);
        }
        generator.finish(vec.len());

        assert_eq!(row_range_set.get_offset(), 3);
        assert_eq!(row_range_set.get_row_ranges().len(), 3);
        assert_eq!(row_range_set.get_row_ranges()[0].begin, 1);
        assert_eq!(row_range_set.get_row_ranges()[0].end, 2);
        assert_eq!(row_range_set.get_row_ranges()[1].begin, 4);
        assert_eq!(row_range_set.get_row_ranges()[1].end, 6);
        assert_eq!(row_range_set.get_row_ranges()[2].begin, 7);
        assert_eq!(row_range_set.get_row_ranges()[2].end, 9);
    }

    #[test]
    fn test_row_range_set_generator_all_false() {
        let offset = 3;

        let mut row_range_set = RowRangeSet::new(offset);
        let mut generator = RowRangeSetGenerator::new(&mut row_range_set);

        let vec = Vec::from([false, false, false, false, false, false, false]);
        for i in 0..vec.len() {
            generator.update(i, vec[i]);
        }
        generator.finish(vec.len());

        assert_eq!(row_range_set.get_offset(), 3);
        assert_eq!(row_range_set.get_row_ranges().len(), 0);
    }

    #[test]
    fn test_row_range_set_generator_all_true() {
        let offset = 3;

        let mut row_range_set = RowRangeSet::new(offset);
        let mut generator = RowRangeSetGenerator::new(&mut row_range_set);

        let vec = Vec::from([true, true, true, true, true, true, true]);
        for i in 0..vec.len() {
            generator.update(i, vec[i]);
        }
        generator.finish(vec.len());

        assert_eq!(row_range_set.get_offset(), 3);
        assert_eq!(row_range_set.get_row_ranges().len(), 1);
        assert_eq!(row_range_set.get_row_ranges()[0].begin, 0);
        assert_eq!(row_range_set.get_row_ranges()[0].end, 7);
    }

    #[test]
    fn test_row_range_set_generator_end_with_true() {
        let offset = 3;

        let mut row_range_set = RowRangeSet::new(offset);
        let mut generator = RowRangeSetGenerator::new(&mut row_range_set);

        let vec = Vec::from([false, false, false, false, true, true, true]);
        for i in 0..vec.len() {
            generator.update(i, vec[i]);
        }
        generator.finish(vec.len());

        assert_eq!(row_range_set.get_offset(), 3);
        assert_eq!(row_range_set.get_row_ranges().len(), 1);
        assert_eq!(row_range_set.get_row_ranges()[0].begin, 4);
        assert_eq!(row_range_set.get_row_ranges()[0].end, 7);
    }

    #[test]
    fn test_get_covered_row_range() {
        // row_range is [6,15)
        let row_range = RowRange::new(1, 10);
        let row_range_offset = 5;

        let res = row_range.get_covered_range(row_range_offset, 2, 5);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_none());

        let res = row_range.get_covered_range(row_range_offset, 16, 19);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_none());

        let res = row_range.get_covered_range(row_range_offset, 7, 10);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_some());
        let res_row_range = res.unwrap();
        assert_eq!(res_row_range.begin, 2);
        assert_eq!(res_row_range.end, 5);

        let res = row_range.get_covered_range(row_range_offset, 3, 7);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_some());
        let res_row_range = res.unwrap();
        assert_eq!(res_row_range.begin, 1);
        assert_eq!(res_row_range.end, 2);

        let res = row_range.get_covered_range(row_range_offset, 13, 17);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_some());
        let res_row_range = res.unwrap();
        assert_eq!(res_row_range.begin, 8);
        assert_eq!(res_row_range.end, 10);

        let res = row_range.get_covered_range(row_range_offset, 5, 2);
        assert!(res.is_err());
    }

    #[test]
    fn test_get_left_remaining_row_range() {
        // row_range is [6,15)
        let row_range = RowRange::new(1, 10);
        let row_range_offset = 5;

        let res = row_range.get_left_remaining_range(row_range_offset, 2, 5);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_none());

        let res = row_range.get_left_remaining_range(row_range_offset, 3, 8);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_none());

        let res = row_range.get_left_remaining_range(row_range_offset, 7, 10);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_some());
        let res_row_range = res.unwrap();
        assert_eq!(res_row_range.begin, 1);
        assert_eq!(res_row_range.end, 2);

        let res = row_range.get_left_remaining_range(row_range_offset, 13, 17);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_some());
        let res_row_range = res.unwrap();
        assert_eq!(res_row_range.begin, 1);
        assert_eq!(res_row_range.end, 8);

        let res = row_range.get_left_remaining_range(row_range_offset, 5, 2);
        assert!(res.is_err());
    }

    #[test]
    fn test_get_right_remaining_row_range() {
        // row_range is [6,15)
        let row_range = RowRange::new(1, 10);
        let row_range_offset = 5;

        let res = row_range.get_right_remaining_range(row_range_offset, 13, 18);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_none());

        let res = row_range.get_right_remaining_range(row_range_offset, 16, 19);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_none());

        let res = row_range.get_right_remaining_range(row_range_offset, 7, 10);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_some());
        let res_row_range = res.unwrap();
        assert_eq!(res_row_range.begin, 5);
        assert_eq!(res_row_range.end, 10);

        let res = row_range.get_right_remaining_range(row_range_offset, 3, 7);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_some());
        let res_row_range = res.unwrap();
        assert_eq!(res_row_range.begin, 2);
        assert_eq!(res_row_range.end, 10);

        let res = row_range.get_right_remaining_range(row_range_offset, 5, 2);
        assert!(res.is_err());
    }
}
