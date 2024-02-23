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
    use crate::utils::row_range_set::{RowRangeSet, RowRangeSetGenerator};

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
}
