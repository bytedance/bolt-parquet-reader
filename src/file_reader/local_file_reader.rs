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

use std::collections::HashMap;

use crate::bridge::result_bridge::ResultBridgeEnum;
use crate::file_reader::row_group_reader::RowGroupReader;
use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
use crate::metadata::parquet_footer::FileMetaDataLoader;
use crate::metadata::parquet_metadata_thrift::FileMetaData;
use crate::metadata::utils::{prepare_schema, ColumnSchema};
use crate::utils::exceptions::BoltReaderError;
use crate::utils::local_file_loader::LocalFileLoader;
use crate::utils::shared_memory_buffer::SharedMemoryBuffer;

const DEFAULT_PARQUET_FOOTER_PRELOAD_SIZE: usize = 1 << 20;

pub struct LocalFileReader<'a> {
    path: String,
    column_schemas: HashMap<String, ColumnSchema>,
    columns_to_read: HashMap<String, Option<&'a dyn FixedLengthRangeFilter>>,
    footer: FileMetaData,
    row_group_index: usize,
    num_row_groups: usize,
    row_group_reader: Option<RowGroupReader<'a>>,
}

impl<'a> LocalFileReader<'a> {
    pub fn load_parquet_footer(file: &String) -> Result<FileMetaData, BoltReaderError> {
        let metadata_loader =
            FileMetaDataLoader::new(&String::from(file), DEFAULT_PARQUET_FOOTER_PRELOAD_SIZE);
        assert!(metadata_loader.is_ok());
        let mut metadata_loader = metadata_loader.unwrap();
        metadata_loader.load_parquet_footer()
    }

    pub fn from_local_file(
        path: &String,
        columns_to_read: HashMap<String, Option<&'a dyn FixedLengthRangeFilter>>,
    ) -> Result<LocalFileReader<'a>, BoltReaderError> {
        let footer = Self::load_parquet_footer(path)?;
        let column_schemas = prepare_schema(&footer.schema)?;
        let num_row_groups = footer.row_groups.len();

        Ok(LocalFileReader {
            path: path.clone(),
            column_schemas,
            columns_to_read,
            footer,
            row_group_index: 0,
            num_row_groups,
            row_group_reader: None,
        })
    }

    pub fn prepare_local_row_group_reader(&mut self) -> Result<bool, BoltReaderError> {
        if self.row_group_index >= self.num_row_groups {
            return Ok(true);
        }

        let file = LocalFileLoader::new(&self.path)?;

        let buffer_begin = match self.footer.row_groups[self.row_group_index].file_offset {
            None => {
                let first_column_metadata = self.footer.row_groups[self.row_group_index].columns[0]
                    .meta_data
                    .as_ref();
                match first_column_metadata {
                    None => {
                        return Err(BoltReaderError::FileReaderError(String::from(
                            "Unable to find the offset of the row group",
                        )));
                    }
                    Some(first_column_metadata) => {
                        let dictionary_page_offset =
                            first_column_metadata.dictionary_page_offset.as_ref();
                        match dictionary_page_offset {
                            None => first_column_metadata.data_page_offset,
                            Some(dictionary_page_offset) => *dictionary_page_offset,
                        }
                    }
                }
            }
            Some(buffer_begin) => buffer_begin,
        };

        let total_columns = self.footer.row_groups[self.row_group_index].columns.len();
        let buffer_end =
            self.footer.row_groups[self.row_group_index].columns[total_columns - 1].file_offset;

        let buffer = SharedMemoryBuffer::from_file(
            &file,
            buffer_begin as usize,
            (buffer_end - buffer_begin) as usize,
        )?;

        self.row_group_reader = Some(RowGroupReader::from_buffer(
            buffer,
            &self.footer.row_groups[self.row_group_index],
            &self.column_schemas,
            &self.columns_to_read,
        )?);

        Ok(false)
    }

    pub fn advance_to_next_row_group(&mut self) -> bool {
        self.row_group_index += 1;
        self.row_group_reader = None;

        self.row_group_index >= self.num_row_groups
    }

    fn skip_internal(&mut self, skip_size: &mut usize) -> Result<bool, BoltReaderError> {
        // No Row Group Reader is prepared
        if self.row_group_reader.is_none() {
            let row_group_size = self.footer.row_groups[self.row_group_index].num_rows as usize;

            // Skip the whole Row Group
            if row_group_size <= *skip_size {
                *skip_size -= row_group_size;
                self.row_group_index += 1;
                return Ok(self.row_group_index == self.num_row_groups);
            } else {
                self.prepare_local_row_group_reader()?;
            }

            Ok(false)
        } else {
            let row_group_reader = self.row_group_reader.as_mut().unwrap();
            if skip_size >= &mut row_group_reader.get_remaining_rows() {
                *skip_size -= row_group_reader.get_remaining_rows();
                self.row_group_reader = None;
                self.row_group_index += 1;

                Ok(false)
            } else {
                row_group_reader.skip(*skip_size)?;
                *skip_size = 0;

                Ok(true)
            }
        }
    }

    pub fn skip(&mut self, mut skip_size: usize) -> Result<bool, BoltReaderError> {
        let mut skip_finished = false;

        while !skip_finished {
            skip_finished = self.skip_internal(&mut skip_size)?;
        }

        // Is the whole file exhausted?
        if self.row_group_index >= self.num_row_groups {
            return Ok(true);
        }

        Ok(false)
    }

    #[allow(clippy::type_complexity)]
    pub fn read(
        &mut self,
        batch_size: usize,
    ) -> Result<(HashMap<String, ResultBridgeEnum>, bool), BoltReaderError> {
        if self.row_group_reader.is_none() {
            self.prepare_local_row_group_reader()?;
        }

        let row_group_reader = self.row_group_reader.as_mut().unwrap();

        let (result_bridge, _result_row_range_set, row_group_finished) =
            row_group_reader.read(batch_size)?;

        if row_group_finished {
            return Ok((result_bridge, self.advance_to_next_row_group()));
        }

        Ok((result_bridge, false))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::file_reader::local_file_reader::LocalFileReader;
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;

    const BOOLEAN_COLUMN: &str = "boolean";
    const INT32_COLUMN: &str = "int";
    const INT64_COLUMN: &str = "bigint";
    const FLOAT32_COLUMN: &str = "float";
    const FLOAT64_COLUMN: &str = "double";
    const STRING_COLUMN: &str = "string";

    #[test]
    fn test_reading_file() {
        let path = String::from("src/sample_files/plain_row_group.parquet");
        let batch_size = 10000;

        let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
            HashMap::new();
        columns_to_read.insert(String::from(BOOLEAN_COLUMN), None);
        columns_to_read.insert(String::from(INT32_COLUMN), None);
        columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
        columns_to_read.insert(String::from(INT64_COLUMN), None);
        columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
        columns_to_read.insert(String::from(STRING_COLUMN), None);

        let res = LocalFileReader::from_local_file(&path.to_string(), columns_to_read);
        assert!(res.is_ok());
        let mut file_reader = res.unwrap();

        let mut finished = false;

        while !finished {
            let res = file_reader.read(batch_size);
            assert!(res.is_ok());
            let res = res.unwrap();
            finished = res.1;
        }
    }

    #[test]
    fn test_skip() {
        let path = String::from("src/sample_files/plain_row_group.parquet");
        let batch_size = 10000;

        let mut skip_size = 100;

        for _i in 0..5 {
            let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> =
                HashMap::new();
            columns_to_read.insert(String::from(BOOLEAN_COLUMN), None);
            columns_to_read.insert(String::from(INT32_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT32_COLUMN), None);
            columns_to_read.insert(String::from(INT64_COLUMN), None);
            columns_to_read.insert(String::from(FLOAT64_COLUMN), None);
            columns_to_read.insert(String::from(STRING_COLUMN), None);

            let res = LocalFileReader::from_local_file(&path.to_string(), columns_to_read);
            assert!(res.is_ok());
            let mut file_reader = res.unwrap();

            let skip = skip_size;

            let mut finished = file_reader.skip(skip).unwrap();

            while !finished {
                let res = file_reader.read(batch_size);
                assert!(res.is_ok());
                let res = res.unwrap();
                finished = res.1;
            }
            skip_size = skip_size * 10;
        }
    }
}
