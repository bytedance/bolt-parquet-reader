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
use std::intrinsics::unlikely;
use std::rc::Rc;

use crate::bridge::result_bridge::ResultBridge;
use crate::column_reader::column_reader_base::{ColumnReader, PhysicalDataType};
use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
use crate::metadata::page_header::read_page_header;
use crate::metadata::parquet_metadata_thrift::{ColumnMetaData, Encoding, PageHeader, Type};
use crate::page_reader::data_page_v1::boolean_data_page_v1::BooleanDataPageReaderV1;
use crate::page_reader::data_page_v1::data_page_base::{
    get_data_page_covered_range, get_data_page_remaining_range, DataPage, DataPageEnum,
};
use crate::page_reader::data_page_v1::plain_data_page_float32_v1::PlainDataPageReaderFloat32V1;
use crate::page_reader::data_page_v1::plain_data_page_float64_v1::PlainDataPageReaderFloat64V1;
use crate::page_reader::data_page_v1::plain_data_page_int32_v1::PlainDataPageReaderInt32V1;
use crate::page_reader::data_page_v1::plain_data_page_int64_v1::PlainDataPageReaderInt64V1;
use crate::page_reader::data_page_v1::rle_bp_data_page_float32_v1::RleBpDataPageReaderFloat32V1;
use crate::page_reader::data_page_v1::rle_bp_data_page_float64_v1::RleBpDataPageReaderFloat64V1;
use crate::page_reader::data_page_v1::rle_bp_data_page_int32_v1::RleBpDataPageReaderInt32V1;
use crate::page_reader::data_page_v1::rle_bp_data_page_int64_v1::RleBpDataPageReaderInt64V1;
use crate::page_reader::dictionary_page::dictionary_page_base::DictionaryPageEnum;
use crate::page_reader::dictionary_page::dictionary_page_float32::DictionaryPageFloat32;
use crate::page_reader::dictionary_page::dictionary_page_float32_with_filters::DictionaryPageWithFilterFloat32;
use crate::page_reader::dictionary_page::dictionary_page_float64::DictionaryPageFloat64;
use crate::page_reader::dictionary_page::dictionary_page_float64_with_filters::DictionaryPageWithFilterFloat64;
use crate::page_reader::dictionary_page::dictionary_page_int32::DictionaryPageInt32;
use crate::page_reader::dictionary_page::dictionary_page_int32_with_filters::DictionaryPageWithFilterInt32;
use crate::page_reader::dictionary_page::dictionary_page_int64::DictionaryPageInt64;
use crate::page_reader::dictionary_page::dictionary_page_int64_with_filters::DictionaryPageWithFilterInt64;
use crate::utils::byte_buffer_base::{BufferEnum, ByteBufferBase};
use crate::utils::exceptions::BoltReaderError;
use crate::utils::rep_def_parser::RepDefParser;
use crate::utils::row_range_set::{RowRange, RowRangeSet};

pub struct PrimitiveColumnReader<'a> {
    physical_data_type: PhysicalDataType,
    num_values: usize,
    reading_index: usize,
    data_page_offset: usize,
    bytes_read: usize,
    type_size: usize,
    max_rep: u32,
    max_def: u32,
    column_size: usize,
    column_name: String,
    buffer: BufferEnum<'a>,
    dictionary_page: Option<Rc<DictionaryPageEnum>>,
    current_data_page_header: Option<PageHeader>,
    current_data_page: Option<DataPageEnum<'a>>,
    filter: Option<&'a dyn FixedLengthRangeFilter>,
}

#[allow(dead_code)]
impl<'a> std::fmt::Display for PrimitiveColumnReader<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Fixed Length Column: num_values {}, has dictionary {}",
            self.num_values,
            self.dictionary_page.is_some()
        )
    }
}

impl<'a> ColumnReader for PrimitiveColumnReader<'a> {
    fn get_physical_type(&self) -> &PhysicalDataType {
        &self.physical_data_type
    }

    fn get_column_num_values(&self) -> usize {
        self.num_values
    }

    fn get_data_type_size(&self) -> usize {
        self.type_size
    }

    fn get_column_name(&self) -> &String {
        &self.column_name
    }

    fn read(
        &mut self,
        mut to_read: RowRange,
        to_read_offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<(), BoltReaderError> {
        if to_read.get_length() == 0 {
            return Ok(());
        }

        if unlikely(to_read.begin + to_read_offset < self.reading_index) {
            return Err(BoltReaderError::PrimitiveColumnReaderError(format!(
                "Primitive Column Reader: Cannot read backward. Current reading index: {}, attemp to read: {}", self.reading_index, to_read.begin + to_read_offset
            )));
        }

        let mut finished = false;
        while !finished {
            if self.current_data_page.is_none() {
                self.prepare_data_page()?;
            }

            let page_header = self.current_data_page_header.as_ref().unwrap();
            let data_page_header = page_header.data_page_header.as_ref().unwrap();

            let covered_range = get_data_page_covered_range(
                self.data_page_offset,
                self.data_page_offset + data_page_header.num_values as usize,
                to_read_offset,
                &to_read,
            )?;

            let remaining_range = get_data_page_remaining_range(
                self.data_page_offset,
                self.data_page_offset + data_page_header.num_values as usize,
                to_read_offset,
                &to_read,
            )?;

            let mut need_next_page = true;
            if let Some(covered_range) = covered_range {
                if self.current_data_page.is_none() {
                    self.load_data_page()?;
                }
                let data_page = self.current_data_page.as_mut().unwrap();

                if !result_bridge.may_has_null() && data_page.data_page_has_null() {
                    result_bridge.set_may_has_null(true);
                }

                self.reading_index = covered_range.end + to_read_offset;
                need_next_page = data_page.read(
                    covered_range,
                    to_read_offset,
                    result_row_range_set,
                    result_bridge,
                )?;
            }

            finished = remaining_range.is_none();

            if let Some(remaining_range) = remaining_range {
                to_read = remaining_range;
            }

            if need_next_page && !self.get_next_data_page()? && !finished {
                return Err(BoltReaderError::PrimitiveColumnReaderError(String::from(
                    "Reading outside of the column",
                )));
            }
        }

        Ok(())
    }

    fn read_with_filter(
        &mut self,
        mut to_read: RowRange,
        to_read_offset: usize,
        result_row_range_set: &mut RowRangeSet,
        result_bridge: &mut dyn ResultBridge,
    ) -> Result<(), BoltReaderError> {
        if self.filter.is_none() {
            return Err(BoltReaderError::PrimitiveColumnReaderError(String::from(
                "Filter doesn't exist for this column",
            )));
        }

        if to_read.get_length() == 0 {
            return Ok(());
        }

        if unlikely(to_read.begin + to_read_offset < self.reading_index) {
            return Err(BoltReaderError::PrimitiveColumnReaderError(format!(
                "Fixed Length Column Reader: Cannot read backward. Current reading index: {}, attemp to read: {}", self.reading_index, to_read.begin + to_read_offset
            )));
        }

        let mut finished = false;
        while !finished {
            if self.current_data_page.is_none() {
                self.prepare_data_page()?;
            }

            let page_header = self.current_data_page_header.as_ref().unwrap();
            let data_page_header = page_header.data_page_header.as_ref().unwrap();

            let covered_range = get_data_page_covered_range(
                self.data_page_offset,
                self.data_page_offset + data_page_header.num_values as usize,
                to_read_offset,
                &to_read,
            )?;

            let remaining_range = get_data_page_remaining_range(
                self.data_page_offset,
                self.data_page_offset + data_page_header.num_values as usize,
                to_read_offset,
                &to_read,
            )?;

            let mut need_next_page = true;

            if let Some(covered_range) = covered_range {
                if self.current_data_page.is_none() {
                    self.load_data_page()?;
                }
                let data_page = self.current_data_page.as_mut().unwrap();

                if !result_bridge.may_has_null() && data_page.data_page_has_null() {
                    result_bridge.set_may_has_null(true);
                }

                self.reading_index = covered_range.end + to_read_offset;
                need_next_page = data_page.read_with_filter(
                    covered_range,
                    to_read_offset,
                    result_row_range_set,
                    result_bridge,
                )?;
            }

            finished = remaining_range.is_none();

            if let Some(remaining_range) = remaining_range {
                to_read = remaining_range;
            }

            if need_next_page && !self.get_next_data_page()? && !finished {
                return Err(BoltReaderError::PrimitiveColumnReaderError(String::from(
                    "Reading outside of the column",
                )));
            }
        }

        Ok(())
    }
}

impl<'a> PrimitiveColumnReader<'a> {
    pub fn new(
        max_rep: u32,
        max_def: u32,
        column_name: String,
        buffer: BufferEnum<'a>,
        filter: Option<&'a dyn FixedLengthRangeFilter>,
    ) -> Result<PrimitiveColumnReader<'a>, BoltReaderError> {
        Ok(PrimitiveColumnReader {
            physical_data_type: PhysicalDataType::None,
            num_values: 0,
            reading_index: 0,
            data_page_offset: 0,
            bytes_read: 0,
            type_size: 0,
            max_rep,
            max_def,
            column_size: 0,
            column_name,
            buffer,
            dictionary_page: None,
            current_data_page_header: Option::None,
            current_data_page: Option::None,
            filter,
        })
    }

    pub fn prepare_column_reader(
        &mut self,
        column_meta_data: &ColumnMetaData,
    ) -> Result<(), BoltReaderError> {
        self.num_values = column_meta_data.num_values as usize;
        self.column_size = column_meta_data.total_compressed_size as usize;

        self.physical_data_type = match column_meta_data.type_ {
            Type::BOOLEAN => PhysicalDataType::Boolean,
            Type::INT32 => PhysicalDataType::Int32,
            Type::INT64 => PhysicalDataType::Int64,
            Type::FLOAT => PhysicalDataType::Float32,
            Type::DOUBLE => PhysicalDataType::Float64,
            _ => {
                return Err(BoltReaderError::NotYetImplementedError(format!(
                    "Primitive Type Column: Type {} not yet implemented",
                    column_meta_data.type_.0
                )))
            }
        };

        self.type_size = match column_meta_data.type_ {
            Type::BOOLEAN => 1,
            Type::INT32 | Type::FLOAT => 4,
            Type::INT64 | Type::DOUBLE => 8,
            _ => {
                return Err(BoltReaderError::NotYetImplementedError(format!(
                    "Primitive Type Column: Type {} not yet implemented",
                    column_meta_data.type_.0
                )))
            }
        };

        match column_meta_data.dictionary_page_offset {
            None => None,
            Some(_) => {
                let rpos = self.buffer.get_rpos();
                let first_page_header = read_page_header(&mut self.buffer)?;
                if first_page_header.dictionary_page_header.is_some() {
                    self.dictionary_page = match self.physical_data_type {
                        PhysicalDataType::Int32 => match self.filter {
                            None => Some(Rc::from(DictionaryPageEnum::DictionaryPageInt32(
                                DictionaryPageInt32::new(&first_page_header, &mut self.buffer, 4)?,
                            ))),
                            Some(filter) => {
                                Some(Rc::from(DictionaryPageEnum::DictionaryPageWithFilterInt32(
                                    DictionaryPageWithFilterInt32::new(
                                        &first_page_header,
                                        &mut self.buffer,
                                        4,
                                        filter,
                                    )?,
                                )))
                            }
                        },
                        PhysicalDataType::Int64 => match self.filter {
                            None => Some(Rc::from(DictionaryPageEnum::DictionaryPageInt64(
                                DictionaryPageInt64::new(&first_page_header, &mut self.buffer, 8)?,
                            ))),
                            Some(filter) => {
                                Some(Rc::from(DictionaryPageEnum::DictionaryPageWithFilterInt64(
                                    DictionaryPageWithFilterInt64::new(
                                        &first_page_header,
                                        &mut self.buffer,
                                        8,
                                        filter,
                                    )?,
                                )))
                            }
                        },
                        PhysicalDataType::Float32 => match self.filter {
                            None => Some(Rc::from(DictionaryPageEnum::DictionaryPageFloat32(
                                DictionaryPageFloat32::new(
                                    &first_page_header,
                                    &mut self.buffer,
                                    4,
                                )?,
                            ))),
                            Some(filter) => Some(Rc::from(
                                DictionaryPageEnum::DictionaryPageWithFilterFloat32(
                                    DictionaryPageWithFilterFloat32::new(
                                        &first_page_header,
                                        &mut self.buffer,
                                        4,
                                        filter,
                                    )?,
                                ),
                            )),
                        },
                        PhysicalDataType::Float64 => match self.filter {
                            None => Some(Rc::from(DictionaryPageEnum::DictionaryPageFloat64(
                                DictionaryPageFloat64::new(
                                    &first_page_header,
                                    &mut self.buffer,
                                    8,
                                )?,
                            ))),
                            Some(filter) => Some(Rc::from(
                                DictionaryPageEnum::DictionaryPageWithFilterFloat64(
                                    DictionaryPageWithFilterFloat64::new(
                                        &first_page_header,
                                        &mut self.buffer,
                                        8,
                                        filter,
                                    )?,
                                ),
                            )),
                        },

                        _ => None,
                    };
                } else {
                    self.buffer.set_rpos(rpos);
                }
                Some(())
            }
        };

        Ok(())
    }

    pub fn prepare_data_page(&mut self) -> Result<(), BoltReaderError> {
        let rpos = self.buffer.get_rpos();
        self.current_data_page_header = Some(read_page_header(&mut self.buffer)?);
        self.bytes_read += self.buffer.get_rpos() - rpos;

        Ok(())
    }

    pub fn load_data_page(&mut self) -> Result<(), BoltReaderError> {
        let page_header = self.current_data_page_header.as_ref().unwrap();
        let data_page_header = page_header.data_page_header.as_ref().unwrap();

        let rep_rle_bp = data_page_header.repetition_level_encoding == Encoding::RLE
            || data_page_header.repetition_level_encoding == Encoding::BIT_PACKED;

        let def_rle_bp = data_page_header.definition_level_encoding == Encoding::RLE
            || data_page_header.definition_level_encoding == Encoding::BIT_PACKED;

        // TODO: Add decompression support
        if page_header.crc.is_none() {
            // Uncompressed
            let rpos = self.buffer.get_rpos();
            let validity = RepDefParser::parse_rep_def(
                &mut self.buffer,
                data_page_header.num_values as usize,
                self.max_rep,
                rep_rle_bp,
                self.max_def,
                def_rle_bp,
            )?;

            let data_size =
                page_header.uncompressed_page_size - (self.buffer.get_rpos() - rpos) as i32;

            match &page_header.data_page_header {
                Some(data_page_v1) => data_page_v1,
                None => {
                    return Err(BoltReaderError::FixedLengthDataPageError(String::from(
                        "Error when reading Data Page V1 Header",
                    )))
                }
            };

            let is_dictionary_encoded = (page_header.data_page_header.as_ref().unwrap().encoding
                == Encoding::PLAIN_DICTIONARY)
                || (page_header.data_page_header.as_ref().unwrap().encoding
                    == Encoding::RLE_DICTIONARY);

            if self.dictionary_page.is_some() && is_dictionary_encoded {
                self.current_data_page = match self.physical_data_type {
                    PhysicalDataType::Boolean => None,

                    PhysicalDataType::Int32 => Some(DataPageEnum::RleBpDataPageReaderInt32V1(
                        RleBpDataPageReaderInt32V1::new(
                            page_header,
                            &mut self.buffer,
                            self.data_page_offset,
                            self.type_size,
                            validity.0,
                            data_size as usize,
                            self.filter,
                            validity.1,
                            Rc::clone(self.dictionary_page.as_ref().unwrap()),
                        )?,
                    )),
                    PhysicalDataType::Int64 => Some(DataPageEnum::RleBpDataPageReaderInt64V1(
                        RleBpDataPageReaderInt64V1::new(
                            page_header,
                            &mut self.buffer,
                            self.data_page_offset,
                            self.type_size,
                            validity.0,
                            data_size as usize,
                            self.filter,
                            validity.1,
                            Rc::clone(self.dictionary_page.as_ref().unwrap()),
                        )?,
                    )),
                    PhysicalDataType::Float32 => Some(DataPageEnum::RleBpDataPageReaderFloat32V1(
                        RleBpDataPageReaderFloat32V1::new(
                            page_header,
                            &mut self.buffer,
                            self.data_page_offset,
                            self.type_size,
                            validity.0,
                            data_size as usize,
                            self.filter,
                            validity.1,
                            Rc::clone(self.dictionary_page.as_ref().unwrap()),
                        )?,
                    )),
                    PhysicalDataType::Float64 => Some(DataPageEnum::RleBpDataPageReaderFloat64V1(
                        RleBpDataPageReaderFloat64V1::new(
                            page_header,
                            &mut self.buffer,
                            self.data_page_offset,
                            self.type_size,
                            validity.0,
                            data_size as usize,
                            self.filter,
                            validity.1,
                            Rc::clone(self.dictionary_page.as_ref().unwrap()),
                        )?,
                    )),
                    PhysicalDataType::None => None,
                };
            } else {
                self.current_data_page = match self.physical_data_type {
                    PhysicalDataType::Boolean => Some(DataPageEnum::BooleanDataPageReaderV1(
                        BooleanDataPageReaderV1::new(
                            page_header,
                            &mut self.buffer,
                            self.data_page_offset,
                            self.type_size,
                            validity.0,
                            data_size as usize,
                            self.filter,
                            validity.1,
                        )?,
                    )),

                    PhysicalDataType::Int32 => Some(DataPageEnum::PlainDataPageReaderInt32V1(
                        PlainDataPageReaderInt32V1::new(
                            page_header,
                            &mut self.buffer,
                            self.data_page_offset,
                            self.type_size,
                            validity.0,
                            data_size as usize,
                            self.filter,
                            validity.1,
                        )?,
                    )),
                    PhysicalDataType::Int64 => Some(DataPageEnum::PlainDataPageReaderInt64V1(
                        PlainDataPageReaderInt64V1::new(
                            page_header,
                            &mut self.buffer,
                            self.data_page_offset,
                            self.type_size,
                            validity.0,
                            data_size as usize,
                            self.filter,
                            validity.1,
                        )?,
                    )),
                    PhysicalDataType::Float32 => Some(DataPageEnum::PlainDataPageReaderFloat32V1(
                        PlainDataPageReaderFloat32V1::new(
                            page_header,
                            &mut self.buffer,
                            self.data_page_offset,
                            self.type_size,
                            validity.0,
                            data_size as usize,
                            self.filter,
                            validity.1,
                        )?,
                    )),
                    PhysicalDataType::Float64 => Some(DataPageEnum::PlainDataPageReaderFloat64V1(
                        PlainDataPageReaderFloat64V1::new(
                            page_header,
                            &mut self.buffer,
                            self.data_page_offset,
                            self.type_size,
                            validity.0,
                            data_size as usize,
                            self.filter,
                            validity.1,
                        )?,
                    )),
                    PhysicalDataType::None => None,
                };
            }

            // For uncompressed conditions, we move the index back for zero copy.
            self.buffer.set_rpos(rpos);
        } else {
            return Err(BoltReaderError::NotYetImplementedError(String::from(
                "Decompression not yet implemented",
            )));
        }

        Ok(())
    }

    pub fn get_next_data_page(&mut self) -> Result<bool, BoltReaderError> {
        if unlikely(self.current_data_page_header.is_none()) {
            return Err(BoltReaderError::PrimitiveColumnReaderError(String::from(
                "Current Data Page Header is not ready",
            )));
        }

        let old_page_header = self.current_data_page_header.as_ref().unwrap();
        let old_data_page_header = old_page_header.data_page_header.as_ref().unwrap();
        self.bytes_read += old_page_header.compressed_page_size as usize;
        if unlikely(self.bytes_read > self.column_size) {
            return Err(BoltReaderError::PrimitiveColumnReaderError(format!(
                "Column read bytes {} is larger than the column size {}.",
                self.bytes_read, self.column_size
            )));
        } else if self.bytes_read == self.column_size {
            return Ok(false);
        }

        self.buffer
            .set_rpos(self.buffer.get_rpos() + old_page_header.compressed_page_size as usize);

        self.data_page_offset += old_data_page_header.num_values as usize;

        self.current_data_page = Option::None;
        self.current_data_page_header = Option::None;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;

    use rand::{thread_rng, Rng};

    use crate::bridge::float64_bridge::Float64Bridge;
    use crate::bridge::int64_bridge::Int64Bridge;
    use crate::bridge::result_bridge::ResultBridge;
    use crate::column_reader::column_reader_base::ColumnReader;
    use crate::column_reader::primitive_column_reader::PrimitiveColumnReader;
    use crate::filters::fixed_length_filter::FixedLengthRangeFilter;
    use crate::filters::float_point_range_filter::FloatPointRangeFilter;
    use crate::filters::integer_range_filter::IntegerRangeFilter;
    use crate::metadata::parquet_footer::FileMetaDataLoader;
    use crate::metadata::parquet_metadata_thrift::FileMetaData;
    use crate::utils::byte_buffer_base::BufferEnum;
    use crate::utils::exceptions::BoltReaderError;
    use crate::utils::file_loader::LoadFile;
    use crate::utils::file_streaming_byte_buffer::{FileStreamingBuffer, StreamingByteBuffer};
    use crate::utils::local_file_loader::LocalFileLoader;
    use crate::utils::row_range_set::{RowRange, RowRangeSet};
    use crate::utils::test_utils::test_utils::{
        verify_plain_float64_non_null_result, verify_plain_float64_nullable_result,
        verify_plain_int64_non_null_result, verify_rle_bp_float64_nullable_result,
        verify_rle_bp_int64_non_null_result,
    };

    const STEAMING_BUFFER_SIZE: usize = 1 << 8;

    fn get_random_number_in_range(shift: u32) -> u32 {
        let mut rng = thread_rng();
        let random_value: u32 = rng.gen();
        random_value % (1 << shift)
    }

    fn load_file_metadata(path: &String) -> FileMetaData {
        let metadata_loader = FileMetaDataLoader::new(&String::from(path), 1 << 20);
        assert!(metadata_loader.is_ok());
        let mut metadata_loader = metadata_loader.unwrap();
        let res = metadata_loader.load_parquet_footer();
        assert!(res.is_ok());
        res.unwrap()
    }

    fn load_column_to_direct_buffer(path: &String) -> BufferEnum {
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();

        let res = file.load_file_to_buffer(4, file.get_file_size() - 4);
        assert!(res.is_ok());
        BufferEnum::DirectByteBuffer(res.unwrap())
    }

    fn load_column_to_streaming_buffer<'a>(
        file: &'a (dyn LoadFile + 'a),
        buffer_size: usize,
    ) -> BufferEnum {
        let res = StreamingByteBuffer::from_file(file, 4, file.get_file_size() - 4, buffer_size);
        assert!(res.is_ok());
        BufferEnum::StreamingByteBuffer(res.unwrap())
    }

    fn load_column_reader<'a>(
        path: &'a String,
        buffer: BufferEnum<'a>,
        filter: Option<&'a dyn FixedLengthRangeFilter>,
    ) -> Result<PrimitiveColumnReader<'a>, BoltReaderError> {
        let footer = load_file_metadata(&path);

        let column_meta_data = &footer.row_groups[0].columns[0].meta_data.as_ref().unwrap();
        let mut column_reader =
            PrimitiveColumnReader::new(0, 1, String::from("col1"), buffer, filter)?;
        column_reader.prepare_column_reader(column_meta_data)?;

        Ok(column_reader)
    }

    #[test]
    fn test_loading_dictionary_page() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");
        let buffer = load_column_to_direct_buffer(&path);
        let res = load_column_reader(&path, buffer, None);
        assert!(res.is_ok());
        let column_reader = res.unwrap();
        let dictionary = column_reader.dictionary_page;
        assert!(dictionary.is_some());
    }

    #[test]
    fn test_loading_dictionary_page_streaming_buffer() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");
        let res = LocalFileLoader::new(&path);
        assert!(res.is_ok());
        let file = res.unwrap();
        let buffer = load_column_to_streaming_buffer(&file, STEAMING_BUFFER_SIZE);
        let res = load_column_reader(&path, buffer, None);
        assert!(res.is_ok());
        let column_reader = res.unwrap();
        let dictionary = column_reader.dictionary_page;
        assert!(dictionary.is_some());
    }

    #[test]
    fn test_reading_plain_bigint_column_from_beginning() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");

        let num_values = 1000000;
        let num_tests = 100;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);
            let res = load_column_reader(&path, buffer, None);
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Int64Bridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());
                verify_plain_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_middle() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");

        let num_values = 1000000;
        let num_tests = 10;

        for num_test in 0..num_tests {
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + num_test;
                let buffer = load_column_to_direct_buffer(&path);
                let res = load_column_reader(&path, buffer, None);
                assert!(res.is_ok());
                let mut column_reader = res.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = Int64Bridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());
                    verify_plain_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_beginning_with_filter() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");

        let num_values = 1000000;
        let num_tests = 100;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);

            let filter_low = get_random_number_in_range(15) as i128;
            let filter_length = get_random_number_in_range(20) as i128;
            let filter = IntegerRangeFilter::new(filter_low, filter_low + filter_length, true);

            let res = load_column_reader(&path, buffer, Some(&filter));
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Int64Bridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());
                verify_plain_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_middle_filter() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");

        let num_values = 1000000;
        let num_tests = 10;

        for num_test in 0..num_tests {
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + num_test;

                let buffer = load_column_to_direct_buffer(&path);

                let filter_low = get_random_number_in_range(15) as i128;
                let filter_length = get_random_number_in_range(20) as i128;
                let filter = IntegerRangeFilter::new(filter_low, filter_low + filter_length, true);

                let res = load_column_reader(&path, buffer, Some(&filter));
                assert!(res.is_ok());
                let mut column_reader = res.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = Int64Bridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    verify_plain_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_plain_double_column_from_beginning() {
        let path = String::from("src/sample_files/plain_double_column.parquet");

        let num_values = 1000000;
        let num_tests = 100;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);
            let res = load_column_reader(&path, buffer, None);
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Float64Bridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());
                verify_plain_float64_non_null_result(&result_row_range_set, &raw_bridge, None);

                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_plain_double_column_from_middle() {
        let path = String::from("src/sample_files/plain_double_column.parquet");

        let num_values = 1000000;
        let num_tests = 10;

        for num_test in 0..num_tests {
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + num_test;
                let buffer = load_column_to_direct_buffer(&path);
                let res = load_column_reader(&path, buffer, None);
                assert!(res.is_ok());
                let mut column_reader = res.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = Float64Bridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());
                    verify_plain_float64_non_null_result(&result_row_range_set, &raw_bridge, None);

                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_plain_double_column_from_beginning_with_filter() {
        let path = String::from("src/sample_files/plain_double_column.parquet");

        let num_values = 1000000;
        let num_tests = 100;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);

            let filter_low = get_random_number_in_range(15) as i128;
            let filter_length = get_random_number_in_range(20) as i128;
            let filter = IntegerRangeFilter::new(filter_low, filter_low + filter_length, true);

            let res = load_column_reader(&path, buffer, Some(&filter));
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Float64Bridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());
                verify_plain_float64_non_null_result(&result_row_range_set, &raw_bridge, None);

                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_plain_double_column_from_middle_filter() {
        let path = String::from("src/sample_files/plain_double_column.parquet");

        let num_values = 1000000;
        let num_tests = 10;

        for num_test in 0..num_tests {
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + num_test;

                let buffer = load_column_to_direct_buffer(&path);

                let filter_low = get_random_number_in_range(15) as i128;
                let filter_length = get_random_number_in_range(20) as i128;
                let filter = IntegerRangeFilter::new(filter_low, filter_low + filter_length, true);

                let res = load_column_reader(&path, buffer, Some(&filter));
                assert!(res.is_ok());
                let mut column_reader = res.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = Float64Bridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    verify_plain_float64_non_null_result(&result_row_range_set, &raw_bridge, None);

                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_beginning_streaming_buffer() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let num_values = 1000000;

        for shift in 12..16 {
            let buffer_size = 1 << shift;
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let res = LocalFileLoader::new(&path);
            assert!(res.is_ok());
            let file = res.unwrap();
            let buffer = load_column_to_streaming_buffer(&file, buffer_size);
            let res = load_column_reader(&path, buffer, None);
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Int64Bridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());

                verify_plain_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_middle_streaming_buffer() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let num_values = 1000000;

        for shift in 12..16 {
            let buffer_size = 1 << shift;
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + shift;
                let res = LocalFileLoader::new(&path);
                assert!(res.is_ok());
                let file = res.unwrap();
                let buffer = load_column_to_streaming_buffer(&file, buffer_size);
                let res = load_column_reader(&path, buffer, None);
                assert!(res.is_ok());
                let mut column_reader = res.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = Int64Bridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    verify_plain_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_beginning_with_filter_streaming_buffer() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let num_values = 1000000;

        for shift in 12..16 {
            let buffer_size = 1 << shift;
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let res = LocalFileLoader::new(&path);
            assert!(res.is_ok());
            let file = res.unwrap();
            let buffer = load_column_to_streaming_buffer(&file, buffer_size);

            let filter_low = get_random_number_in_range(15) as i128;
            let filter_length = get_random_number_in_range(20) as i128;
            let filter = IntegerRangeFilter::new(filter_low, filter_low + filter_length, true);

            let res = load_column_reader(&path, buffer, Some(&filter));
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Int64Bridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());

                verify_plain_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_plain_bigint_column_from_middle_filter_streaming_buffer() {
        let path = String::from("src/sample_files/plain_bigint_column.parquet");
        let num_values = 1000000;

        for shift in 12..16 {
            let buffer_size = 1 << shift;
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + shift;
                let res = LocalFileLoader::new(&path);
                assert!(res.is_ok());
                let file = res.unwrap();
                let buffer = load_column_to_streaming_buffer(&file, buffer_size);

                let filter_low = get_random_number_in_range(15) as i128;
                let filter_length = get_random_number_in_range(20) as i128;
                let filter = IntegerRangeFilter::new(filter_low, filter_low + filter_length, true);

                let res = load_column_reader(&path, buffer, Some(&filter));
                assert!(res.is_ok());
                let mut column_reader = res.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = Int64Bridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    verify_plain_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_nullalbe_plain_double_column_from_beginning() {
        let path = String::from("src/sample_files/plain_double_column_with_nulls.parquet");

        let num_values = 1000000;
        let num_tests = 10;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);
            let res = load_column_reader(&path, buffer, None);
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Float64Bridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    verify_plain_float64_nullable_result(&result_row_range_set, &raw_bridge, None);
                }
                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_nullalbe_plain_double_column_from_middle() {
        let path = String::from("src/sample_files/plain_double_column_with_nulls.parquet");

        let num_values = 1000000;
        let num_tests = 5;

        for num_test in 0..num_tests {
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + num_test;
                let buffer = load_column_to_direct_buffer(&path);
                let res = load_column_reader(&path, buffer, None);
                assert!(res.is_ok());
                let mut column_reader = res.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = Float64Bridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    if !raw_bridge.is_empty() {
                        verify_plain_float64_nullable_result(
                            &result_row_range_set,
                            &raw_bridge,
                            None,
                        );
                    }

                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_non_null_plain_double_column_from_beginning_with_filter() {
        let path = String::from("src/sample_files/plain_double_column_with_nulls.parquet");

        let num_values = 1000000;
        let num_tests = 3;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);

            let filter_low = get_random_number_in_range(15) as i64;
            let filter_length = get_random_number_in_range(20) as i64;
            let filter = FloatPointRangeFilter::new(
                filter_low as f64,
                (filter_low + filter_length) as f64,
                true,
                true,
                false,
                false,
                false,
            );
            let res = load_column_reader(&path, buffer, Some(&filter));
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Float64Bridge::new(false, step as usize);

                let res = column_reader.read_with_filter(
                    to_read,
                    offset,
                    &mut result_row_range_set,
                    &mut raw_bridge,
                );
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    verify_plain_float64_nullable_result(&result_row_range_set, &raw_bridge, None);
                }
                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_nullable_plain_double_column_from_beginning_with_filter() {
        let path = String::from("src/sample_files/plain_double_column_with_nulls.parquet");

        let num_values = 1000000;
        let num_tests = 3;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);

            let filter_low = get_random_number_in_range(15) as i64;
            let filter_length = get_random_number_in_range(20) as i64;
            let filter = FloatPointRangeFilter::new(
                filter_low as f64,
                (filter_low + filter_length) as f64,
                true,
                true,
                false,
                false,
                true,
            );
            let res = load_column_reader(&path, buffer, Some(&filter));
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Float64Bridge::new(false, step as usize);

                let res = column_reader.read_with_filter(
                    to_read,
                    offset,
                    &mut result_row_range_set,
                    &mut raw_bridge,
                );
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    verify_plain_float64_nullable_result(&result_row_range_set, &raw_bridge, None);
                }
                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_non_null_plain_double_column_from_middle_filter() {
        let path = String::from("src/sample_files/plain_double_column_with_nulls.parquet");

        let num_values = 1000000;

        let mut begin_base = 1;
        while begin_base < num_values {
            let mut begin = begin_base;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);
            let filter_low = get_random_number_in_range(15) as i64;
            let filter_length = get_random_number_in_range(20) as i64;
            let filter = FloatPointRangeFilter::new(
                filter_low as f64,
                (filter_low + filter_length) as f64,
                true,
                true,
                false,
                false,
                false,
            );
            let res = load_column_reader(&path, buffer, Some(&filter));
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Float64Bridge::new(false, step as usize);

                let res = column_reader.read_with_filter(
                    to_read,
                    offset,
                    &mut result_row_range_set,
                    &mut raw_bridge,
                );
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    verify_plain_float64_nullable_result(&result_row_range_set, &raw_bridge, None);
                }
                begin = end;
            }
            begin_base = begin_base << 4;
        }
    }

    #[test]
    fn test_reading_nullable_plain_double_column_from_middle_filter() {
        let path = String::from("src/sample_files/plain_double_column_with_nulls.parquet");

        let num_values = 1000000;

        let mut begin_base = 1;
        while begin_base < num_values {
            let mut begin = begin_base;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);
            let filter_low = get_random_number_in_range(15) as i64;
            let filter_length = get_random_number_in_range(20) as i64;
            let filter = FloatPointRangeFilter::new(
                filter_low as f64,
                (filter_low + filter_length) as f64,
                true,
                true,
                false,
                false,
                true,
            );
            let res = load_column_reader(&path, buffer, Some(&filter));
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Float64Bridge::new(false, step as usize);

                let res = column_reader.read_with_filter(
                    to_read,
                    offset,
                    &mut result_row_range_set,
                    &mut raw_bridge,
                );
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    verify_plain_float64_nullable_result(&result_row_range_set, &raw_bridge, None);
                }
                begin = end;
            }
            begin_base = begin_base << 4;
        }
    }

    #[test]
    fn test_reading_rle_bp_bigint_column_from_beginning() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");

        let num_values = 1000000;
        let num_tests = 100;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);
            let res = load_column_reader(&path, buffer, None);
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Int64Bridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());
                verify_rle_bp_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_rle_bp_bigint_column_from_middle() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");

        let num_values = 1000000;
        let num_tests = 10;

        for num_test in 0..num_tests {
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + num_test;
                let buffer = load_column_to_direct_buffer(&path);
                let res = load_column_reader(&path, buffer, None);
                assert!(res.is_ok());
                let mut column_reader = res.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = Int64Bridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());
                    verify_rle_bp_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_rle_bp_bigint_column_from_beginning_with_filter() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");

        let num_values = 1000000;
        let num_tests = 100;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);

            let filter_low = get_random_number_in_range(15) as i128;
            let filter_length = get_random_number_in_range(20) as i128;
            let filter = IntegerRangeFilter::new(filter_low, filter_low + filter_length, true);

            let res = load_column_reader(&path, buffer, Some(&filter));
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Int64Bridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());
                verify_rle_bp_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_rle_bp_bigint_column_from_middle_filter() {
        let path = String::from("src/sample_files/rle_bp_bigint_column.parquet");

        let num_values = 1000000;
        let num_tests = 10;

        for num_test in 0..num_tests {
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + num_test;

                let buffer = load_column_to_direct_buffer(&path);

                let filter_low = get_random_number_in_range(15) as i128;
                let filter_length = get_random_number_in_range(20) as i128;
                let filter = IntegerRangeFilter::new(filter_low, filter_low + filter_length, true);

                let res = load_column_reader(&path, buffer, Some(&filter));
                assert!(res.is_ok());
                let mut column_reader = res.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = Int64Bridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    verify_rle_bp_int64_non_null_result(&result_row_range_set, &raw_bridge, None);

                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_nullalbe_rle_bp_double_column_from_beginning() {
        let path = String::from("src/sample_files/rle_bp_double_column_with_nulls.parquet");

        let num_values = 1000000;
        let num_tests = 10;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);
            let res = load_column_reader(&path, buffer, None);
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Float64Bridge::new(false, step as usize);

                let res =
                    column_reader.read(to_read, offset, &mut result_row_range_set, &mut raw_bridge);
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    verify_rle_bp_float64_nullable_result(&result_row_range_set, &raw_bridge, None);
                }
                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_nullalbe_rle_bp_double_column_from_middle() {
        let path = String::from("src/sample_files/rle_bp_double_column_with_nulls.parquet");

        let num_values = 1000000;
        let num_tests = 5;

        for num_test in 0..num_tests {
            let mut begin_base = 1;
            while begin_base < num_values {
                let mut begin = begin_base;
                let step = get_random_number_in_range(15) + num_test;
                let buffer = load_column_to_direct_buffer(&path);
                let res = load_column_reader(&path, buffer, None);
                assert!(res.is_ok());
                let mut column_reader = res.unwrap();

                while begin < num_values {
                    let end = min(begin + step, num_values);
                    let to_read = RowRange::new(begin as usize, end as usize);
                    let offset = 0;
                    let mut result_row_range_set = RowRangeSet::new(offset);
                    let mut raw_bridge = Float64Bridge::new(false, step as usize);

                    let res = column_reader.read(
                        to_read,
                        offset,
                        &mut result_row_range_set,
                        &mut raw_bridge,
                    );
                    assert!(res.is_ok());

                    if !raw_bridge.is_empty() {
                        verify_rle_bp_float64_nullable_result(
                            &result_row_range_set,
                            &raw_bridge,
                            None,
                        );
                    }

                    begin = end;
                }
                begin_base = begin_base << 1;
            }
        }
    }

    #[test]
    fn test_reading_non_null_rle_bp_double_column_from_beginning_with_filter() {
        let path = String::from("src/sample_files/rle_bp_double_column_with_nulls.parquet");

        let num_values = 1000000;
        let num_tests = 3;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);

            let filter_low = get_random_number_in_range(15) as i64;
            let filter_length = get_random_number_in_range(20) as i64;
            let filter = FloatPointRangeFilter::new(
                filter_low as f64,
                (filter_low + filter_length) as f64,
                true,
                true,
                false,
                false,
                false,
            );
            let res = load_column_reader(&path, buffer, Some(&filter));
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Float64Bridge::new(false, step as usize);

                let res = column_reader.read_with_filter(
                    to_read,
                    offset,
                    &mut result_row_range_set,
                    &mut raw_bridge,
                );
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    verify_rle_bp_float64_nullable_result(&result_row_range_set, &raw_bridge, None);
                }
                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_nullable_rle_bp_double_column_from_beginning_with_filter() {
        let path = String::from("src/sample_files/rle_bp_double_column_with_nulls.parquet");

        let num_values = 1000000;
        let num_tests = 3;

        for _num_test in 0..num_tests {
            let mut begin = 0;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);

            let filter_low = get_random_number_in_range(15) as i64;
            let filter_length = get_random_number_in_range(20) as i64;
            let filter = FloatPointRangeFilter::new(
                filter_low as f64,
                (filter_low + filter_length) as f64,
                true,
                true,
                false,
                false,
                true,
            );
            let res = load_column_reader(&path, buffer, Some(&filter));
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Float64Bridge::new(false, step as usize);

                let res = column_reader.read_with_filter(
                    to_read,
                    offset,
                    &mut result_row_range_set,
                    &mut raw_bridge,
                );
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    verify_rle_bp_float64_nullable_result(&result_row_range_set, &raw_bridge, None);
                }
                begin = end;
            }
        }
    }

    #[test]
    fn test_reading_non_null_rle_bp_double_column_from_middle_filter() {
        let path = String::from("src/sample_files/rle_bp_double_column_with_nulls.parquet");

        let num_values = 1000000;

        let mut begin_base = 1;
        while begin_base < num_values {
            let mut begin = begin_base;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);
            let filter_low = get_random_number_in_range(15) as i64;
            let filter_length = get_random_number_in_range(20) as i64;
            let filter = FloatPointRangeFilter::new(
                filter_low as f64,
                (filter_low + filter_length) as f64,
                true,
                true,
                false,
                false,
                false,
            );
            let res = load_column_reader(&path, buffer, Some(&filter));
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Float64Bridge::new(false, step as usize);

                let res = column_reader.read_with_filter(
                    to_read,
                    offset,
                    &mut result_row_range_set,
                    &mut raw_bridge,
                );
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    verify_rle_bp_float64_nullable_result(&result_row_range_set, &raw_bridge, None);
                }
                begin = end;
            }
            begin_base = begin_base << 4;
        }
    }

    #[test]
    fn test_reading_nullable_rle_bp_double_column_from_middle_filter() {
        let path = String::from("src/sample_files/rle_bp_double_column_with_nulls.parquet");

        let num_values = 1000000;

        let mut begin_base = 1;
        while begin_base < num_values {
            let mut begin = begin_base;
            let step = get_random_number_in_range(15);
            let buffer = load_column_to_direct_buffer(&path);
            let filter_low = get_random_number_in_range(15) as i64;
            let filter_length = get_random_number_in_range(20) as i64;
            let filter = FloatPointRangeFilter::new(
                filter_low as f64,
                (filter_low + filter_length) as f64,
                true,
                true,
                false,
                false,
                true,
            );
            let res = load_column_reader(&path, buffer, Some(&filter));
            assert!(res.is_ok());
            let mut column_reader = res.unwrap();

            while begin < num_values {
                let end = min(begin + step, num_values);
                let to_read = RowRange::new(begin as usize, end as usize);
                let offset = 0;
                let mut result_row_range_set = RowRangeSet::new(offset);
                let mut raw_bridge = Float64Bridge::new(false, step as usize);

                let res = column_reader.read_with_filter(
                    to_read,
                    offset,
                    &mut result_row_range_set,
                    &mut raw_bridge,
                );
                assert!(res.is_ok());

                if !raw_bridge.is_empty() {
                    verify_rle_bp_float64_nullable_result(&result_row_range_set, &raw_bridge, None);
                }
                begin = end;
            }
            begin_base = begin_base << 4;
        }
    }
}
