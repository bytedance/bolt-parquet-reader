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

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::{io, result};

/// Result type that could result in an [BoltReaderError]
pub type Result<T, E = BoltReaderError> = result::Result<T, E>;

#[derive(Debug)]
pub enum BoltReaderError {
    IoError(io::Error),
    InsufficientMemoryError(String),
    NotYetImplementedError(String),
    InternalError(String),
    FileFormatError(String),
    MetadataError(String),
    VarintDecodingError(String),
    RleBpDecodingError(String),
    FixedLengthDictionaryPageError(String),
    FixedLengthDataPageError(String),
    BooleanDataPageError(String),
    BridgeError(String),
    RepDefError(String),
    FixedLengthColumnReaderError(String),
}

impl Error for BoltReaderError {}

impl Display for BoltReaderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BoltReaderError::IoError(e) => {
                writeln!(f, "I/O Error: {e}")
            }
            BoltReaderError::InsufficientMemoryError(e) => {
                writeln!(f, "Insufficient Memory: {e}")
            }
            BoltReaderError::NotYetImplementedError(e) => {
                writeln!(f, "Not Yet Implemented: {e}")
            }
            BoltReaderError::InternalError(e) => {
                writeln!(f, "Internal Error: {e}")
            }
            BoltReaderError::FileFormatError(e) => {
                writeln!(f, "File Format Error: {e}")
            }
            BoltReaderError::MetadataError(e) => {
                writeln!(f, "Parquet Metadata Error: {e}")
            }
            BoltReaderError::VarintDecodingError(e) => {
                writeln!(f, "Varint Decoding Error: {e}")
            }
            BoltReaderError::RleBpDecodingError(e) => {
                writeln!(f, "Rle Bp Decoding Error: {e}")
            }
            BoltReaderError::FixedLengthDictionaryPageError(e) => {
                writeln!(f, "Dictionary Page Loading Error: {e}")
            }
            BoltReaderError::FixedLengthDataPageError(e) => {
                writeln!(f, "Fixed Length Data Page Error: {e}")
            }
            BoltReaderError::BooleanDataPageError(e) => {
                writeln!(f, "Boolean Data Page Error: {e}")
            }
            BoltReaderError::BridgeError(e) => {
                writeln!(f, "Bridge Error: {e}")
            }
            BoltReaderError::RepDefError(e) => {
                writeln!(f, "Repetition and Definition Error: {e}")
            }
            BoltReaderError::FixedLengthColumnReaderError(e) => {
                writeln!(f, "Fixed Length Column Reader Error: {e}")
            }
        }
    }
}

impl From<io::Error> for BoltReaderError {
    fn from(e: io::Error) -> Self {
        BoltReaderError::IoError(e)
    }
}

impl From<parquet2::error::Error> for BoltReaderError {
    fn from(e: parquet2::error::Error) -> Self {
        BoltReaderError::RleBpDecodingError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::utils::exceptions::BoltReaderError;

    fn open_file_internal(file: String) -> Result<String, BoltReaderError> {
        File::open(file)?;
        Ok(String::from("foo"))
    }

    #[test]
    fn test_io_error() {
        let file = String::from("not_exist.txt");
        let res = open_file_internal(file);
        match res {
            Ok(_) => {}
            Err(e) => assert_eq!(
                e.to_string(),
                "I/O Error: No such file or directory (os error 2)\n"
            ),
        }
    }

    #[test]
    fn test_insufficient_memory_error() {
        let error: BoltReaderError = BoltReaderError::InsufficientMemoryError(String::from("foo"));
        assert_eq!(error.to_string(), "Insufficient Memory: foo\n");
    }

    #[test]
    fn test_not_yet_implemented_error() {
        let error: BoltReaderError = BoltReaderError::NotYetImplementedError(String::from("foo"));
        assert_eq!(error.to_string(), "Not Yet Implemented: foo\n");
    }

    #[test]
    fn test_internal_error() {
        let error: BoltReaderError = BoltReaderError::InternalError(String::from("foo"));
        assert_eq!(error.to_string(), "Internal Error: foo\n");
    }

    #[test]
    fn test_file_format_error() {
        let error: BoltReaderError = BoltReaderError::FileFormatError(String::from("foo"));
        assert_eq!(error.to_string(), "File Format Error: foo\n");
    }

    #[test]
    fn test_metadata_error() {
        let error: BoltReaderError = BoltReaderError::MetadataError(String::from("foo"));
        assert_eq!(error.to_string(), "Parquet Metadata Error: foo\n");
    }
}
