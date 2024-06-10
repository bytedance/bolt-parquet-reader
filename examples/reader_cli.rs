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
use std::time::Instant;

use argparse::{ArgumentParser, Store};
use bolt_parquet_reader::file_reader::local_file_reader::LocalFileReader;
use bolt_parquet_reader::filters::fixed_length_filter::FixedLengthRangeFilter;

fn parse_string_to_list(input: String, delimiter: String) -> Vec<String> {
    input
        .split(&delimiter)
        .map(|s| s.to_string())
        .filter(|s| s.len() > 0)
        .collect()
}

fn main() {
    let mut files_str: String = String::new();
    let mut columns_str: String = String::new();
    let mut batch_size: usize = 0;

    {
        let mut ap = ArgumentParser::new();
        ap.set_description(
            "Bolt Parquet Reader. Please input the files, columns and reading step length",
        );
        ap.refer(&mut files_str).add_option(
            &["-f", "--files"],
            Store,
            "Files to be read. Format \"file1;file2;\"",
        );
        ap.refer(&mut columns_str).add_option(
            &["-c", "--columns"],
            Store,
            "Columns to be read. Format \"col1;col2;\"",
        );
        ap.refer(&mut batch_size)
            .add_option(&["-s", "--step"], Store, "step length for reading");

        ap.parse_args_or_exit();
    }

    let files = parse_string_to_list(files_str, String::from(";"));
    let columns = parse_string_to_list(columns_str, String::from(";"));

    let mut columns_to_read: HashMap<String, Option<&dyn FixedLengthRangeFilter>> = HashMap::new();

    // We can add a filter for each column here by replace the None.
    for column in columns {
        columns_to_read.insert(column, None);
    }

    let total_time = Instant::now();

    for file in &files {
        let mut file_reader =
            LocalFileReader::from_local_file(&file.to_string(), columns_to_read.clone())
                .expect("unable to read file");

        let start = Instant::now();
        let mut finished = false;
        while !finished {
            let res = file_reader.read(batch_size).expect("Reading error");
            finished = res.1;
        }
        println!(
            "Finished Reading File: {}, Time: {} ms",
            &file,
            start.elapsed().as_millis()
        );
    }

    println!(
        "Finished Reading All the {} Files, Time: {} ms",
        files.len(),
        total_time.elapsed().as_millis()
    );
}
