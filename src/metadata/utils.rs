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
use std::intrinsics::unlikely;

use crate::metadata::parquet_metadata_thrift::{SchemaElement, Type};
use crate::utils::exceptions::BoltReaderError;

// Current, the ColumnType only supports primitive type. More nested type related strut members will be added in the
// future.
#[allow(dead_code)]
pub struct ColumnSchema {
    is_leaf: bool,
    name: String,
    type_: Option<Type>,
    children: Option<Vec<ColumnSchema>>,
    column_idx: usize,
    max_def: u32,
    max_rep: u32,
}

impl ColumnSchema {
    pub fn new(
        is_leaf: bool,
        name: String,
        type_: Option<Type>,
        column_idx: usize,
    ) -> Result<ColumnSchema, BoltReaderError> {
        if !is_leaf {
            return Err(BoltReaderError::NotYetImplementedError(String::from(
                "Not Yet Implemented: Nested Type",
            )));
        }

        Ok(ColumnSchema {
            is_leaf,
            name,
            type_,
            children: None,
            column_idx,
            max_def: 1,
            max_rep: 0,
        })
    }
}

// Currently, this API is only able to parse primitive Parquet Schema. The Parquet schema is stored
// in recursive structure to represent the nested types. But for primitive types, all the schemas
// are at leaf level.
// TODO: Support complex types.
pub fn prepare_schema(
    schema_elements: &[SchemaElement],
) -> Result<HashMap<String, ColumnSchema>, BoltReaderError> {
    if unlikely(schema_elements.is_empty()) {
        return Err(BoltReaderError::MetadataError(String::from(
            "File schema is empty.",
        )));
    }

    let root = &schema_elements[0];
    if unlikely(root.num_children.is_none()) {
        return Err(BoltReaderError::MetadataError(String::from(
            "File schema only contains root.",
        )));
    }

    prepare_schema_internal(schema_elements, 0)
}

// Implemented without recursion for primitive types only. Need to be updated when supporting nested
// types.
pub fn prepare_schema_internal(
    schema_elements: &[SchemaElement],
    schema_index: usize,
) -> Result<HashMap<String, ColumnSchema>, BoltReaderError> {
    let mut columns = HashMap::new();
    let schema_node = &schema_elements[schema_index];
    match schema_node.num_children {
        None => {
            return Err(BoltReaderError::MetadataError(String::from(
                "Parent Schema Node Does Not Contain Children.",
            )));
        }
        Some(num_children) => {
            let mut leaf_index = 0;
            for child in schema_elements
                .iter()
                .take(num_children as usize + 1)
                .skip(schema_index + 1)
            {
                // TODO: Use recursion here
                match child.num_children {
                    Some(_) => {
                        return Err(BoltReaderError::NotYetImplementedError(String::from(
                            "Not Yet Implemented: Nested Type",
                        )))
                    }

                    None => match child.type_ {
                        None => {
                            return Err(BoltReaderError::MetadataError(String::from(
                                "Primitive Type Column Does Not Contain Type Info",
                            )))
                        }
                        Some(type_) => {
                            columns.insert(
                                child.name.clone(),
                                ColumnSchema::new(
                                    true,
                                    child.name.clone(),
                                    Some(type_),
                                    leaf_index,
                                )?,
                            );
                            leaf_index += 1;
                        }
                    },
                }
            }
        }
    }

    Ok(columns)
}

#[cfg(test)]
mod tests {
    use crate::metadata::parquet_footer::FileMetaDataLoader;
    use crate::metadata::parquet_metadata_thrift::{FileMetaData, Type};
    use crate::metadata::utils::prepare_schema;

    const DEFAULT_PARQUET_FOOTER_PRELOAD_SIZE: usize = 1 << 20;

    fn load_parquet_footer(file: String) -> FileMetaData {
        let metadata_loader =
            FileMetaDataLoader::new(&String::from(&file), DEFAULT_PARQUET_FOOTER_PRELOAD_SIZE);
        assert!(metadata_loader.is_ok());
        let mut metadata_loader = metadata_loader.unwrap();
        let res = metadata_loader.load_parquet_footer();
        assert!(res.is_ok());
        res.unwrap()
    }

    #[test]
    fn test_load_column_schema() {
        let file = String::from("src/sample_files/plain_bigint_column.parquet");
        let footer = load_parquet_footer(file);

        let res = prepare_schema(&footer.schema);
        assert!(res.is_ok());
        let columns = res.unwrap();
        assert_eq!(columns.len(), 1);
        let res = columns.get("col1");
        assert!(res.is_some());
        let column = res.unwrap();
        assert_eq!(column.is_leaf, true);
        assert_eq!(column.name, "col1");
        assert!(column.type_.is_some());
        assert_eq!(column.type_.unwrap(), Type::INT64);
        assert!(column.children.is_none());
    }

    #[test]
    fn test_load_column_schema_lineitem() {
        let file = String::from("src/sample_files/lineitem.parquet");
        let footer = load_parquet_footer(file);

        let res = prepare_schema(&footer.schema);
        assert!(res.is_ok());
        let columns = res.unwrap();
        assert_eq!(columns.len(), 22);
        let columns_metadata = &footer.row_groups[0].columns;

        for i in 0..columns_metadata.len() {
            let column_metadata = columns_metadata[i].meta_data.as_ref().unwrap();
            let name = column_metadata.path_in_schema[0].clone();

            let res = columns.get(&name);
            assert!(res.is_some());
            let column = res.unwrap();
            assert_eq!(column.name, name);
        }
    }
}
