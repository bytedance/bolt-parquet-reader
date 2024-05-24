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

use crate::metadata::parquet_metadata_thrift::{
    ConvertedType, FieldRepetitionType, SchemaElement, Type,
};
use crate::utils::exceptions::BoltReaderError;

pub enum SchemaNodeType {
    Leaf,
    List,
    ListElement,
    Map,
    MapElement,
    Struct,
}

// Current, the ColumnType only supports primitive type. More nested type related strut members will be added in the
// future.
pub struct ColumnSchema {
    pub is_leaf: bool,
    pub name: String,
    schema_node_type: SchemaNodeType,
    pub physical_type: Option<Type>,
    pub children: Option<Vec<ColumnSchema>>,
    pub column_idx: usize,
    pub max_def: u32,
    pub max_rep: u32,
}

impl ColumnSchema {
    pub fn new(
        is_leaf: bool,
        name: String,
        schema_node_type: SchemaNodeType,
        physical_type: Option<Type>,
        column_idx: usize,
        max_def: u32,
        max_rep: u32,
    ) -> Result<ColumnSchema, BoltReaderError> {
        Ok(ColumnSchema {
            is_leaf,
            name,
            schema_node_type,
            physical_type,
            children: None,
            column_idx,
            max_def,
            max_rep,
        })
    }
}

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

    prepare_schema_internal(schema_elements)
}

pub fn create_column_schema_node(
    cur_schema: &SchemaElement,
    max_def: u32,
    max_rep: u32,
    column_idx: usize,
) -> Result<ColumnSchema, BoltReaderError> {
    match cur_schema.type_ {
        // Non-leaf Node
        None => {
            match cur_schema.converted_type {
                // There are 3 possibilities:
                // 1. ListElement
                // 2. MapElement
                // 3. Struct
                None => match cur_schema.repetition_type {
                    None => Err(BoltReaderError::MetadataError(String::from(
                        "Schema Repetition Type cannot be empty for nested types",
                    ))),
                    Some(repetition_type) => {
                        if repetition_type == FieldRepetitionType::REPEATED {
                            match cur_schema.num_children {
                                None => Err(BoltReaderError::MetadataError(String::from(
                                    "Nested Types must have childrens",
                                ))),
                                Some(num_children) => {
                                    if num_children == 1 {
                                        ColumnSchema::new(
                                            false,
                                            cur_schema.name.clone(),
                                            SchemaNodeType::ListElement,
                                            None,
                                            column_idx,
                                            max_def,
                                            max_rep,
                                        )
                                    } else if num_children == 2 {
                                        ColumnSchema::new(
                                            false,
                                            cur_schema.name.clone(),
                                            SchemaNodeType::MapElement,
                                            None,
                                            column_idx,
                                            max_def,
                                            max_rep,
                                        )
                                    } else {
                                        Err(BoltReaderError::MetadataError(String::from(
                                            "List/Map must have at most 2 children",
                                        )))
                                    }
                                }
                            }
                        } else {
                            ColumnSchema::new(
                                false,
                                cur_schema.name.clone(),
                                SchemaNodeType::Struct,
                                None,
                                column_idx,
                                max_def,
                                max_rep,
                            )
                        }
                    }
                },
                // Converted Types
                // TODO: Currently, we only have List and Map. Add all the converted types in the future.
                Some(converted_type) => match converted_type {
                    ConvertedType::LIST => ColumnSchema::new(
                        false,
                        cur_schema.name.clone(),
                        SchemaNodeType::List,
                        None,
                        column_idx,
                        max_def,
                        max_rep,
                    ),
                    ConvertedType::MAP => ColumnSchema::new(
                        false,
                        cur_schema.name.clone(),
                        SchemaNodeType::Map,
                        None,
                        column_idx,
                        max_def,
                        max_rep,
                    ),
                    ConvertedType::MAP_KEY_VALUE => ColumnSchema::new(
                        false,
                        cur_schema.name.clone(),
                        SchemaNodeType::MapElement,
                        None,
                        column_idx,
                        max_def,
                        max_rep,
                    ),
                    _ => Err(BoltReaderError::MetadataError(format!(
                        "This converted type {} is not yes implemented",
                        converted_type.0
                    ))),
                },
            }
        }

        // Leaf Node
        Some(physical_type) => ColumnSchema::new(
            true,
            cur_schema.name.clone(),
            SchemaNodeType::Leaf,
            Some(physical_type),
            column_idx,
            max_def,
            max_rep,
        ),
    }
}

pub fn prepare_schema_internal_recursive(
    schema_elements: &[SchemaElement],
    schema_idx: &mut usize,
    mut max_def: u32,
    mut max_rep: u32,
    column_idx: &mut usize,
) -> Result<ColumnSchema, BoltReaderError> {
    let cur_schema = &schema_elements[*schema_idx];

    match cur_schema.repetition_type {
        None => {}
        Some(repetition_type) => {
            if repetition_type != FieldRepetitionType::REQUIRED {
                max_def += 1;
            }
            if repetition_type == FieldRepetitionType::REPEATED {
                max_rep += 1;
            }
        }
    }

    let mut column_schema = create_column_schema_node(cur_schema, max_def, max_rep, *column_idx)?;
    let schema_node_type = &column_schema.schema_node_type;
    *schema_idx += 1;
    match schema_node_type {
        SchemaNodeType::Leaf => {
            *column_idx += 1;
        }

        SchemaNodeType::List | SchemaNodeType::ListElement | SchemaNodeType::Map => {
            let num_children = cur_schema.num_children.unwrap();
            if num_children != 1 {
                return Err(BoltReaderError::MetadataError(String::from(
                    "List/Map/ListElement Schema Node should have 1 child",
                )));
            }
            let children: Vec<ColumnSchema> = vec![prepare_schema_internal_recursive(
                schema_elements,
                schema_idx,
                max_def,
                max_rep,
                column_idx,
            )?];
            column_schema.children = Some(children);
        }

        SchemaNodeType::MapElement => {
            let num_children = cur_schema.num_children.unwrap();
            if num_children != 2 {
                return Err(BoltReaderError::MetadataError(String::from(
                    "MapElement Schema Node should have 1 child",
                )));
            }
            let mut children: Vec<ColumnSchema> = Default::default();
            for _ in 0..num_children {
                children.push(prepare_schema_internal_recursive(
                    schema_elements,
                    schema_idx,
                    max_def,
                    max_rep,
                    column_idx,
                )?);
            }
            column_schema.children = Some(children);
        }

        SchemaNodeType::Struct => {
            let num_children = cur_schema.num_children.unwrap();

            let mut children: Vec<ColumnSchema> = Default::default();
            for _ in 0..num_children {
                children.push(prepare_schema_internal_recursive(
                    schema_elements,
                    schema_idx,
                    max_def,
                    max_rep,
                    column_idx,
                )?);
            }
            column_schema.children = Some(children);
        }
    };

    Ok(column_schema)
}

pub fn prepare_schema_internal(
    schema_elements: &[SchemaElement],
) -> Result<HashMap<String, ColumnSchema>, BoltReaderError> {
    let mut columns = HashMap::new();

    let mut schema_idx = 0;
    let mut column_idx = 0;
    let root =
        prepare_schema_internal_recursive(schema_elements, &mut schema_idx, 0, 0, &mut column_idx)?;

    match root.children {
        None => {
            return Err(BoltReaderError::MetadataError(String::from(
                "The root schema node has no child",
            )))
        }
        Some(children) => {
            for child in children {
                columns.insert(child.name.clone(), child);
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
        assert!(column.physical_type.is_some());
        assert_eq!(column.physical_type.unwrap(), Type::INT64);
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
