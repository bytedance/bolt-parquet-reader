<!---
  Copyright (c) ByteDance, Inc. and its affiliates.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# ByteDance Bolt Parquet Reader
ByteDance's next generation universal high-performance Parquet Reader.

# Design Goal
The Bolt Parquet Reader is a native Parquet Reader in Rust language.

This design supports steaming reading, which allows to read the whole batch in smaller batches and reduce the peak memory cost. And, as a result, it is able to increase the overall parallelism.

Moreover, Bolt Parquet Reader is designed with a sophisticated filter push down strategies and range selectivity operations. Considering the consecutive reading progress, this feature is able to reduce unnecessary branching operations.

# Roadmap
This project is under actively development. You are more than welcomed to make contributions.

# How to Compile

### 1. Pull the code
```
git clone https://github.com/bytedance/bolt-parquet-reader.git
```

### 2. Compile and Execute
```
cargo +nightly fmt --all
cargo +nightly fmt --all -- --check
cargo +nightly build --package bolt-parquet-reader --lib
cargo +nightly test --verbose
cargo +nightly clippy --verbose
```

# License
The Bolt Parquet Reader is licensed under Apache 2.0.

During the development, we referenced a lot to Rust Arrow 2 implementation and would like to express our appreciation the authors.