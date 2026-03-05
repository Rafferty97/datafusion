// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Execution plan for reading JSON files (line-delimited and array formats)

use std::any::Any;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::task::Poll;

use crate::file_format::JsonDecoder;

use datafusion_common::error::{DataFusionError, Result};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common_runtime::JoinSet;
use datafusion_datasource::decoder::{DecoderDeserializer, deserialize_stream};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::{
    ListingTableUrl, PartitionedFile, RangeCalculation, as_file_source, calculate_range,
};
use datafusion_physical_plan::projection::ProjectionExprs;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use arrow::json::ReaderBuilder;
use arrow::{datatypes::SchemaRef, json};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;

use futures::{StreamExt, TryStreamExt};
use object_store::buffered::BufWriter;
use object_store::{GetOptions, GetResultPayload, ObjectStore};
use tokio::io::AsyncWriteExt;

/// A [`FileOpener`] that opens a JSON file and yields a [`FileOpenFuture`]
pub struct JsonOpener {
    batch_size: usize,
    projected_schema: SchemaRef,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
    /// When `true` (default), expects newline-delimited JSON (NDJSON).
    /// When `false`, expects JSON array format `[{...}, {...}]`.
    newline_delimited: bool,
    /// When `true`, each item is treated as the value for the sole field in the schema.
    /// When `false`, each item is treated as an object whose keys map to the fields in the schema.
    single_field: bool,
}

impl JsonOpener {
    /// Returns a [`JsonOpener`]
    pub fn new(
        batch_size: usize,
        projected_schema: SchemaRef,
        file_compression_type: FileCompressionType,
        object_store: Arc<dyn ObjectStore>,
        newline_delimited: bool,
        single_field: bool,
    ) -> Self {
        Self {
            batch_size,
            projected_schema,
            file_compression_type,
            object_store,
            newline_delimited,
            single_field,
        }
    }
}

/// JsonSource holds the extra configuration that is necessary for [`JsonOpener`]
#[derive(Clone)]
pub struct JsonSource {
    table_schema: datafusion_datasource::TableSchema,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projection: SplitProjection,
    /// When `true` (default), expects newline-delimited JSON (NDJSON).
    /// When `false`, expects JSON array format `[{...}, {...}]`.
    newline_delimited: bool,
    /// When `true`, each item is treated as the value for the sole field in the schema.
    /// When `false`, each item is treated as an object whose keys map to the fields in the schema.
    single_field: bool,
}

impl JsonSource {
    /// Initialize a JsonSource with the provided schema
    pub fn new(table_schema: impl Into<datafusion_datasource::TableSchema>) -> Self {
        let table_schema = table_schema.into();
        Self {
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
            newline_delimited: true,
            single_field: false,
        }
    }

    /// Set whether to read as newline-delimited JSON.
    ///
    /// When `true` (default), expects newline-delimited format.
    /// When `false`, expects JSON array format `[{...}, {...}]`.
    pub fn with_newline_delimited(mut self, newline_delimited: bool) -> Self {
        self.newline_delimited = newline_delimited;
        self
    }

    /// Set whether to treat each item in the file as the value of the sole field in the schema,
    /// where an "item" is a single line in a newline-delimited JSON file,
    /// or an element of the top-level array in a JSON array format file.
    ///
    /// When `true`, each item is treated as the value for the sole field in the schema.
    /// When `false` (default), each item is treated as an object whose keys map to the fields in the schema.
    pub fn with_single_field(mut self, single_field: bool) -> Self {
        self.single_field = single_field;
        self
    }
}

impl From<JsonSource> for Arc<dyn FileSource> {
    fn from(source: JsonSource) -> Self {
        as_file_source(source)
    }
}

impl FileSource for JsonSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        // Get the projected file schema for JsonOpener
        let file_schema = self.table_schema.file_schema();
        let projected_schema =
            Arc::new(file_schema.project(&self.projection.file_indices)?);

        let mut opener = Arc::new(JsonOpener {
            batch_size: self
                .batch_size
                .expect("Batch size must set before creating opener"),
            projected_schema,
            file_compression_type: base_config.file_compression_type,
            object_store,
            newline_delimited: self.newline_delimited,
            single_field: self.single_field,
        }) as Arc<dyn FileOpener>;

        // Wrap with ProjectionOpener
        opener = ProjectionOpener::try_new(
            self.projection.clone(),
            Arc::clone(&opener),
            self.table_schema.file_schema(),
        )?;

        Ok(opener)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &datafusion_datasource::TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        let split_projection =
            SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        source.projection = split_projection;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "json"
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &dyn datafusion_physical_plan::PhysicalExpr,
        ) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // Visit projection expressions
        let mut tnr = TreeNodeRecursion::Continue;
        for proj_expr in &self.projection.source {
            tnr = tnr.visit_sibling(|| f(proj_expr.expr.as_ref()))?;
        }
        Ok(tnr)
    }
}

impl FileOpener for JsonOpener {
    /// Open a partitioned JSON file.
    ///
    /// If `file_meta.range` is `None`, the entire file is opened.
    /// Else `file_meta.range` is `Some(FileRange{start, end})`, which corresponds to the byte range [start, end) within the file.
    ///
    /// Note: `start` or `end` might be in the middle of some lines. In such cases, the following rules
    /// are applied to determine which lines to read:
    /// 1. The first line of the partition is the line in which the index of the first character >= `start`.
    /// 2. The last line of the partition is the line in which the byte at position `end - 1` resides.
    ///
    /// Note: JSON array format does not support range-based scanning.
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let schema = Arc::clone(&self.projected_schema);
        let batch_size = self.batch_size;
        let file_compression_type = self.file_compression_type.to_owned();
        let newline_delimited = self.newline_delimited;
        let single_field = self.single_field;

        // JSON array format requires reading the complete file
        if !newline_delimited && partitioned_file.range.is_some() {
            return Err(DataFusionError::NotImplemented(
                "JSON array format does not support range-based file scanning. \
                 Disable repartition_file_scans or use newline-delimited JSON format."
                    .to_string(),
            ));
        }

        Ok(Box::pin(async move {
            let calculated_range =
                calculate_range(&partitioned_file, &store, None).await?;

            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(range)) => Some(range.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(
                        futures::stream::poll_fn(move |_| Poll::Ready(None)).boxed()
                    );
                }
            };

            let options = GetOptions {
                range,
                ..Default::default()
            };

            let result = store
                .get_opts(&partitioned_file.object_meta.location, options)
                .await?;

            let reader_builder = if single_field {
                ReaderBuilder::new(schema)
            } else {
                debug_assert!(schema.fields().len() == 1);
                ReaderBuilder::new_with_field(schema.fields()[0].clone())
            };
            let reader_builder = reader_builder
                .with_batch_size(batch_size)
                .with_flatten(!newline_delimited);

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => {
                    let bytes = match partitioned_file.range {
                        None => file_compression_type.convert_read(file)?,
                        Some(_) => {
                            file.seek(SeekFrom::Start(result.range.start as _))?;
                            let limit = result.range.end - result.range.start;
                            file_compression_type.convert_read(file.take(limit))?
                        }
                    };

                    let reader = BufReader::new(bytes);
                    let arrow_reader = reader_builder.build(reader)?;

                    Ok(futures::stream::iter(arrow_reader)
                        .map(|r| r.map_err(Into::into))
                        .boxed())
                }
                GetResultPayload::Stream(s) => {
                    let s = s.map_err(DataFusionError::from);
                    let decoder = reader_builder.build_decoder()?;
                    let input = file_compression_type.convert_stream(s.boxed())?.fuse();
                    let stream = deserialize_stream(
                        input,
                        DecoderDeserializer::new(JsonDecoder::new(decoder)),
                    );
                    Ok(stream.map_err(Into::into).boxed())
                }
            }
        }))
    }
}

pub async fn plan_to_json(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
) -> Result<()> {
    let path = path.as_ref();
    let parsed = ListingTableUrl::parse(path)?;
    let object_store_url = parsed.object_store();
    let store = task_ctx.runtime_env().object_store(&object_store_url)?;
    let writer_buffer_size = task_ctx
        .session_config()
        .options()
        .execution
        .objectstore_writer_buffer_size;
    let mut join_set = JoinSet::new();
    for i in 0..plan.output_partitioning().partition_count() {
        let storeref = Arc::clone(&store);
        let plan: Arc<dyn ExecutionPlan> = Arc::clone(&plan);
        let filename = format!("{}/part-{i}.json", parsed.prefix());
        let file = object_store::path::Path::parse(filename)?;

        let mut stream = plan.execute(i, Arc::clone(&task_ctx))?;
        join_set.spawn(async move {
            let mut buf_writer =
                BufWriter::with_capacity(storeref, file.clone(), writer_buffer_size);

            let mut buffer = Vec::with_capacity(1024);
            while let Some(batch) = stream.next().await.transpose()? {
                let mut writer = json::LineDelimitedWriter::new(buffer);
                writer.write(&batch)?;
                buffer = writer.into_inner();
                buf_writer.write_all(&buffer).await?;
                buffer.clear();
            }

            buf_writer.shutdown().await.map_err(DataFusionError::from)
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => res?, // propagate DataFusion error
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use datafusion_datasource::FileRange;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    /// Helper to create a test schema
    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[tokio::test]
    async fn test_json_array_from_file() -> Result<()> {
        // Test reading JSON array format from a file
        let json_data = r#"[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]"#;

        let store = Arc::new(InMemory::new());
        let path = Path::from("test.json");
        store
            .put(&path, PutPayload::from_static(json_data.as_bytes()))
            .await?;

        let opener = JsonOpener::new(
            1024,
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false, // JSON array format
            false,
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let stream = opener.open(file)?.await?;
        let batches: Vec<_> = stream.try_collect().await?;

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_from_stream() -> Result<()> {
        // Test reading JSON array format from object store stream (simulates S3)
        let json_data = r#"[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}, {"id": 3, "name": "charlie"}]"#;

        // Use InMemory store which returns Stream payload
        let store = Arc::new(InMemory::new());
        let path = Path::from("test_stream.json");
        store
            .put(&path, PutPayload::from_static(json_data.as_bytes()))
            .await?;

        let opener = JsonOpener::new(
            2, // small batch size to test multiple batches
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false, // JSON array format
            false,
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let stream = opener.open(file)?.await?;
        let batches: Vec<_> = stream.try_collect().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_nested_objects() -> Result<()> {
        // Test JSON array with nested objects and arrays
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("data", DataType::Utf8, true),
        ]));

        let json_data = r#"[
            {"id": 1, "data": "{\"nested\": true}"},
            {"id": 2, "data": "[1, 2, 3]"}
        ]"#;

        let store = Arc::new(InMemory::new());
        let path = Path::from("nested.json");
        store
            .put(&path, PutPayload::from_static(json_data.as_bytes()))
            .await?;

        let opener = JsonOpener::new(
            1024,
            schema,
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false,
            false,
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let stream = opener.open(file)?.await?;
        let batches: Vec<_> = stream.try_collect().await?;

        assert_eq!(batches[0].num_rows(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_empty() -> Result<()> {
        // Test empty JSON array
        let json_data = "[]";

        let store = Arc::new(InMemory::new());
        let path = Path::from("empty.json");
        store
            .put(&path, PutPayload::from_static(json_data.as_bytes()))
            .await?;

        let opener = JsonOpener::new(
            1024,
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false,
            false,
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let stream = opener.open(file)?.await?;
        let batches: Vec<_> = stream.try_collect().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_range_not_supported() {
        // Test that range-based scanning returns error for JSON array format
        let store = Arc::new(InMemory::new());
        let path = Path::from("test.json");
        store
            .put(&path, PutPayload::from_static(b"[]"))
            .await
            .unwrap();

        let opener = JsonOpener::new(
            1024,
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false, // JSON array format
            false,
        );

        let meta = store.head(&path).await.unwrap();
        let mut file = PartitionedFile::new(path.to_string(), meta.size);
        file.range = Some(FileRange { start: 0, end: 10 });

        let result = opener.open(file);
        match result {
            Ok(_) => panic!("Expected error for range-based JSON array scanning"),
            Err(e) => {
                assert!(
                    e.to_string().contains("does not support range-based"),
                    "Unexpected error message: {e}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_ndjson_still_works() -> Result<()> {
        // Ensure NDJSON format still works correctly
        let json_data =
            "{\"id\": 1, \"name\": \"alice\"}\n{\"id\": 2, \"name\": \"bob\"}\n";

        let store = Arc::new(InMemory::new());
        let path = Path::from("test.ndjson");
        store
            .put(&path, PutPayload::from_static(json_data.as_bytes()))
            .await?;

        let opener = JsonOpener::new(
            1024,
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            true, // NDJSON format
            false,
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let stream = opener.open(file)?.await?;
        let batches: Vec<_> = stream.try_collect().await?;

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_large_file() -> Result<()> {
        // Test with a larger JSON array to verify streaming works
        let mut json_data = String::from("[");
        for i in 0..1000 {
            if i > 0 {
                json_data.push(',');
            }
            json_data.push_str(&format!(r#"{{"id": {i}, "name": "user{i}"}}"#));
        }
        json_data.push(']');

        let store = Arc::new(InMemory::new());
        let path = Path::from("large.json");
        store
            .put(&path, PutPayload::from(Bytes::from(json_data)))
            .await?;

        let opener = JsonOpener::new(
            100, // batch size of 100
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false,
            false,
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let stream = opener.open(file)?.await?;
        let batches: Vec<_> = stream.try_collect().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1000);

        // Should have multiple batches due to batch_size=100
        assert!(batches.len() >= 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_stream_cancellation() -> Result<()> {
        // Test that cancellation works correctly (tasks are aborted when stream is dropped)
        let mut json_data = String::from("[");
        for i in 0..10000 {
            if i > 0 {
                json_data.push(',');
            }
            json_data.push_str(&format!(r#"{{"id": {i}, "name": "user{i}"}}"#));
        }
        json_data.push(']');

        let store = Arc::new(InMemory::new());
        let path = Path::from("cancel_test.json");
        store
            .put(&path, PutPayload::from(Bytes::from(json_data)))
            .await?;

        let opener = JsonOpener::new(
            10, // small batch size
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false,
            false,
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let mut stream = opener.open(file)?.await?;

        // Read only first batch, then drop the stream (simulating cancellation)
        let first_batch = stream.next().await;
        assert!(first_batch.is_some());

        // Drop the stream - this should abort the spawned tasks via SpawnedTask's Drop
        drop(stream);

        // Give tasks time to be aborted
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // If we reach here without hanging, cancellation worked
        Ok(())
    }
}
