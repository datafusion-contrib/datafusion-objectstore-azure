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

//! ObjectStore implementation for the Azure Datalake Gen2 API
use std::io::{self, Read};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use async_trait::async_trait;
use azure_core::new_http_client;
use azure_core::prelude::Range;
use azure_storage::core::clients::{AsStorageClient, StorageAccountClient, StorageClient};
use azure_storage::core::storage_shared_key_credential::StorageSharedKeyCredential;
use azure_storage_blobs::prelude::*;
use azure_storage_datalake::clients::{DataLakeClient, FileClient};
use bytes::Buf;
use datafusion_data_access::{
    object_store::{FileMetaStream, ListEntryStream, ObjectReader, ObjectStore},
    FileMeta, Result as DFAccessResult, SizedFile,
};
use futures::{future::BoxFuture, AsyncRead, AsyncSeekExt};
use range_reader::{RangeOutput, RangedAsyncReader};

async fn new_client(account_name: String, account_key: String) -> DataLakeClient {
    let credential = StorageSharedKeyCredential::new(account_name, account_key);
    DataLakeClient::new(credential, None)
}

async fn new_blob_client(account_name: String, account_key: String) -> Arc<StorageClient> {
    let http_client = new_http_client();
    let storage_account_client =
        StorageAccountClient::new_access_key(http_client.clone(), account_name, account_key);
    storage_account_client.as_storage_client()
    // let container_client = storage_client.as_container_client(file_system_name.to_owned());
}

/// `ObjectStore` implementation for the Azure Datalake Gen2 API
#[derive(Debug)]
pub struct AzureFileSystem {
    client: DataLakeClient,
    storage_client: Arc<StorageClient>,
}

impl AzureFileSystem {
    /// Create new `AzureFileSystem`
    pub async fn new<A, K>(account_name: A, account_key: K) -> Self
    where
        A: Into<String>,
        K: Into<String>,
    {
        let account: String = account_name.into();
        let key: String = account_key.into();
        Self {
            client: new_client(account.clone(), key.clone()).await,
            storage_client: new_blob_client(account, key).await,
        }
    }
}

#[async_trait]
impl ObjectStore for AzureFileSystem {
    async fn list_file(&self, prefix: &str) -> DFAccessResult<FileMetaStream> {
        let (file_system, prefix) = match prefix.split_once('/') {
            Some((file_system, prefix)) => (file_system.to_owned(), prefix),
            None => (prefix.to_owned(), ""),
        };

        // TODO use datalake client once https://github.com/Azure/azure-sdk-for-rust/issues/720 is resolved
        let container_client = self.storage_client.as_container_client(&file_system);
        let objs = container_client
            .list_blobs()
            .prefix(prefix)
            .execute()
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?
            .blobs
            .blobs
            .into_iter()
            .filter_map(|blob| {
                let sized_file = SizedFile {
                    size: blob.properties.content_length,
                    path: format!("{}/{}", &file_system, blob.name),
                };
                if sized_file.size > 0 {
                    Some(Ok(FileMeta {
                        sized_file,
                        last_modified: Some(blob.properties.last_modified),
                    }))
                } else {
                    None
                }
            })
            .collect::<Vec<Result<FileMeta, _>>>();
        Ok(Box::pin(futures::stream::iter(objs)))
    }

    async fn list_dir(
        &self,
        _prefix: &str,
        _delimiter: Option<String>,
    ) -> DFAccessResult<ListEntryStream> {
        todo!()
    }

    fn file_reader(&self, file: SizedFile) -> DFAccessResult<Arc<dyn ObjectReader>> {
        let (file_system, path) = match file.path.split_once('/') {
            Some((file_system, prefix)) => Ok((file_system.to_owned(), prefix)),
            // We can never have a file at the root, since we expect the file system to be part of the path
            None => Err(io::Error::from(io::ErrorKind::InvalidInput)),
        }?;
        let client = self
            .client
            .clone()
            .into_file_system_client(file_system)
            .into_file_client(path);
        Ok(Arc::new(AzureFileReader::new(client, file)?))
    }
}

struct AzureFileReader {
    client: FileClient,
    file: SizedFile,
}

impl AzureFileReader {
    fn new(client: FileClient, file: SizedFile) -> DFAccessResult<Self> {
        Ok(Self { client, file })
    }
}

#[async_trait]
impl ObjectReader for AzureFileReader {
    async fn chunk_reader(&self, start: u64, length: usize) -> DFAccessResult<Box<dyn AsyncRead>> {
        let client = self.client.clone();

        let range_get = Box::new(move |range_start: u64, range_length: usize| {
            let get_object = client.read();

            Box::pin(async move {
                let get_object = get_object.clone();

                let response = get_object
                    .range(Range::new(range_start, range_start + range_length as u64))
                    .into_future()
                    .await;

                let mut data = match response {
                    Ok(res) => Ok(res.data),
                    Err(err) => Err(io::Error::new(io::ErrorKind::Other, err)),
                }?
                .to_vec();
                data.truncate(range_length);
                Ok(RangeOutput {
                    start: range_start,
                    data: data.to_vec(),
                })
            }) as BoxFuture<'static, std::io::Result<RangeOutput>>
        });

        // at least 4kb per request
        let mut reader = RangedAsyncReader::new(length, 4 * 1024, range_get);
        reader.seek(std::io::SeekFrom::Start(start)).await?;

        Ok(Box::new(reader))
    }

    fn sync_chunk_reader(
        &self,
        start: u64,
        length: usize,
    ) -> DFAccessResult<Box<dyn Read + Send + Sync>> {
        // let file_path = self.file.path.clone();
        let client = self.client.clone();

        // once the async chunk file readers have been implemented this complexity can be removed
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let get_object = client.read();
                let resp = if length > 0 {
                    // range bytes requests are inclusive
                    get_object
                        .range(Range::new(start, start + length as u64))
                        .into_future()
                        .await
                } else {
                    get_object.into_future().await
                };

                let bytes = match resp {
                    Ok(res) => Ok(res.data),
                    Err(err) => Err(io::Error::new(io::ErrorKind::Other, err)),
                };

                tx.send(bytes).unwrap();
            })
        });

        let bytes = rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))??;

        Ok(Box::new(bytes.reader()))
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;
    use datafusion::datasource::listing::*;
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;

    // Test that `AzureFileSystem` can read files
    #[tokio::test]
    async fn test_read_files() -> DFAccessResult<()> {
        let account_name = crate::test_utils::get_account();
        let account_key = crate::test_utils::get_key();
        let azure_file_system = AzureFileSystem::new(account_name, account_key).await;

        let mut files = azure_file_system.list_file("parquet-testing-data").await?;

        while let Some(file) = files.next().await {
            let sized_file = file.unwrap().sized_file;
            println!("{:?}", sized_file);
            let mut reader = azure_file_system
                .file_reader(sized_file.clone())
                .unwrap()
                .sync_chunk_reader(0, sized_file.size as usize)
                .unwrap();

            let mut bytes = Vec::new();
            let size = reader.read_to_end(&mut bytes)?;

            assert_eq!(size as u64, sized_file.size);
        }

        Ok(())
    }

    // Test that reading files with `AzureFileSystem` produces the expected results
    #[tokio::test]
    async fn test_read_range() -> DFAccessResult<()> {
        let start = 10;
        let length = 128;

        let mut file = std::fs::File::open("parquet-testing/data/alltypes_plain.snappy.parquet")?;
        let mut raw_bytes = Vec::new();
        file.read_to_end(&mut raw_bytes)?;
        let raw_slice = &raw_bytes[start..start + length];
        assert_eq!(raw_slice.len(), length);

        let account_name = crate::test_utils::get_account();
        let account_key = crate::test_utils::get_key();
        let azure_file_system = AzureFileSystem::new(account_name, account_key).await;

        let mut files = azure_file_system
            .list_file("parquet-testing-data/alltypes_plain.snappy.parquet")
            .await?;

        if let Some(file) = files.next().await {
            let sized_file = file.unwrap().sized_file;
            let mut reader = azure_file_system
                .file_reader(sized_file)
                .unwrap()
                .sync_chunk_reader(start as u64, length)
                .unwrap();

            let mut reader_bytes = Vec::new();
            let size = reader.read_to_end(&mut reader_bytes)?;

            assert_eq!(size, length);
            assert_eq!(&reader_bytes, raw_slice);
        }

        Ok(())
    }

    // Test that reading Parquet file with `AzureFileSystem` can create a `ListingTable`
    #[tokio::test]
    async fn test_read_parquet() -> DFAccessResult<()> {
        let account_name = crate::test_utils::get_account();
        let account_key = crate::test_utils::get_key();
        let azure_file_system = Arc::new(AzureFileSystem::new(account_name, account_key).await);

        let filename = "parquet-testing-data/alltypes_plain.snappy.parquet";

        let config = ListingTableConfig::new(azure_file_system, filename)
            .infer()
            .await
            .unwrap();

        let table = ListingTable::try_new(config).unwrap();

        let exec = table.scan(&None, &[], Some(1024)).await.unwrap();
        assert_eq!(exec.statistics().num_rows, Some(2));

        Ok(())
    }

    // Test that a SQL query can be executed on a Parquet file that was read from `AzureFileSystem`
    #[tokio::test]
    async fn test_sql_query() -> DFAccessResult<()> {
        let account_name = crate::test_utils::get_account();
        let account_key = crate::test_utils::get_key();
        let azure_file_system = Arc::new(AzureFileSystem::new(account_name, account_key).await);
        let filename = "parquet-testing-data/alltypes_plain.snappy.parquet";

        let config = ListingTableConfig::new(azure_file_system, filename)
            .infer()
            .await
            .unwrap();

        let table = ListingTable::try_new(config).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("tbl", Arc::new(table)).unwrap();

        let batches = ctx
            .sql("SELECT * FROM tbl")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected = vec![
        "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
        "| id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |",
        "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
        "| 6  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30342f30312f3039 | 30         | 2009-04-01 00:00:00 |",
        "| 7  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30342f30312f3039 | 31         | 2009-04-01 00:01:00 |",
        "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+"
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    // Test that the AzureFileSystem allows reading from different buckets
    // #[tokio::test]
    // #[should_panic(expected = "Could not parse metadata: bad data")]
    // async fn test_read_alternative_bucket() {
    //     let account_name = crate::test_utils::get_account();
    //     let account_key = crate::test_utils::get_key();
    //     let azure_file_system = Arc::new(AzureFileSystem::new(account_name, account_key).await);
    //     let filename = "parquet-testing-bad-data/PARQUET-1481.parquet";
    //
    //     let config = ListingTableConfig::new(azure_file_system, filename)
    //         .infer()
    //         .await
    //         .unwrap();
    //
    //     let table = ListingTable::try_new(config).unwrap();
    //
    //     table.scan(&None, &[], Some(1024)).await.unwrap();
    // }

    // Test that `AzureFileSystem` can be registered as object store on a DataFusion `ExecutionContext`
    #[tokio::test]
    async fn test_ctx_register_object_store() -> DFAccessResult<()> {
        let account_name = crate::test_utils::get_account();
        let account_key = crate::test_utils::get_key();
        let azure_file_system = Arc::new(AzureFileSystem::new(account_name, account_key).await);

        let ctx = SessionContext::new();
        ctx.runtime_env()
            .register_object_store("adls2", azure_file_system);

        let (_, name) = ctx.runtime_env().object_store("adls2").unwrap();
        assert_eq!(name, "adls2");

        Ok(())
    }

    //     // Test that an appropriate error message is produced for a non existent bucket
    //     #[tokio::test]
    //     #[should_panic(expected = "NoSuchBucket")]
    //     async fn test_read_nonexistent_bucket() {
    //         let azure_file_system = AzureFileSystem::new(
    //             Some(SharedCredentialsProvider::new(Credentials::new(
    //                 ACCESS_KEY_ID,
    //                 SECRET_ACCESS_KEY,
    //                 None,
    //                 None,
    //                 PROVIDER_NAME,
    //             ))),
    //             None,
    //             Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
    //             None,
    //             None,
    //             None,
    //         )
    //         .await;
    //
    //         let mut files = azure_file_system
    //             .list_file("nonexistent_data")
    //             .await
    //             .unwrap();
    //
    //         while let Some(file) = files.next().await {
    //             let sized_file = file.unwrap().sized_file;
    //             let mut reader = azure_file_system
    //                 .file_reader(sized_file.clone())
    //                 .unwrap()
    //                 .sync_chunk_reader(0, sized_file.size as usize)
    //                 .unwrap();
    //
    //             let mut bytes = Vec::new();
    //             let size = reader.read_to_end(&mut bytes).unwrap();
    //
    //             assert_eq!(size as u64, sized_file.size);
    //         }
    //     }

    // Test that no files are returned if a non existent file URI is provided
    #[tokio::test]
    async fn test_read_nonexistent_file() {
        let account_name = crate::test_utils::get_account();
        let account_key = crate::test_utils::get_key();
        let azure_file_system = Arc::new(AzureFileSystem::new(account_name, account_key).await);

        let mut files = azure_file_system
            .list_file("parquet-testing-data/nonexistent_file.txt")
            .await
            .unwrap();

        assert!(files.next().await.is_none())
    }
}
