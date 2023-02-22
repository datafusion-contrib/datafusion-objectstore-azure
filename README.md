# DataFusion-ObjectStore-Azure

🚨 WARNING 🚨: This project is archived, as datafusion has moved to the object_store crate, which natively supports Azure.

Azure storage account as an ObjectStore for [Datafusion](https://github.com/apache/arrow-datafusion).

## Querying files on Azure Storage with DataFusion

This crate implements the DataFusion `ObjectStore` trait on Azure Storage. We leverage the still unofficial
[Azure-SDK-for-Rust](https://github.com/Azure/azure-sdk-for-rust) for interacting with Azure. As such we can
make no assurances on API stability either on Azure's part or within this crate. This crates API is tightly
connected with DataFusion, a fast moving project, and as such we will make changes inline with those upstream changes.

## Examples

Load credentials from default AWS credential provider (such as environment or ~/.aws/credentials)

```rust
let azure_file_system = Arc::new(AzureFileSystem::default().await);
```

`AzureFileSystem::default()` is a convenience wrapper for `AzureFileSystem::new(None, None, None, None, None, None)`.

Connect using access key and secret.

```rust
// Example credentials provided by MinIO
const AZURE_STORAGE_ACCOUNT: &str = "...";
const AZURE_STORAGE_KEY: &str = "...";

let azure_file_system = AzureFileSystem::new(AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY).await;
```

Using DataFusion's `ListingTableConfig` we register a table into a DataFusion `SessionContext` so that it can be queried.

```rust
let filename = "data/alltypes_plain.snappy.parquet";
let config = ListingTableConfig::new(azure_file_system, filename).infer().await?;

let table = ListingTable::try_new(config)?;
let mut ctx = SessionContext::new();
ctx.register_table("tbl", Arc::new(table))?;

let df = ctx.sql("SELECT * FROM tbl").await?;
df.show()
```

We can also register the `AzureFileSystem` directly as an `ObjectStore` on an `SessionContext`.
This provides an idiomatic way of creating `TableProviders` that can be queried.

```rust
session_ctx.register_object_store(
    "adls2",
    Arc::new(AzureFileSystem::default().await),
);

let input_uri = "adls2://parquet-testing/data/alltypes_plain.snappy.parquet";

let (object_store, _) = ctx.object_store(input_uri)?;

let config = ListingTableConfig::new(azure_file_system, filename).infer().await?;

let mut table_provider: Arc<dyn TableProvider + Send + Sync> = Arc::new(ListingTable::try_new(config)?);
```

## Testing

For testing we aim to support the `mock_testing_framework` used within the
[`azure-sdk-for-rust`](https://github.com/Azure/azure-sdk-for-rust), however right now we
still need to use the blob client for which the testing framework is not yet implemented.

As no appropriate mocking server exists for the ADLS Gen2 storage accounts, tests need to be executed
locally, against a storage account provided by the developer.

First clone the test data repository:

```bash
git submodule update --init --recursive
```

When this does not work, manually run the following:

```bash
git submodule add -f https://github.com/apache/parquet-testing.git parquet-testing
```

Then create a container in your storage account:

```bash
# create the AZURE_STORAGE_ACCOUNT 'datafusion'
az storage account create --resource-group datafusion --kind StorageV2 --location westeurope --sku Standard_LRS --name datafusion
# fetch the AZURE_STORAGE_KEY
az storage account keys list -g datafusion -n datafusion | jq -r ".[0].value"

# create and fill container with data
az storage container create --resource-group datafusion --account-name datafusion --public-access container --name parquet-testing-data
az storage blob directory upload --account-name datafusion --container parquet-testing-data -s "./parquet-testing/data/*" -d . --recursive

# create and fill container for bad_data
az storage container create --resource-group datafusion --account-name datafusion --public-access container --name parquet-testing-bad-data
az storage blob directory upload --account-name datafusion --container parquet-testing-bad-data -s "./parquet-testing/bad_data/*" -d . --recursive
```

Place a file called `.env` in the root of the repo (this is ignored by `.gitignore`) and store provide credentials

```toml
AZURE_STORAGE_ACCOUNT="..."
AZURE_STORAGE_KEY="..."
```

Then execute the tests

```sh
cargo test
```
