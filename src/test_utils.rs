// #![cfg(feature = "mock_transport_framework")]
use azure_core::ClientOptions;
use azure_storage::storage_shared_key_credential::StorageSharedKeyCredential;
use azure_storage_datalake::prelude::*;
use dotenv::dotenv;
use std::error::Error;

// pub async fn create_data_lake_client(
//     transaction_name: &str,
// ) -> Result<DataLakeClient, Box<dyn Error + Send + Sync>> {
//     let account_name = (std::env::var(azure_core::mock::TESTING_MODE_KEY).as_deref()
//         == Ok(azure_core::mock::TESTING_MODE_RECORD))
//     .then(get_account)
//     .unwrap_or_else(String::new);
//
//     let account_key = (std::env::var(azure_core::mock::TESTING_MODE_KEY).as_deref()
//         == Ok(azure_core::mock::TESTING_MODE_RECORD))
//     .then(get_key)
//     .unwrap_or_else(String::new);
//
//     let options = ClientOptions::new_with_transaction_name(transaction_name.into());
//
//     Ok(DataLakeClient::new_with_shared_key(
//         StorageSharedKeyCredential::new(account_name, account_key),
//         None,
//         options,
//     ))
// }

pub fn get_account() -> String {
    dotenv().ok();
    std::env::var("AZURE_STORAGE_ACCOUNT").expect("Set env variable AZURE_STORAGE_ACCOUNT first!")
}

pub fn get_key() -> String {
    dotenv().ok();
    std::env::var("AZURE_STORAGE_KEY").expect("Set env variable AZURE_STORAGE_KEY first!")
}
