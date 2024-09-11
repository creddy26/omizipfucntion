import azure.functions as func
import logging
import pyzipper
import io
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import time

# SP account details to connect to storage account
tenant_id = os.getenv('AZURE_TENANT_ID')
client_id = os.getenv('SP_CLIENT_ID')
client_secret = os.getenv('SP_CLIENT_SECRET')
account_url = os.getenv('AZURE_STORAGE_ACCOUNT_URL')

# Azure Key Vault details
key_vault_url = os.getenv('AZURE_KEY_VAULT_URL')
secret_name = os.getenv('AZURE_KEY_VAULT_SECRET_NAME') 

# Create a credential and clients
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

# Retrieve the password from Azure Key Vault
password = secret_client.get_secret(secret_name).value.encode('utf-8')

# Blob trigger function
app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="datafiles/input_files/{name}",
                  connection="AzureWebJobsStorage") 
def BlobTrigger1(myblob: func.InputStream, name: str):
    logging.info(f"Blob trigger function processed blob - Name: {name}, Size: {myblob.length} bytes")

    if not name.endswith('.zip'):
        logging.info(f"Skipping non-zip file: {name}")
        return

    container_client = blob_service_client.get_container_client('datafiles')
    blob_client = container_client.get_blob_client(f'input_files/{name}')
    
    try:
        zip_bytes = myblob.read()
        with pyzipper.AESZipFile(io.BytesIO(zip_bytes), 'r') as zf:
            zf.setpassword(password)
            for file_name in zf.namelist():
                extracted_data = zf.read(file_name)
                extracted_blob_name = f'extracted/input_files/{file_name}'
                container_client.get_blob_client(extracted_blob_name).upload_blob(extracted_data, overwrite=True)

        archived_blob_name = f'archived/{name}'
        archived_blob_client = container_client.get_blob_client(archived_blob_name)
        archived_blob_client.start_copy_from_url(blob_client.url)

        while archived_blob_client.get_blob_properties().copy.status != 'success':
            time.sleep(1)

        blob_client.delete_blob()
        logging.info(f"Successfully processed and archived {name}")

    except (RuntimeError, pyzipper.BadZipFile):
        logging.error(f"Failed to extract {name} due to incorrect password or bad zip file.")
