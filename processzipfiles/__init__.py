import azure.functions as func
import logging
import pyzipper
import io
from azure.storage.blob.aio import BlobServiceClient
from azure.identity.aio import ClientSecretCredential
from azure.keyvault.secrets.aio import SecretClient
import asyncio

# Service Principal details to connect to storage account
tenant_id = 'e28d23e3-803d-418d-a720-c0bed39f77b6'
client_id = '3772ed4b-6645-448a-8ba3-4efcdcc76b9e'
client_secret = 'S3V8Q~Z0Sd5eVepKvU1lxRYFFLX4FIIkeSXFZaJl'
account_url = 'https://storagecc3.blob.core.windows.net'

# Azure Key Vault details where .zip password file is stored
key_vault_url = 'https://kvomifile.vault.azure.net/'
secret_name = 'passwordfile'

# Create a credential object to connect to storage account using service principal account
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Create BlobServiceClient and SecretClient objects
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

# Function to retrieve the password from Azure Key Vault
async def get_secret_value(secret_client, secret_name):
    secret = await secret_client.get_secret(secret_name)
    return secret.value

# EventGrid trigger function definition
async def main(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')

    # Retrieve the password and print it at the beginning
    password = await get_secret_value(secret_client, secret_name)
    
      
    # Proceed with processing the event
    data = azeventgrid.get_json()
    blob_url = data['url']
    container_name = blob_url.split('/')[-2]
    blob_name = blob_url.split('/')[-1]

    if blob_name.endswith('.zip'):
        logging.info(f"Blob {blob_name} is a zip file. Proceeding with extraction.")
        container_client = blob_service_client.get_container_client(container_name)
        await process_zip_file(container_client, blob_name, password)
    else:
        logging.info(f"File {blob_name} is not a .zip file and will not be processed.")

# Function to process the zip file
async def process_zip_file(container_client, blob_name, password):
    async with container_client.get_blob_client(blob_name) as blob_client:
        blob_stream = await blob_client.download_blob()
        zip_stream = io.BytesIO(await blob_stream.readall())

        try:
            with pyzipper.AESZipFile(zip_stream, 'r', compression=pyzipper.ZIP_DEFLATED) as zf:
                zf.setpassword(password.encode('utf-8'))
                file_list = zf.namelist()
                logging.info(f"Password is correct for {blob_name}. Extracting {len(file_list)} files...")
                await asyncio.gather(*[
                    upload_extracted_file(container_client, file_name, zf.read(file_name))
                    for file_name in file_list
                ])
                await archive_blob(container_client, blob_name)

        except RuntimeError as e:
            logging.error(f"Incorrect password for {blob_name}. Error: {str(e)}")

# Function to upload extracted files to blob storage
async def upload_extracted_file(container_client, file_name, data):
    extracted_blob_name = f'extracted/input_files/{file_name}'
    async with container_client.get_blob_client(extracted_blob_name) as extracted_blob_client:
        await extracted_blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Extracted and uploaded {file_name}")

# Function to archive the processed zip file
async def archive_blob(container_client, blob_name):
    archived_blob_name = f'archived/{blob_name}'
    async with container_client.get_blob_client(archived_blob_name) as archived_blob_client, \
               container_client.get_blob_client(blob_name) as blob_client:
        await archived_blob_client.start_copy_from_url(blob_client.url)
        logging.info(f"Started archiving {blob_name}")
