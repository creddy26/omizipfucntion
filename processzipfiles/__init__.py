import azure.functions as func
import logging
import pyzipper
import io
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import asyncio

# Azure details - use environment variables or managed identity for security
tenant_id = 'your-tenant-id'
client_id = 'your-client-id'
client_secret = 'your-client-secret'
account_url = 'https://storagecc3.blob.core.windows.net'
key_vault_url = 'https://kvomifile.vault.azure.net/'  # Key Vault URL
secret_name = 'passwordfile'  # Secret name in your Key Vault

# Initialize once at the start
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

# Fetch password once and cache it
password = secret_client.get_secret(secret_name).value

app = func.FunctionApp()

@app.event_grid_trigger(arg_name="azeventgrid")
async def EventGridTrigger1(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')
    
    # Extract the blob details from the event data
    data = azeventgrid.get_json()
    blob_url = data['url']
    container_name = data['url'].split('/')[-2]
    blob_name = data['url'].split('/')[-1]

    if blob_name.endswith('.zip'):
        logging.info(f"Blob {blob_name} is a zip file. Proceeding with extraction.")
        
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
        # Download the blob content
        zip_stream = io.BytesIO(blob_client.download_blob().readall())

        try:
            with pyzipper.AESZipFile(zip_stream, 'r', compression=pyzipper.ZIP_DEFLATED) as zf:
                zf.setpassword(password.encode('utf-8'))
                zf.read(zf.namelist()[0])  # Test password with first file
                logging.info(f"Password is correct for {blob_name}. Extracting files...")
                
                # Extract all files and upload them asynchronously
                for file_name in zf.namelist():
                    logging.info(f"Extracting {file_name}")
                    extracted_data = zf.read(file_name)
                    extracted_blob_name = f'extracted/input_files/{file_name}'
                    extracted_blob_client = container_client.get_blob_client(extracted_blob_name)
                    await extracted_blob_client.upload_blob(extracted_data, overwrite=True)

                # Move the processed zip file to 'archived' folder
                archived_blob_name = f'archived/{blob_name}'
                archived_blob_client = container_client.get_blob_client(archived_blob_name)
                copy_operation = archived_blob_client.start_copy_from_url(blob_client.url)

                # Await the completion of the copy process asynchronously
                while (await archived_blob_client.get_blob_properties()).copy.status != 'success':
                    await asyncio.sleep(1)

                logging.info(f"Blob {blob_name} has been successfully archived.")
        
        except RuntimeError:
            logging.error(f"Incorrect password for {blob_name}. Skipping this file.")
    else:
        logging.info(f"File {blob_name} is not a .zip file and will not be processed.")
