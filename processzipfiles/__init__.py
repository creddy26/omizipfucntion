import azure.functions as func
import logging
import pyzipper
import io
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import time

# Service Principal details to connect to storage account
tenant_id = 'e28d23e3-803d-418d-a720-c0bed39f77b6'
client_id = '3772ed4b-6645-448a-8ba3-4efcdcc76b9e'
client_secret = 'S3V8Q~Z0Sd5eVepKvU1lxRYFFLX4FIIkeSXFZaJl'
account_url = 'https://storagecc3.blob.core.windows.net'

# Azure Key Vault details where .zip password file is stored
key_vault_url = 'https://kvomifile.vault.azure.net/'  # Key Vault URL
secret_name = 'passwordfile'  # Secret name in your Key Vault

# Create a credential object to connect to storage account using service principal account
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Create BlobServiceClient and SecretClient objects
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
secret_client = SecretClient(vault_url=key_vault_url, credential=credential)


app = func.FunctionApp()

@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger1(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')
    
    # Extract the blob details from the event data
    data = azeventgrid.get_json()
    blob_url = data['url']
    container_name = data['url'].split('/')[-2]
    blob_name = data['url'].split('/')[-1]

    # Check if the blob is a zip file
    if blob_name.endswith('.zip'):
        logging.info(f"Blob {blob_name} is a zip file. Proceeding with extraction.")
        
        # Get the container and blob client
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
        # Download the blob content
        zip_stream = io.BytesIO(blob_client.download_blob().readall())
        
        # Attempt to extract the zip file using the password
        try:
            with pyzipper.AESZipFile(zip_stream, 'r', compression=pyzipper.ZIP_DEFLATED) as zf:
                zf.setpassword(password.encode('utf-8'))
                
                # Try to read the first file to check if the password is correct
                first_file = zf.namelist()[0]
                zf.read(first_file)
                logging.info(f"Password is correct for {blob_name}. Extracting files...")
                
                # Extract all files and upload them to the 'extracted' folder in Blob Storage
                for file_name in zf.namelist():
                    logging.info(f"Extracting {file_name}")
                    extracted_data = zf.read(file_name)

                    # Save the extracted file to the output blob location
                    extracted_blob_name = f'extracted/input_files/{file_name}'
                    extracted_blob_client = container_client.get_blob_client(extracted_blob_name)
                    extracted_blob_client.upload_blob(extracted_data, overwrite=True)

                # Move the processed zip file to the 'archived' folder
                archived_blob_name = f'archived/{blob_name}'
                archived_blob_client = container_client.get_blob_client(archived_blob_name)

                # Start copying the blob to the archive location
                archived_blob_client.start_copy_from_url(blob_client.url)

                # Ensure the copy operation is complete
                while archived_blob_client.get_blob_properties().copy.status != 'success':
                    time.sleep(1)  # Wait for the copy operation to complete

                logging.info(f"Blob {blob_name} has been successfully archived.")
        
        except RuntimeError:
            logging.error(f"Incorrect password for {blob_name}. Skipping this file.")
    else:
        logging.info(f"File {blob_name} is not a .zip file and will not be processed.")
