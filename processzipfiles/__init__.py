import azure.functions as func
import logging
import pyzipper
import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import time

# Service Principal (SP) account details for connecting to storage account
tenant_id = 'e28d23e3-803d-418d-a720-c0bed39f77b6'
client_id = '3772ed4b-6645-448a-8ba3-4efcdcc76b9e'
client_secret = 'S3V8Q~Z0Sd5eVepKvU1lxRYFFLX4FIIkeSXFZaJl'
account_url = 'https://storagecc3.blob.core.windows.net'

# Azure Key Vault details
key_vault_url = 'https://kvomifile.vault.azure.net/'  # Replace with your Key Vault URL
secret_name = 'passwordfile'  # Replace with the secret name in your Key Vault

# Create a credential object
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Create BlobServiceClient and SecretClient objects
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

# Retrieve the password from Azure Key Vault
password = secret_client.get_secret(secret_name).value

# Define the FunctionApp
app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="datafiles/input_files", connection="AzureWebJobsStorage")
def BlobTrigger1(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name} \n"
                 f"Blob Size: {myblob.length} bytes")

    # Container client for blob operations
    container_client = blob_service_client.get_container_client("datafiles/input_files")
    
    # Download the zip file from blob storage
    blob_client = container_client.get_blob_client(myblob.name)
    download_stream = blob_client.download_blob()
    zip_bytes = download_stream.readall()

    # Attempt to extract the zip file using the password
    try:
        with pyzipper.AESZipFile(io.BytesIO(zip_bytes), 'r', compression=pyzipper.ZIP_DEFLATED) as zf:
            zf.setpassword(password.encode('utf-8'))

            # Try to read the first file to check if the password is correct
            first_file = zf.namelist()[0]
            zf.read(first_file)
            logging.info(f"Password is correct for {myblob.name}. Extracting files...")

            for file_name in zf.namelist():
                logging.info(f"Extracting {file_name}")
                extracted_data = zf.read(file_name)

                # Save the extracted file to another blob location
                extracted_blob_name = f'extracted/input_files/{file_name}'
                extracted_blob_client = container_client.get_blob_client(extracted_blob_name)
                extracted_blob_client.upload_blob(extracted_data, overwrite=True)

        # Move the processed zip file to archived_files
        archived_blob_name = f'archived/{myblob.name.split("/")[-1]}'
        archived_blob_client = container_client.get_blob_client(archived_blob_name)

        # Start copying the blob to the archive location
        archived_blob_client.start_copy_from_url(blob_client.url)

        # Ensure the copy operation is complete
        while archived_blob_client.get_blob_properties().copy.status != 'success':
            time.sleep(1)  # Wait for the copy operation to complete

        # Delete the original blob after successful copy
        blob_client.delete_blob()

    except RuntimeError:
        logging.error(f"Incorrect password for {myblob.name}. Skipping this file.")
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

# Function App setup
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.blob_trigger(arg_name="myblob", path="datafiles/input_files", connection="AzureWebJobsStorage")
def BlobTrigger1(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob - Name: {myblob.name}, Blob Size: {myblob.length} bytes")

    # Retrieve password for zip file from Key Vault
    password_secret = secret_client.get_secret(secret_name)
    zip_password = password_secret.value.encode('utf-8')  # Convert to bytes

    # Prepare blob name for archive and extraction
    extracted_files_container = "datafiles/extracted_input_files"
    archived_files_container = "datafiles/archived_files"

    try:
        # Read the zip file from blob
        with io.BytesIO(myblob.read()) as blob_data:
            # Open the zip file using pyzipper and password
            with pyzipper.AESZipFile(blob_data) as zip_file:
                zip_file.pwd = zip_password
                
                # List all files in the zip and extract each one
                for file_name in zip_file.namelist():
                    logging.info(f"Extracting file: {file_name}")
                    
                    # Read the file content from the zip
                    extracted_data = zip_file.read(file_name)
                    
                    # Upload extracted file to the 'extracted_input_files' container
                    extracted_blob_client = blob_service_client.get_blob_client(container=extracted_files_container, blob=file_name)
                    extracted_blob_client.upload_blob(extracted_data)
                    
                    logging.info(f"File {file_name} extracted and uploaded to {extracted_files_container}")
        
        # After extraction, move the original zip file to the archive folder
        archive_blob_client = blob_service_client.get_blob_client(container=archived_files_container, blob=myblob.name)
        archive_blob_client.start_copy_from_url(myblob.uri)
        logging.info(f"Original zip file moved to {archived_files_container}")

    except Exception as e:
        logging.error(f"Error processing blob: {str(e)}")

    # Log completion of the function
    logging.info(f"Blob trigger function completed for blob {myblob.name}")

