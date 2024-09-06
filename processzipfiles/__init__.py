import azure.functions as func
import logging
import pyzipper
import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import time

# SP account details to connect to storage account
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

# Define the container name
container_name = "your-container-name"  # Replace with the actual container name

# Get a reference to the container
container_client = blob_service_client.get_container_client(container_name)

# Assuming `blob` is passed or known
blob_name = "your-blob-name.zip"  # Replace with the actual blob name
blob_client = container_client.get_blob_client(blob_name)

# Download the zip file from blob storage
download_stream = blob_client.download_blob()
zip_bytes = download_stream.readall()

# Attempt to extract the zip file using the password
try:
    with pyzipper.AESZipFile(io.BytesIO(zip_bytes), 'r', compression=pyzipper.ZIP_DEFLATED) as zf:
        zf.setpassword(password.encode('utf-8'))
        
        # Try to read the first file to check if the password is correct
        first_file = zf.namelist()[0]
        zf.read(first_file)
        logging.info(f"Password is correct for {blob_name}. Extracting files...")

        for file_name in zf.namelist():
            logging.info(f"Extracting {file_name}")
            extracted_data = zf.read(file_name)

            # Process the extracted file (e.g., save to another blob location)
            extracted_blob_name = f'extracted/input_files/{file_name}'
            extracted_blob_client = container_client.get_blob_client(extracted_blob_name)
            extracted_blob_client.upload_blob(extracted_data, overwrite=True)

    # Move the processed zip file to archived_files then delete the .zip file
    archived_blob_name = f'archived/{blob_name.split("/")[-1]}'
    archived_blob_client = container_client.get_blob_client(archived_blob_name)

    # Start copying the blob to the archive location
    archived_blob_client.start_copy_from_url(blob_client.url)

    # Ensure the copy operation is complete
    while archived_blob_client.get_blob_properties().copy.status != 'success':
        time.sleep(1)  # Wait for the copy operation to complete

    # Delete the original blob after successful copy
    blob_client.delete_blob()

except RuntimeError:
    logging.error(f"Incorrect password for {blob_name}. Skipping this file.")
else:
    logging.info(f"File {blob_name} is not a .zip file and will not be processed.")
