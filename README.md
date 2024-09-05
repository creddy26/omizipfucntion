Azure Function - Zip File Extraction with Password from Azure Blob Storage
This Azure Function is triggered by Event Grid when a zip file is uploaded to Azure Blob Storage. It extracts the contents of password-protected zip files, saves the extracted files to another Blob Storage location, and archives the processed zip file.

Features
Password-protected Zip file extraction using pyzipper.
Azure Key Vault integration for retrieving the zip file password.
Azure Blob Storage for managing zip files and extracted content.
Event Grid Trigger to automate the process when a new file is uploaded.
Prerequisites
Azure Subscription
Create a Function App
Create a BlobTrigger function within the Function App.
Azure Storage Account with Blob Containers
Azure Key Vault with a secret containing the zip file password
Create a Service Principle to access the Storage Account - Contributor role.
Use Deployment Center withing Function App to todeploy code from git
Python 3.8+
Setup Instructions
1. Clone the repository
git clone <repository-url>
