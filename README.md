## **Automated Cloud Composer Environment Deployment**

If you want to create a development instance with automatic generation of BigQuery datasets and permission granting, make sure your user account has enough permissions to run builds, assign permissions to the Cloud Build service account, 
 and create artifacts in Cloud Storage and BigQuery. Additionally, ensure to have the Cloud Composer environment to run the dag code. Click the button below to proceed.

[![Open in Cloud Shell](https://gstatic.com/cloudssh/images/open-btn.svg)](https://shell.cloud.google.com/cloudshell/?terminal=true&show=terminal&cloudshell_git_repo=https%3A%2F%2Fgithub.com/dataplatr/cloud-composer-oneclick&cloudshell_tutorial=docs%2Ftutorial.md)

### Deployment configuration

| Parameter | Default Value | Description |
|---|---|---|
| projectId | - | Project where the source dataset is and the build will run. |
| CreateComposer | true | Execute the deployment for Cloud Composer. |
| location | “us-central1” | Location where the Cloud Composer Environment will be created. |
| serciceAccountName | - | Bucket where DAG related files will be generated. |
| projectNumber | "us-central1" | Location where dataform Repository will be created. |
| composerEnvName | - | This is where the raw data lands from ORACLE. |
