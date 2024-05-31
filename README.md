## **Automated Cloud Composer Environment Deployment**


[![Open in Cloud Shell](https://gstatic.com/cloudssh/images/open-btn.svg)](https://shell.cloud.google.com/cloudshell/?terminal=true&show=terminal&cloudshell_git_repo=https%3A%2F%2Fgithub.com/dataplatr/cloud-composer-oneclick&cloudshell_tutorial=docs%2Ftutorial.md)

### Deployment configuration

| Parameter | Default Value | Description |
|---|---|---|
| projectId | - | Project where the source dataset is and the build will run. |
| CreateComposer | true | Execute the deployment for Cloud Composer. |
| location | “us-central1” | The region where the environment is located. |
| serciceAccountName | - | User-managed service account for Cloud Composer environments. |
| projectNumber | "us-central1" | Location where dataform Repository will be created. |
| composerEnvName | - | Name of the cloud composer environment. |



Note : For composerEnvName, name must start with a lowercase letter followed by up to 62 lowercase letters, numbers, or hyphens, and cannot end with a hyphen. The environment name is used to create subcomponents for the environment, so you must provide a name that is also valid as a Cloud Storage bucket name. See [Bucket naming guidelines](https://cloud.google.com/storage/docs/buckets#naming) for a list of restrictions.
