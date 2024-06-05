## **Automated Cloud Composer Environment Deployment**

Click the button below to create a Cloud Composer and a service account in just a few clicks.

[![Open in Cloud Shell](https://gstatic.com/cloudssh/images/open-btn.svg)](https://shell.cloud.google.com/cloudshell/?terminal=true&show=terminal&cloudshell_git_repo=https%3A%2F%2Fgithub.com/dataplatr/cloud-composer-oneclick&cloudshell_tutorial=docs%2Ftutorial.md)

### Configuration

| Parameter | Default Value | Description |
|---|---|---|
| projectId | - |  Name of the Google Cloud project |
| CreateComposer | true | Execute the deployment for Cloud Composer. |
| location | us-central1 | The region where the environment is located. |
| serciceAccountName | custom-service-account | User-managed service account for Cloud Composer environments.
| composerEnvName | dev-composer | Name of the cloud composer environment. |
| composer_size | small | Composer Environment size small/medium/large. |

Note : For composerEnvName, name must start with a lowercase letter followed by up to 62 lowercase letters, numbers, or hyphens, and cannot end with a hyphen. The environment name is used to create subcomponents for the environment, so you must provide a name that is also valid as a Cloud Storage bucket name. See [Bucket naming guidelines](https://cloud.google.com/storage/docs/buckets#naming) for a list of restrictions.

### Choosing Environment Size
Select one of three environment presets based on the following estimates:

1. Total number of DAGs that you plan to deploy in the environment
2. Maximum number of concurrent DAG runs
3. Maximum number of concurrent tasks

Your environment's performance depends on the implementation of specific DAGs that you run in your environment. The following table lists estimates that are based on the average resource consumption. If you expect your DAGs to consume more resources, adjust the estimates accordingly.

| Recommended preset | Total DAGs | Max concurrent DAG runs | Max concurrent tasks
|---|---|---|---|
| Small | 50 | 15 | 18 |
| Medium | 250 | 60 | 100 |
| Large | 1000 | 250 | 400 |

For a guide about selecting optimal scale and performance parameters for your environment, see [Optimize environment performance and costs](https://cloud.google.com/composer/docs/composer-2/optimize-environments).
