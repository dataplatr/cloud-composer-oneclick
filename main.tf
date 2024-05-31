provider "google-beta" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "composer_api" {
  provider = google-beta
  project = var.project_id
  service = "composer.googleapis.com"
  // Disabling Cloud Composer API might irreversibly break all other
  // environments in your project.
  disable_on_destroy = false
}

resource "google_service_account" "custom_service_account" {
  provider = google-beta
  account_id   = var.sa_name
  display_name = "Example Custom Service Account"
}

resource "google_project_iam_member" "custom_service_account" {
  provider = google-beta
  project  = var.project_id
  member   = format("serviceAccount:%s", google_service_account.custom_service_account.email)
  // Role for Public IP environments
  role     = "roles/composer.worker"
}

resource "google_service_account_iam_member" "custom_service_account" {
  provider = google-beta
  service_account_id = google_service_account.custom_service_account.name
  role = "roles/composer.ServiceAgentV2Ext"
  member = "serviceAccount:service-${var.project_number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

resource "google_composer_environment" "composer_env" {
  name   = var.composer_env_name
  region = var.region
  labels = {
    env = "dev"
  }

 config {

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    software_config {
      image_version = "composer-2-airflow-2"
    }

    node_config {
      service_account = google_service_account.custom_service_account.email
    }

    workloads_config {
      worker {
        min_count = 1
        max_count = 3
        cpu = 0.5
        memory_gb = 2
        storage_gb = 1
      }

      scheduler {
        cpu = 0.5
        memory_gb = 2
        storage_gb = 1
        count = 1
      }
      triggerer {
        count = 1
        cpu = 0.5
        memory_gb = 0.5
      }
      web_server {
        cpu = 0.5
        memory_gb = 1.875
        storage_gb = 1
      }
  }
  
}
}