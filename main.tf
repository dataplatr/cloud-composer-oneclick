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

    environment_size = local.environment_size

    software_config {
      image_version = "composer-2-airflow-2"
    }

    node_config {
      service_account = google_service_account.custom_service_account.email
    }

    workloads_config {
      worker {
        min_count  = local.worker_config.min_count
        max_count  = local.worker_config.max_count
        cpu        = local.worker_config.cpu
        memory_gb  = local.worker_config.memory_gb
        storage_gb = local.worker_config.storage_gb
      }

      scheduler {
        count      = local.scheduler_config.count
        cpu        = local.scheduler_config.cpu
        memory_gb  = local.scheduler_config.memory_gb
        storage_gb = local.scheduler_config.storage_gb
      }

      triggerer {
        count      = local.triggerer_config.count
        cpu        = local.triggerer_config.cpu
        memory_gb  = local.triggerer_config.memory_gb
      }

      web_server {
        cpu        = local.web_server_config.cpu
        memory_gb  = local.web_server_config.memory_gb
        storage_gb = local.web_server_config.storage_gb
      }
  }
  
}
}