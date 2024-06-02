locals {
  environment_size = (var.composer_size == "large" ? "ENVIRONMENT_SIZE_LARGE" :
                     var.composer_size == "medium" ? "ENVIRONMENT_SIZE_MEDIUM" :
                     "ENVIRONMENT_SIZE_SMALL")

  worker_config = {
    min_count  = var.composer_size == "large" ? 3 : var.composer_size == "medium" ? 2 : 1
    max_count  = var.composer_size == "large" ? 9 : var.composer_size == "medium" ? 6 : 3
    cpu        = var.composer_size == "large" ? 2.0 : var.composer_size == "medium" ? 1.0 : 0.5
    memory_gb  = var.composer_size == "large" ? 8 : var.composer_size == "medium" ? 4 : 2
    storage_gb = var.composer_size == "large" ? 10 : var.composer_size == "medium" ? 5 : 1
  }

  scheduler_config = {
    count      = var.composer_size == "large" ? 3 : var.composer_size == "medium" ? 2 : 1
    cpu        = var.composer_size == "large" ? 2.0 : var.composer_size == "medium" ? 1.0 : 0.5
    memory_gb  = var.composer_size == "large" ? 8 : var.composer_size == "medium" ? 4 : 2
    storage_gb = var.composer_size == "large" ? 10 : var.composer_size == "medium" ? 5 : 1
  }

  triggerer_config = {
    count      = 1
    cpu        = var.composer_size == "large" ? 1.0 : var.composer_size == "medium" ? 0.75 : 0.5
    memory_gb  = var.composer_size == "large" ? 1.5 : var.composer_size == "medium" ? 1.0 : 0.5
  }

  web_server_config = {
    cpu        = var.composer_size == "large" ? 2.0 : var.composer_size == "medium" ? 1.0 : 0.5
    memory_gb  = var.composer_size == "large" ? 7.5 : var.composer_size == "medium" ? 3.75 : 1.875
    storage_gb = var.composer_size == "large" ? 10 : var.composer_size == "medium" ? 5 : 1
  }
}