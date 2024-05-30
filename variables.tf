variable "region" {
  description = "region to deploy resources"
  type        = string
  default     = "us-central1"
}

variable "project_id" {
  description = "Project ID"
  type        = string
}

variable "project_number" {
  description = "Project Number"
  type        = number
}

variable "composer_env_name" {
  description = "Cloud Composer Name"
  type        = string
}

variable "sa_name" {
  description = "Custom Service Account Name"
  type        = string
}