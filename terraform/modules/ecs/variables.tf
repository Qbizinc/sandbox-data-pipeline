variable "demo_app_cluster_name" {
  description = "ECS Cluster Name"
  type        = string
}

variable "availability_zones" {
  description = "us-west-2 AZs"
  type        = list(string)
}

variable "demo_app_task_famliy" {
  description = "ECS Task Family"
  type        = string
}

variable "container_port" {
  description = "Container Port"
  type        = number
}

variable "demo_app_task_name" {
  description = "ECS Task Name"
  type        = string
}

variable "ecs_task_execution_role_name" {
  description = "ECS Task Execution Role Name"
  type        = string
}

variable "application_load_balancer_name" {
  description = "ALB Name"
  type        = string
}

variable "target_group_name" {
  description = "ALB Target Group Name"
  type        = string
}

variable "demo_app_service_name" {
  description = "ECS Service Name"
  type        = string
}

variable "image_uri" {
  description = "ECR Image URI"
  type        = string
}

variable "default_tags" {
  description = "Default Tags"
  type        = map(string)
  default     = {
    Owner       = "Soren"
    Project     = "ECS-Fargate-Airflow"
    Environment = "Dev"
  }
}