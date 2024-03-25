locals {
  demo_app_cluster_name        = "demo-app-cluster"
  availability_zones           = ["us-west-2a", "us-west-2b", "us-west-2c"]
  demo_app_task_famliy         = "demo-app-task"
  container_port               = 8080
  demo_app_task_name           = "demo-app-task"
  ecs_task_execution_role_name = "demo-app-task-execution-role"

  application_load_balancer_name = "cc-demo-app-alb"
  target_group_name              = "cc-demo-alb-tg"

  demo_app_service_name = "cc-demo-app-service"
  image_uri             = "907770664110.dkr.ecr.us-west-2.amazonaws.com/airflowdeployement"
  default_tags = {
    Owner       = "Soren"
    Project     = "ECS-Fargate-Airflow"
    Environment = "Dev"
  }
}
