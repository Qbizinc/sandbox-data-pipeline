resource "aws_ecs_cluster" "demo_app_cluster" {
  name = var.demo_app_cluster_name
  tags = var.default_tags
}

resource "aws_default_vpc" "default_vpc" {}

resource "aws_default_subnet" "default_subnet_a" {
  availability_zone = var.availability_zones[0]
}

resource "aws_default_subnet" "default_subnet_b" {
  availability_zone = var.availability_zones[1]
}

resource "aws_default_subnet" "default_subnet_c" {
  availability_zone = var.availability_zones[2]
}

resource "aws_efs_file_system" "airflow_efs" {
  creation_token = "my-product"

  lifecycle_policy {
    transition_to_ia = "AFTER_30_DAYS"
  }
  tags = merge(var.default_tags, {
    Name = "AirflowEFS"
  })
}

resource "aws_efs_access_point" "airflow_efs_ap" {
  file_system_id = aws_efs_file_system.airflow_efs.id
  root_directory {
    path =  "/postgres"
    creation_info {
      owner_uid = 100
      owner_gid = 100
      permissions = 770
    }
  }
  tags = var.default_tags
}

data aws_ecs_task_definition andre_td {
  # task_definition = "arn:aws:ecs:us-west-2:907770664110:task-definition/AirflowAndres:3"
  task_definition = "AirflowAndres"
}

resource "aws_ecs_task_definition" "mount_task" {
  family                   = "MountEFSEBS"
  cpu = 8192
  memory = 16384
  container_definitions    = jsonencode(
  [
    {
      name = var.demo_app_task_name
      image = var.image_uri
      essential = true
      portMappings = [
        {
          containerPort = var.container_port
          hostPort = var.container_port
        }
      ]
      memory = 4096
      cpu =  1024
    },
    {
      name = "Postgres"
      image = "public.ecr.aws/docker/library/postgres:alpine3.19"
      essential = true
      portMappings = [
        {
          containerPort = 5432
          hostPort = 5432
        }
      ]
      memory = 8192
      cpu = 2048
    }
  ]
  )
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  volume {
    name = "service-storage"

    efs_volume_configuration {
      file_system_id          = aws_efs_file_system.airflow_efs.id
      # root_directory          = "/opt/data"
      transit_encryption      = "ENABLED"
      transit_encryption_port = 2999
      authorization_config {
        access_point_id = aws_efs_access_point.airflow_efs_ap.id
      }
    }
  }
}

resource "aws_iam_role" "ecs_task_execution_role" {
  name               = var.ecs_task_execution_role_name
  assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json
  tags               = var.default_tags
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_role_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}
resource "aws_iam_role_policy_attachment" "s3_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}
resource "aws_iam_role_policy_attachment" "secrets_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/SecretsManagerReadWrite"
}

resource "aws_iam_role_policy_attachment" "system_manager_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::907770664110:policy/get_ssm_parameters"
}


resource "aws_alb" "application_load_balancer" {
  name               = var.application_load_balancer_name
  load_balancer_type = "application"
  subnets = [
    "${aws_default_subnet.default_subnet_a.id}",
    "${aws_default_subnet.default_subnet_b.id}",
  ]
  security_groups = ["${aws_security_group.load_balancer_security_group.id}"]
  tags = var.default_tags
}

resource "aws_security_group" "load_balancer_security_group" {
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = var.default_tags
}

resource "aws_lb_target_group" "target_group" {
  name        = var.target_group_name
  port        = var.container_port
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_default_vpc.default_vpc.id
  target_health_state {
    enable_unhealthy_connection_termination = false
  }
  tags = var.default_tags
}

resource "aws_lb_listener" "listener" {
  load_balancer_arn = aws_alb.application_load_balancer.arn
  port              = "8080"
  protocol          = "HTTP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.target_group.arn
  }
  tags = var.default_tags
}

resource "aws_ecs_service" "demo_app_service" {
  name            = var.demo_app_service_name
  cluster         = aws_ecs_cluster.demo_app_cluster.id
  # task_definition = aws_ecs_task_definition.demo_app_task.arn
  task_definition = data.aws_ecs_task_definition.andre_td.arn
  launch_type     = "FARGATE"
  desired_count   = 1

  load_balancer {
    target_group_arn = aws_lb_target_group.target_group.arn
    # container_name   = aws_ecs_task_definition.demo_app_task.family
    container_name   = "AirflowDocker"
    container_port   = var.container_port
  }

  network_configuration {
    subnets          = [
      "${aws_default_subnet.default_subnet_a.id}",
      "${aws_default_subnet.default_subnet_b.id}",
    ]
    assign_public_ip = true
    security_groups  = ["${aws_security_group.service_security_group.id}"]
  }
  tags = var.default_tags
}

resource "aws_security_group" "service_security_group" {
  ingress {
    from_port       = 0
    to_port         = 0
    protocol        = "-1"
    security_groups = ["${aws_security_group.load_balancer_security_group.id}"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = var.default_tags
}

data "aws_route53_zone" "qbiz_wiki_com" {
  name = "qbiz-wiki.com"
}

resource "aws_route53_record" "airflow" {
  zone_id = data.aws_route53_zone.qbiz_wiki_com.zone_id
  name    = "airflow-ecs.qbiz-wiki.com"
  type    = "CNAME"
  ttl     = 300
  records = [aws_alb.application_load_balancer.dns_name]
}