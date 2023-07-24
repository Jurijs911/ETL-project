resource "aws_secretsmanager_secret" "source_db" {
  name = "source_db"
}

variable "source_credentials" {
  default = {
    username = "project_user_2"
    password = "sFPX5AMNguGwU0iSh8rsUn45"
  }

  type = map(string)
}

resource "aws_secretsmanager_secret_version" "source_db_value" {
  secret_id     = aws_secretsmanager_secret.source_db.id
  secret_string = jsonencode(var.source_credentials)
}

resource "aws_secretsmanager_secret" "destination_db" {
  name = "destination_db"
}

variable "destination_credentials" {
  default = {
    username = "project_team_2"
    password = "wtMQ2eqap95gss2"
  }

  type = map(string)
}

resource "aws_secretsmanager_secret_version" "destination_db_value" {
  secret_id     = aws_secretsmanager_secret.destination_db.id
  secret_string = jsonencode(var.destination_credentials)
}
