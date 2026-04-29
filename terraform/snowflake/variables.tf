variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username"
  type        = string
  default     = "svc_terraform"
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}
