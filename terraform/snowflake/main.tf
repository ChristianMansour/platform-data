# ============================================
# WAREHOUSE (compute)
# ============================================
resource "snowflake_warehouse" "platform_wh" {
  name           = "PLATFORM_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "Warehouse principal de la plateforme"
}

# ============================================
# DATABASE
# ============================================
resource "snowflake_database" "platform_db" {
  name    = "PLATFORM_DB"
  comment = "Base de données principale"
}

# ============================================
# SCHEMAS (médaillon)
# ============================================
resource "snowflake_schema" "bronze" {
  database = snowflake_database.platform_db.name
  name     = "BRONZE"
  comment  = "Données brutes"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.platform_db.name
  name     = "SILVER"
  comment  = "Données nettoyées"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.platform_db.name
  name     = "GOLD"
  comment  = "Marts analytiques"
}

# ============================================
# ROLES (RBAC)
# ============================================
resource "snowflake_role" "engineer" {
  name    = "ROLE_ENGINEER"
  comment = "Data engineer : écriture et lecture sur tous les schémas"
}

resource "snowflake_role" "analyst" {
  name    = "ROLE_ANALYST"
  comment = "Analyste : lecture seule sur gold"
}

# ============================================
# GRANTS - Warehouse
# ============================================
resource "snowflake_grant_privileges_to_account_role" "engineer_wh" {
  account_role_name = snowflake_role.engineer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.platform_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_wh" {
  account_role_name = snowflake_role.analyst.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.platform_wh.name
  }
}

# ============================================
# GRANTS - Database
# ============================================
resource "snowflake_grant_privileges_to_account_role" "engineer_db" {
  account_role_name = snowflake_role.engineer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.platform_db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_db" {
  account_role_name = snowflake_role.analyst.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.platform_db.name
  }
}

# ============================================
# GRANTS - Engineer : ALL sur bronze, silver, gold
# ============================================
resource "snowflake_grant_privileges_to_account_role" "engineer_bronze" {
  account_role_name = snowflake_role.engineer.name
  privileges        = ["ALL PRIVILEGES"]
  on_schema {
    schema_name = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.bronze.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "engineer_silver" {
  account_role_name = snowflake_role.engineer.name
  privileges        = ["ALL PRIVILEGES"]
  on_schema {
    schema_name = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.silver.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "engineer_gold" {
  account_role_name = snowflake_role.engineer.name
  privileges        = ["ALL PRIVILEGES"]
  on_schema {
    schema_name = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.gold.name}\""
  }
}

# ============================================
# GRANTS - Analyst : USAGE + SELECT sur gold uniquement
# ============================================
resource "snowflake_grant_privileges_to_account_role" "analyst_gold_usage" {
  account_role_name = snowflake_role.analyst.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.gold.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_gold_select" {
  account_role_name = snowflake_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.gold.name}\""
    }
  }
}

# ============================================
# STAGES (lac de données)
# ============================================
resource "snowflake_stage" "raw_stage" {
  database = snowflake_database.platform_db.name
  schema   = snowflake_schema.bronze.name
  name     = "RAW_STAGE"
  comment  = "Lac de données - zone raw (fichiers bruts)"
}

resource "snowflake_stage" "refined_stage" {
  database = snowflake_database.platform_db.name
  schema   = snowflake_schema.bronze.name
  name     = "REFINED_STAGE"
  comment  = "Lac de données - zone refined (Parquet)"
}

# ============================================
# GRANTS - Stages pour Engineer
# ============================================
resource "snowflake_grant_privileges_to_account_role" "engineer_raw_stage" {
  account_role_name = snowflake_role.engineer.name
  privileges        = ["READ", "WRITE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.bronze.name}\".\"${snowflake_stage.raw_stage.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "engineer_refined_stage" {
  account_role_name = snowflake_role.engineer.name
  privileges        = ["READ", "WRITE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.bronze.name}\".\"${snowflake_stage.refined_stage.name}\""
  }
}
