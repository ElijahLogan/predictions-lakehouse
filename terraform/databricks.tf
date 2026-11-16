

# Reads ~/.databrickscfg (or DATABRICKS_HOST / DATABRICKS_TOKEN).
provider "databricks" {}

locals {
  catalog = "predictions"   # created in the UI (Free Edition limit)
  schemas = {
    raw          = "Landing volume: prediction files and actuals exactly as received."
    bronze       = "Raw records loaded by Auto Loader, plus file lineage."
    silver       = "Cleaned, typed, deduplicated predictions and actuals (long format)."
    intermediate = "dbt working models."
    gold         = "Model evaluation marts. The only layer analysts can read."
  }
}

resource "databricks_schema" "layer" {
  for_each     = local.schemas
  catalog_name = local.catalog
  name         = each.key
  comment      = each.value
}

resource "databricks_volume" "landing" {
  catalog_name = local.catalog
  schema_name  = databricks_schema.layer["raw"].name
  name         = "landing"
  volume_type  = "MANAGED"
}

resource "databricks_grants" "catalog" {
  catalog = local.catalog
  grant {
    principal  = "account users"
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "gold" {
  schema = "${local.catalog}.${databricks_schema.layer["gold"].name}"
  grant {
    principal  = "account users"
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}