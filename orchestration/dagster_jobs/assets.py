"""
Dagster Software-Defined Assets — Azure Lakehouse Pipeline
──────────────────────────────────────────────────────────
Asset graph: Bronze ingestion → dbt Silver → dbt Gold → Feature Store handoff

Run: dagster dev   (opens http://localhost:3000)
"""

from dagster import (
    asset,
    AssetIn,
    AssetExecutionContext,
    MetadataValue,
    Output,
    define_asset_job,
    ScheduleDefinition,
    Definitions,
    FreshnessPolicy,
)
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject
from pathlib import Path
import subprocess
import json
import os

# ── dbt Project Reference ─────────────────────────────────────────────────────
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_project"
DBT_PROFILES_DIR = Path.home() / ".dbt"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    packaged_project_dir=DBT_PROJECT_DIR,
)

# ── dbt Assets (auto-generated from dbt manifest) ────────────────────────────
@dbt_assets(manifest=dbt_project.manifest_path)
def lakehouse_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    All dbt models auto-registered as Dagster assets.
    Dagster reads dbt's manifest.json to infer the full asset graph
    including upstream/downstream dependencies between models.
    """
    yield from dbt.cli(["build"], context=context).stream()


# ── Bronze Ingestion Assets ───────────────────────────────────────────────────
@asset(
    group_name="bronze",
    description="Raw order events ingested from SAP BW via ADF + Auto Loader into ADLS Gen2.",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60),
    metadata={"layer": "bronze", "source": "SAP BW", "format": "Delta"},
)
def bronze_raw_orders(context: AssetExecutionContext):
    """
    Sentinel asset representing the Bronze raw_orders Delta table.
    In production this is written by ADF + Auto Loader pipelines;
    Dagster monitors freshness and raises alerts if the table goes stale.
    """
    # In production: check table metadata via Databricks REST API
    context.log.info("Checking bronze.raw_orders freshness via Unity Catalog...")
    context.log.info("Table populated by ADF pipeline — Auto Loader writes continuously.")
    
    row_count = _get_delta_table_row_count("main.bronze.raw_orders")
    
    yield Output(
        value={"table": "main.bronze.raw_orders", "row_count": row_count},
        metadata={
            "row_count": MetadataValue.int(row_count),
            "table_path": MetadataValue.text("abfss://bronze@<adls>.dfs.core.windows.net/raw_orders"),
            "format": MetadataValue.text("Delta"),
        }
    )


@asset(
    group_name="bronze",
    description="Raw customer master from CRM daily snapshot.",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=1440),  # 24hr — daily snapshot
    metadata={"layer": "bronze", "source": "CRM", "format": "Delta"},
)
def bronze_raw_customers(context: AssetExecutionContext):
    context.log.info("Checking bronze.raw_customers freshness...")
    row_count = _get_delta_table_row_count("main.bronze.raw_customers")

    yield Output(
        value={"table": "main.bronze.raw_customers", "row_count": row_count},
        metadata={"row_count": MetadataValue.int(row_count)},
    )


@asset(
    group_name="bronze",
    description="Raw payment transaction events — high volume, partitioned by event_date.",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=30),
    metadata={"layer": "bronze", "source": "Payment Gateway", "format": "Delta"},
)
def bronze_raw_transactions(context: AssetExecutionContext):
    context.log.info("Checking bronze.raw_transactions freshness...")
    row_count = _get_delta_table_row_count("main.bronze.raw_transactions")

    yield Output(
        value={"table": "main.bronze.raw_transactions", "row_count": row_count},
        metadata={"row_count": MetadataValue.int(row_count)},
    )


# ── Feature Store Handoff Asset ───────────────────────────────────────────────
@asset(
    group_name="feature_store",
    ins={"mart_customer_lifetime": AssetIn(key=["mart_customer_lifetime"])},
    description="""
    Publishes mart_customer_lifetime features to the Real-Time Feature Store
    (Kafka → Delta Lake pipeline). Triggers a Kafka producer job that writes
    updated customer features into the streaming feature store for real-time
    fraud detection and recommendation scoring.
    """,
    metadata={"downstream": "fraud_detection, recommendation_model"},
)
def feature_store_customer_features(
    context: AssetExecutionContext,
    mart_customer_lifetime,
):
    """
    Bridge asset between dbt Gold mart and the streaming feature store.
    After dbt materialises mart_customer_lifetime, this asset publishes
    changed features to Kafka for downstream ML model consumption.
    """
    context.log.info("Publishing updated customer features to Kafka feature store...")
    context.log.info("Feature freshness target: < 2 minutes end-to-end.")

    # In production: trigger the Kafka producer job via Databricks REST API
    # or directly call the FastAPI feature store endpoint
    features_published = _trigger_feature_store_publish(
        source_table="main.gold.mart_customer_lifetime",
        kafka_topic="feature-store.customer-features",
    )

    yield Output(
        value={"features_published": features_published},
        metadata={
            "features_published": MetadataValue.int(features_published),
            "kafka_topic": MetadataValue.text("feature-store.customer-features"),
            "freshness_sla": MetadataValue.text("< 2 minutes"),
        }
    )


# ── Purview Lineage Push Asset ────────────────────────────────────────────────
@asset(
    group_name="governance",
    ins={"mart_customer_lifetime": AssetIn(key=["mart_customer_lifetime"])},
    description="Pushes dbt run artifacts to Microsoft Purview Atlas API for org-wide lineage.",
)
def purview_lineage_sync(context: AssetExecutionContext, mart_customer_lifetime):
    """
    After every successful dbt run, push manifest.json + run_results.json
    to Purview's Atlas API to maintain cross-platform lineage visibility.
    """
    context.log.info("Syncing dbt lineage to Microsoft Purview...")
    
    result = subprocess.run(
        [
            "python", str(DBT_PROJECT_DIR.parent / "purview" / "lineage_bridge.py"),
            "--manifest", str(DBT_PROJECT_DIR / "target" / "manifest.json"),
            "--purview-endpoint", os.environ.get("PURVIEW_ENDPOINT", ""),
        ],
        capture_output=True, text=True
    )
    
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.warning(f"Purview sync warning: {result.stderr}")

    yield Output(
        value={"status": "synced"},
        metadata={"exit_code": MetadataValue.int(result.returncode)},
    )


# ── Helper Functions ──────────────────────────────────────────────────────────
def _get_delta_table_row_count(table_fqn: str) -> int:
    """Query Databricks SQL warehouse for table row count."""
    # In production: use databricks-sdk or DBSQL connector
    # from databricks.sdk import WorkspaceClient
    # w = WorkspaceClient()
    # result = w.statement_execution.execute_statement(
    #     warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
    #     statement=f"SELECT COUNT(*) FROM {table_fqn}"
    # )
    # return int(result.result.data_array[0][0])
    return -1  # Placeholder — replace with real SDK call


def _trigger_feature_store_publish(source_table: str, kafka_topic: str) -> int:
    """Trigger the Kafka feature store producer for updated rows."""
    # In production: call FastAPI feature store endpoint
    # import httpx
    # response = httpx.post(
    #     f"{os.environ['FEATURE_STORE_API']}/publish",
    #     json={"source_table": source_table, "topic": kafka_topic}
    # )
    # return response.json()["rows_published"]
    return -1  # Placeholder


# ── Job Definitions ───────────────────────────────────────────────────────────
daily_lakehouse_job = define_asset_job(
    name="daily_lakehouse_pipeline",
    selection=[
        "bronze_raw_orders",
        "bronze_raw_customers",
        "bronze_raw_transactions",
        "lakehouse_dbt_assets",
        "feature_store_customer_features",
        "purview_lineage_sync",
    ],
    description="Full daily run: Bronze freshness checks → dbt Silver/Gold → Feature Store → Purview sync.",
)

# ── Schedule ─────────────────────────────────────────────────────────────────
daily_schedule = ScheduleDefinition(
    job=daily_lakehouse_job,
    cron_schedule="0 3 * * *",   # 03:00 UTC daily
    execution_timezone="UTC",
)

# ── Dagster Definitions ───────────────────────────────────────────────────────
defs = Definitions(
    assets=[
        bronze_raw_orders,
        bronze_raw_customers,
        bronze_raw_transactions,
        lakehouse_dbt_assets,
        feature_store_customer_features,
        purview_lineage_sync,
    ],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROFILES_DIR),
        ),
    },
    jobs=[daily_lakehouse_job],
    schedules=[daily_schedule],
)
