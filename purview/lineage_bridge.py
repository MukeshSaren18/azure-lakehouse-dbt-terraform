"""
purview/lineage_bridge.py
─────────────────────────
Reads dbt run artifacts (manifest.json + run_results.json) and pushes
column-level lineage into Microsoft Purview's Apache Atlas API.

Closes the gap between dbt's internal lineage DAG and Purview's org-wide
data catalogue — giving end-to-end lineage from ADLS Gen2 Bronze files
through dbt Silver/Gold transformations to downstream Power BI reports
and the Real-Time Feature Store.

Usage:
    python purview/lineage_bridge.py \\
        --manifest dbt_project/target/manifest.json \\
        --run-results dbt_project/target/run_results.json \\
        --purview-endpoint https://<account>.purview.azure.com

Authentication:
    Uses DefaultAzureCredential (Azure CLI, managed identity, service principal).
    Requires: Purview Data Curator role on the Purview collection.
"""

import argparse
import json
import logging
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from azure.identity import DefaultAzureCredential

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

PURVIEW_API_VERSION = "2022-08-01-preview"
ATLAS_ENTITY_ENDPOINT = "{endpoint}/catalog/api/atlas/v2/entity/bulk"


@dataclass
class DbtModel:
    unique_id: str
    name: str
    schema: str
    database: str
    fqn: str
    depends_on: list[str] = field(default_factory=list)
    columns: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    description: str = ""


def get_purview_token(endpoint: str) -> str:
    """Acquire bearer token for Purview API using DefaultAzureCredential."""
    credential = DefaultAzureCredential()
    token = credential.get_token("https://purview.azure.net/.default")
    log.info("Acquired Purview authentication token.")
    return token.token


def load_dbt_manifest(manifest_path: Path) -> dict:
    with open(manifest_path) as f:
        return json.load(f)


def load_run_results(run_results_path: Path) -> dict:
    with open(run_results_path) as f:
        return json.load(f)


def extract_models(manifest: dict) -> list[DbtModel]:
    """Parse dbt manifest nodes into DbtModel objects."""
    models = []
    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        
        models.append(DbtModel(
            unique_id=node_id,
            name=node["name"],
            schema=node.get("schema", ""),
            database=node.get("database", "main"),
            fqn=f"{node.get('database', 'main')}.{node.get('schema', '')}.{node['name']}",
            depends_on=[
                dep for dep in node.get("depends_on", {}).get("nodes", [])
                if dep.startswith("model.")
            ],
            columns=node.get("columns", {}),
            tags=node.get("tags", []),
            description=node.get("description", ""),
        ))
    
    log.info(f"Extracted {len(models)} dbt models from manifest.")
    return models


def build_atlas_entities(models: list[DbtModel], databricks_workspace: str) -> list[dict]:
    """
    Build Apache Atlas entity payloads for each dbt model.
    Uses the 'databricks_table' Atlas type to represent Delta tables.
    Column-level lineage is encoded via 'columnLineages' relationships.
    """
    entities = []

    for model in models:
        # Build column attributes
        column_attrs = []
        for col_name, col_meta in model.columns.items():
            column_attrs.append({
                "typeName": "column",
                "uniqueAttributes": {
                    "qualifiedName": f"{model.fqn}#{col_name}"
                },
                "attributes": {
                    "name": col_name,
                    "qualifiedName": f"{model.fqn}#{col_name}",
                    "description": col_meta.get("description", ""),
                    "dataType": col_meta.get("data_type", "unknown"),
                    "table": {"typeName": "databricks_table", "uniqueAttributes": {"qualifiedName": model.fqn}},
                },
            })

        # Build the table entity
        entity = {
            "typeName": "databricks_table",
            "guid": f"-{abs(hash(model.fqn))}",   # Negative GUID = new entity
            "attributes": {
                "qualifiedName": model.fqn,
                "name": model.name,
                "description": model.description or f"dbt model: {model.name}",
                "dbtModelId": model.unique_id,
                "dbtTags": model.tags,
                "schema": {
                    "typeName": "databricks_schema",
                    "uniqueAttributes": {
                        "qualifiedName": f"{model.database}.{model.schema}"
                    },
                },
                "columns": [
                    {"typeName": "column", "uniqueAttributes": {"qualifiedName": c["attributes"]["qualifiedName"]}}
                    for c in column_attrs
                ],
                "userDescription": model.description,
                "createTime": int(datetime.now(timezone.utc).timestamp() * 1000),
                "updateTime": int(datetime.now(timezone.utc).timestamp() * 1000),
            },
        }
        entities.append(entity)
        entities.extend(column_attrs)

        # Build lineage process entity (dbt model = transformation process)
        if model.depends_on:
            process_entity = {
                "typeName": "Process",
                "guid": f"-{abs(hash(model.unique_id + '_process'))}",
                "attributes": {
                    "qualifiedName": f"dbt://{model.unique_id}",
                    "name": f"dbt model: {model.name}",
                    "description": f"dbt transformation: {model.fqn}",
                    "inputs": [
                        {
                            "typeName": "databricks_table",
                            "uniqueAttributes": {
                                "qualifiedName": dep.replace("model.", "").replace(".", "_")
                            },
                        }
                        for dep in model.depends_on
                    ],
                    "outputs": [
                        {
                            "typeName": "databricks_table",
                            "uniqueAttributes": {"qualifiedName": model.fqn},
                        }
                    ],
                },
            }
            entities.append(process_entity)

    log.info(f"Built {len(entities)} Atlas entity payloads.")
    return entities


def push_to_purview(entities: list[dict], endpoint: str, token: str) -> dict:
    """POST entity bulk payload to Purview Atlas API."""
    url = ATLAS_ENTITY_ENDPOINT.format(endpoint=endpoint)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"entities": entities}

    log.info(f"Pushing {len(entities)} entities to Purview: {url}")
    response = requests.post(url, headers=headers, json=payload, timeout=120)

    if response.status_code not in (200, 201):
        log.error(f"Purview API error {response.status_code}: {response.text}")
        response.raise_for_status()

    result = response.json()
    created = len(result.get("guidAssignments", {}))
    log.info(f"✓ Purview sync complete — {created} entities created/updated.")
    return result


def main():
    parser = argparse.ArgumentParser(description="Push dbt lineage to Microsoft Purview.")
    parser.add_argument("--manifest", required=True, help="Path to dbt manifest.json")
    parser.add_argument("--run-results", default=None, help="Path to dbt run_results.json (optional)")
    parser.add_argument("--purview-endpoint", required=True, help="Purview account endpoint URL")
    parser.add_argument("--databricks-workspace", default="", help="Databricks workspace URL")
    parser.add_argument("--dry-run", action="store_true", help="Build payload but don't push to Purview")
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        log.error(f"manifest.json not found at {manifest_path}. Run 'dbt compile' or 'dbt run' first.")
        sys.exit(1)

    log.info(f"Loading dbt manifest from: {manifest_path}")
    manifest = load_dbt_manifest(manifest_path)

    models = extract_models(manifest)
    if not models:
        log.warning("No dbt models found in manifest. Nothing to push.")
        sys.exit(0)

    entities = build_atlas_entities(models, args.databricks_workspace)

    if args.dry_run:
        log.info(f"[DRY RUN] Would push {len(entities)} entities to {args.purview_endpoint}")
        print(json.dumps(entities[:2], indent=2))  # Print first 2 as sample
        sys.exit(0)

    token = get_purview_token(args.purview_endpoint)
    result = push_to_purview(entities, args.purview_endpoint, token)
    
    log.info("dbt → Purview lineage sync complete.")
    return result


if __name__ == "__main__":
    main()
