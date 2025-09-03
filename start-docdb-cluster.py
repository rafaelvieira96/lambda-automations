import os
import json
import logging
from typing import Dict, List
import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variáveis de ambiente (iguais à função de STOP)
TAG_KEY = os.getenv("TAG_KEY", "work-on-business-time")
TAG_VALUE = os.getenv("TAG_VALUE", "true").lower()
REGIONS = [r.strip() for r in os.getenv("REGIONS", "").split(",") if r.strip()]
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

BOTO_CFG = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    user_agent_extra="docdb-start-by-tag-lambda"
)

def _regions_to_check() -> List[str]:
    if REGIONS:
        return REGIONS
    session = boto3.session.Session()
    return [session.region_name or "us-east-1"]

def _cluster_tags(docdb, arn: str) -> Dict[str, str]:
    resp = docdb.list_tags_for_resource(ResourceName=arn)
    return {t["Key"]: t.get("Value", "") for t in resp.get("TagList", [])}

def _should_start(tags: Dict[str, str]) -> bool:
    val = tags.get(TAG_KEY)
    return (val is not None) and (str(val).lower() == TAG_VALUE)

def _start_cluster(docdb, cluster_id: str):
    if DRY_RUN:
        logger.info(f"[DRY_RUN] start_db_cluster({cluster_id})")
        return
    docdb.start_db_cluster(DBClusterIdentifier=cluster_id)
    logger.info(f"Start iniciado para cluster: {cluster_id}")

def handler(event, context):
    results = []
    for region in _regions_to_check():
        docdb = boto3.client("docdb", region_name=region, config=BOTO_CFG)
        logger.info(f"Verificando DocumentDB clusters na região: {region}")

        paginator = docdb.get_paginator("describe_db_clusters")
        for page in paginator.paginate():
            for c in page.get("DBClusters", []):
                cluster_id = c["DBClusterIdentifier"]
                arn = c.get("DBClusterArn")
                status = c.get("Status", "").lower()

                if not arn:
                    logger.warning(f"Cluster {cluster_id} sem ARN — ignorando.")
                    continue

                tags = _cluster_tags(docdb, arn)
                eligible = _should_start(tags)

                logger.info(json.dumps({
                    "cluster": cluster_id,
                    "region": region,
                    "status": status,
                    "eligible_by_tag": eligible,
                    "tags": {k: tags[k] for k in tags if k.lower() == TAG_KEY.lower()}
                }))

                if not eligible:
                    continue

                if status != "stopped":
                    logger.info(f"Cluster {cluster_id} não está 'stopped' (status={status}). Ignorando.")
                    continue

                try:
                    _start_cluster(docdb, cluster_id)
                    results.append({"cluster": cluster_id, "region": region, "action": "started" if not DRY_RUN else "dry-run"})
                except docdb.exceptions.InvalidDBClusterStateFault:
                    logger.warning(f"Estado inválido para ligar cluster {cluster_id}.")
                except Exception as e:
                    logger.exception(f"Falha ao ligar cluster {cluster_id}: {e}")

    return {
        "started": results,
        "dry_run": DRY_RUN,
        "regions": _regions_to_check(),
        "tag_key": TAG_KEY,
        "tag_value": TAG_VALUE,
    }
