import os
import json
import logging
from typing import Dict, List
import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configurações via variáveis de ambiente
TAG_KEY = os.getenv("TAG_KEY", "work-on-business-time")
TAG_VALUE = os.getenv("TAG_VALUE", "true").lower()
# Comma-separated list, ex: "sa-east-1,us-east-1" (opcional)
REGIONS = [r.strip() for r in os.getenv("REGIONS", "").split(",") if r.strip()]
# dry-run para apenas logar sem parar (true/false)
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

BOTO_CFG = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    user_agent_extra="docdb-stop-by-tag-lambda"
)

def _regions_to_check() -> List[str]:
    if REGIONS:
        return REGIONS
    # fallback: região da função
    session = boto3.session.Session()
    return [session.region_name or "us-east-1"]

def _cluster_tags(docdb, arn: str) -> Dict[str, str]:
    # list_tags_for_resource usa o ARN do cluster
    resp = docdb.list_tags_for_resource(ResourceName=arn)
    return {t["Key"]: t.get("Value", "") for t in resp.get("TagList", [])}

def _should_stop(tags: Dict[str, str]) -> bool:
    val = tags.get(TAG_KEY)
    return (val is not None) and (str(val).lower() == TAG_VALUE)

def _stop_cluster(docdb, cluster_id: str):
    if DRY_RUN:
        logger.info(f"[DRY_RUN] stop_db_cluster({cluster_id})")
        return
    docdb.stop_db_cluster(DBClusterIdentifier=cluster_id)
    logger.info(f"Stop iniciado para cluster: {cluster_id}")

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

                # Sem ARN não dá para buscar tags
                if not arn:
                    logger.warning(f"Cluster {cluster_id} sem ARN — ignorando.")
                    continue

                tags = _cluster_tags(docdb, arn)
                eligible = _should_stop(tags)

                logger.info(json.dumps({
                    "cluster": cluster_id,
                    "region": region,
                    "status": status,
                    "eligible_by_tag": eligible,
                    "tags": {k: tags[k] for k in tags if k.lower() == TAG_KEY.lower()}
                }))

                if not eligible:
                    continue

                if status != "available":
                    logger.info(f"Cluster {cluster_id} não está 'available' (status={status}). Ignorando.")
                    continue

                try:
                    _stop_cluster(docdb, cluster_id)
                    results.append({"cluster": cluster_id, "region": region, "action": "stopped" if not DRY_RUN else "dry-run"})
                except docdb.exceptions.InvalidDBClusterStateFault:
                    logger.warning(f"Estado inválido para parar cluster {cluster_id}.")
                except Exception as e:
                    logger.exception(f"Falha ao parar cluster {cluster_id}: {e}")

    return {
        "stopped": results,
        "dry_run": DRY_RUN,
        "regions": _regions_to_check(),
        "tag_key": TAG_KEY,
        "tag_value": TAG_VALUE,
    }
