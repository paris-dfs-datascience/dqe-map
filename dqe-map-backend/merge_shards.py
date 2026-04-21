"""
merge_shards.py
Merges per-task battle card shards into a single dqe_prospects.json.
Step 4 of the pipeline -- runs on the last Cloud Run task after all shards are written.
Can also be run standalone from the command line.
"""

import json
import os
import logging
from google.cloud import storage
import google.cloud.logging

from pipeline_config import (
    GCS_BUCKET, DEFAULT_OUTPUT_NAME, BATTLECARD_OUTPUT_PREFIX,
    CLOUD_RUN_TASK_COUNT, gcs_output_path,
)

# ── Cloud Logging ─────────────────────────────────────────────────────────────
log_client = google.cloud.logging.Client()
log_client.setup_logging()
logger = logging.getLogger("merge-shards")


def _verify_shard_completeness(shards: list, expected_count: int) -> list:
    """
    Verify all expected shards are present. Returns list of missing shard indices.
    """
    found_indices = set()
    for blob in shards:
        # Extract shard index from name like dqe_prospects_shard_0.json
        name = blob.name.split("/")[-1]
        try:
            idx_str = name.rsplit("_shard_", 1)[1].replace(".json", "")
            found_indices.add(int(idx_str))
        except (IndexError, ValueError):
            continue

    expected_indices = set(range(expected_count))
    missing = sorted(expected_indices - found_indices)
    return missing


def merge_all_shards(gcs_bucket: str = GCS_BUCKET,
                     output_name: str = DEFAULT_OUTPUT_NAME) -> bool:
    """
    Auto-discover all shards matching csv-battle-cards/<output_name>_shard_*.json
    and merge them into csv-battle-cards/<output_name>.json.
    Verifies all expected shards are present before merging.
    Returns True on success.
    """
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)

    prefix = f"{BATTLECARD_OUTPUT_PREFIX}/{output_name}_shard_"
    shards = sorted(
        bucket.list_blobs(prefix=prefix),
        key=lambda b: b.name
    )

    if not shards:
        logger.warning(f"No shards found with prefix: {prefix}")
        return False

    # Verify completeness — require every expected shard before merging and deleting.
    expected_count = CLOUD_RUN_TASK_COUNT
    missing = _verify_shard_completeness(shards, expected_count)
    available_ratio = (expected_count - len(missing)) / expected_count if expected_count > 0 else 0

    if missing:
        logger.error(
            f"Only {expected_count - len(missing)}/{expected_count} shards available "
            f"({available_ratio:.0%}). Missing indices: {missing}. "
            f"Aborting merge so partial shards remain available for re-run."
        )
        return False

    all_cards = []
    shard_summaries = []
    for blob in shards:
        try:
            data = json.loads(blob.download_as_text())
        except json.JSONDecodeError as e:
            logger.error(f"Shard {blob.name} contains invalid JSON: {e}. Skipping.")
            continue

        cards = data.get("battle_cards", [])
        if not isinstance(cards, list):
            logger.error(f"Shard {blob.name} has invalid battle_cards field. Skipping.")
            continue

        all_cards.extend(cards)
        if data.get("summary"):
            shard_summaries.append(data["summary"])
        logger.info(f"{blob.name.split('/')[-1]}: {len(cards)} records")

    # Build merged summary
    merged_summary = {
        "total_records": len(all_cards),
        "shards_merged": len(shards),
        "shards_expected": expected_count,
        "shards_missing": missing,
        "complete": len(missing) == 0,
        "shard_completeness": f"{available_ratio:.0%}",
    }

    output_blob = bucket.blob(gcs_output_path(output_name))
    output_blob.upload_from_string(
        json.dumps({
            "merge_summary": merged_summary,
            "battle_cards": all_cards,
        }, indent=2),
        content_type="application/json"
    )
    logger.info(
        f"Merged {len(all_cards)} total records from {len(shards)}/{expected_count} shards "
        f"-> gs://{gcs_bucket}/{gcs_output_path(output_name)}"
    )

    # Clean up shard files to prevent stale data on next run
    for blob in shards:
        try:
            blob.delete()
            logger.info(f"Deleted shard: {blob.name}")
        except Exception as e:
            logger.warning(f"Could not delete shard {blob.name}: {e}")

    return True


if __name__ == "__main__":
    merge_all_shards()
