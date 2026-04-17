"""
battlecard_generator.py
Orchestrates the full 4-step pipeline as a Cloud Run Job.
Steps: tenant enrichment -> HubSpot refresh -> battle card generation -> shard merge
"""

import csv
import io
import json
import os
import time
import logging
from typing import Dict, List

from google.cloud import storage
import google.cloud.logging

from pipeline_config import (
    PROJECT_ID, GCS_BUCKET, ENRICHED_CSV_BLOB, DEFAULT_OUTPUT_NAME,
    MAX_WORKERS, MAX_ROWS, CLOUD_RUN_TASK_INDEX, CLOUD_RUN_TASK_COUNT,
    CHECKPOINT_BLOB,
)
from battlecard_llm import BattleCardLLM
from battlecard_processor import BattleCardProcessor
from battlecard_storage import BattleCardStorage

# ── Cloud Logging ─────────────────────────────────────────────────────────────
log_client = google.cloud.logging.Client()
log_client.setup_logging()
logger = logging.getLogger("battlecard-generator")


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def _read_checkpoint(gcs_client: storage.Client, gcs_bucket: str) -> dict:
    """Read pipeline checkpoint from GCS. Returns empty dict if none exists."""
    try:
        bucket = gcs_client.bucket(gcs_bucket)
        blob = bucket.blob(CHECKPOINT_BLOB)
        if blob.exists():
            return json.loads(blob.download_as_text())
    except Exception as e:
        logger.warning(f"Could not read checkpoint: {e}")
    return {}


def _write_checkpoint(gcs_client: storage.Client, gcs_bucket: str, checkpoint: dict):
    """Write pipeline checkpoint to GCS with generation-based conditional write to prevent races."""
    try:
        bucket = gcs_client.bucket(gcs_bucket)
        blob = bucket.blob(CHECKPOINT_BLOB)

        # Use if_generation_match to prevent concurrent overwrites.
        # First read current generation; 0 means "only if blob doesn't exist".
        blob.reload()
        generation = blob.generation
        blob.upload_from_string(
            json.dumps(checkpoint, indent=2),
            content_type="application/json",
            if_generation_match=generation
        )
    except Exception as e:
        # If blob doesn't exist yet, write unconditionally
        if "404" in str(e) or "Not Found" in str(e) or "generation" in str(e).lower():
            try:
                bucket = gcs_client.bucket(gcs_bucket)
                blob = bucket.blob(CHECKPOINT_BLOB)
                blob.upload_from_string(
                    json.dumps(checkpoint, indent=2),
                    content_type="application/json"
                )
            except Exception as inner_e:
                logger.error(f"Failed to write checkpoint: {inner_e}")
        else:
            logger.error(f"Checkpoint write conflict or error: {e}")


class CSVBattleCardGenerator:
    """Generates battle cards from enriched CSV data for map visualization."""

    def __init__(self, gcs_bucket: str = GCS_BUCKET, project_id: str = PROJECT_ID):
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        self.llm        = BattleCardLLM(project_id)
        self.processor  = BattleCardProcessor(self.llm, project_id)
        self.storage    = BattleCardStorage(gcs_bucket)
        self.gcs_client = storage.Client()
        logger.info("Initialized CSVBattleCardGenerator")

    def _read_csv_from_gcs(self, blob_path: str) -> List[Dict]:
        logger.info(f"Reading CSV from gs://{self.gcs_bucket}/{blob_path}")
        bucket  = self.gcs_client.bucket(self.gcs_bucket)
        blob    = bucket.blob(blob_path)
        content = blob.download_as_text(encoding="utf-8")
        rows    = list(csv.DictReader(io.StringIO(content)))
        logger.info(f"Loaded {len(rows)} rows from GCS")
        return rows

    def process_csv(self, csv_blob_path: str,
                    max_workers: int = MAX_WORKERS,
                    max_rows: int = None) -> List[Dict]:

        logger.info(f"=== Processing CSV: gs://{self.gcs_bucket}/{csv_blob_path} ===")

        rows = self._read_csv_from_gcs(csv_blob_path)

        if max_rows:
            rows = rows[:max_rows]
            logger.info(f"MAX_ROWS={max_rows}: limited to first {len(rows)} rows")

        # Cloud Run task sharding
        task_index = CLOUD_RUN_TASK_INDEX
        task_count = CLOUD_RUN_TASK_COUNT
        chunk_size = len(rows) // task_count
        start      = task_index * chunk_size
        end        = start + chunk_size if task_index < task_count - 1 else len(rows)
        rows       = rows[start:end]

        logger.info(f"Task {task_index + 1}/{task_count}: rows {start}-{end} ({len(rows)} records)")

        # Pass global start offset so row indices are unique across shards
        battle_cards = self.processor.process_rows_parallel(rows, max_workers, row_offset=start)

        logger.info(f"Token usage - input: {self.llm.total_input_tokens:,} / output: {self.llm.total_output_tokens:,}")
        return battle_cards

    def save_to_gcs(self, battle_cards: List[Dict],
                    output_name: str = DEFAULT_OUTPUT_NAME) -> bool:
        task_index = CLOUD_RUN_TASK_INDEX
        shard_name = f"{output_name}_shard_{task_index}"
        return self.storage.save_to_gcs(
            battle_cards,
            shard_name,
            self.llm.total_input_tokens,
            self.llm.total_output_tokens
        )


# ── Pipeline entry point ──────────────────────────────────────────────────────

def main():
    task_index = CLOUD_RUN_TASK_INDEX
    task_count = CLOUD_RUN_TASK_COUNT
    gcs_client = storage.Client()

    # Read existing checkpoint
    checkpoint = _read_checkpoint(gcs_client, GCS_BUCKET)

    # ── Step 1: Tenant enrichment (task 0 only) ───────────────────────────────
    if task_index == 0:
        if checkpoint.get("enrichment_complete"):
            logger.info("=== Step 1: Tenant Enrichment (skipped - checkpoint) ===")
        else:
            logger.info("\n=== Step 1: Tenant Enrichment ===")
            try:
                from tenant_enrichment import process_csv
                process_csv()
                logger.info("Tenant enrichment complete")
                checkpoint["enrichment_complete"] = True
                checkpoint["enrichment_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S')
                _write_checkpoint(gcs_client, GCS_BUCKET, checkpoint)
            except Exception as e:
                logger.error(f"Tenant enrichment FAILED: {e}")
                # Check if enriched CSV already exists from a prior run
                bucket = gcs_client.bucket(GCS_BUCKET)
                if not bucket.blob(ENRICHED_CSV_BLOB).exists():
                    raise RuntimeError(f"Tenant enrichment failed and no prior enriched CSV exists: {e}")
                logger.warning("Using existing enriched CSV from a prior run")

        # ── Step 2: HubSpot refresh (task 0 only) ─────────────────────────────
        if checkpoint.get("hubspot_complete"):
            logger.info("=== Step 2: HubSpot Refresh (skipped - checkpoint) ===")
        else:
            logger.info("\n=== Step 2: HubSpot Refresh ===")
            try:
                from hubspot_update import refresh_hubspot_companies
                refresh_hubspot_companies()
                logger.info("HubSpot refresh complete")
                checkpoint["hubspot_complete"] = True
                checkpoint["hubspot_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S')
                _write_checkpoint(gcs_client, GCS_BUCKET, checkpoint)
            except Exception as e:
                logger.error(f"HubSpot refresh FAILED: {e}")
                # Check if HubSpot data already exists from a prior run
                from pipeline_config import HUBSPOT_BLOB
                bucket = gcs_client.bucket(GCS_BUCKET)
                if not bucket.blob(HUBSPOT_BLOB).exists():
                    raise RuntimeError(f"HubSpot refresh failed and no prior data exists: {e}")
                logger.warning("Using existing HubSpot data from a prior run")

    # ── Step 3: Battle card generation (all tasks) ────────────────────────────
    logger.info(f"\n=== Step 3: Battle Card Generation (task {task_index + 1}/{task_count}) ===")
    generator    = CSVBattleCardGenerator(gcs_bucket=GCS_BUCKET, project_id=PROJECT_ID)
    battle_cards = generator.process_csv(ENRICHED_CSV_BLOB, max_workers=MAX_WORKERS, max_rows=MAX_ROWS)
    generator.save_to_gcs(battle_cards, output_name=DEFAULT_OUTPUT_NAME)

    # ── Step 4: Merge shards (last task only) ─────────────────────────────────
    if task_index == task_count - 1:
        logger.info("\n=== Step 4: Merging Shards ===")
        try:
            from merge_shards import merge_all_shards
            success = merge_all_shards(GCS_BUCKET, DEFAULT_OUTPUT_NAME)
            if success:
                logger.info("Shard merge complete")
                checkpoint["pipeline_complete"] = True
                checkpoint["pipeline_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S')
                _write_checkpoint(gcs_client, GCS_BUCKET, checkpoint)
            else:
                logger.error("Shard merge returned failure — no shards found or merge aborted")
        except Exception as e:
            logger.error(f"Shard merge FAILED: {e}")
            raise


def _write_run_report(gcs_client: storage.Client, gcs_bucket: str,
                      task_index: int, task_count: int,
                      battle_cards: list, checkpoint: dict,
                      llm: 'BattleCardLLM', start_time: float,
                      step_errors: list):
    """Write a plain-text run report to EY-file/last_run.txt after every run."""
    try:
        elapsed = time.time() - start_time
        elapsed_min = elapsed / 60

        total = len(battle_cards)
        scores = [c.get("llm_analysis", {}).get("overall_score", 0) for c in battle_cards]
        nonzero = [s for s in scores if s > 0]
        zero_count = total - len(nonzero)
        avg_score = sum(nonzero) / len(nonzero) if nonzero else 0

        geocode_ok = sum(1 for c in battle_cards if c.get("geocode_data", {}).get("geocode_status") == "success")
        geocode_fail = total - geocode_ok

        hubspot_matched = sum(1 for c in battle_cards if c.get("hubspot_match", {}).get("matched"))
        netsuite_matched = sum(1 for c in battle_cards if c.get("netsuite_match", {}).get("matched"))

        error_cards = [c for c in battle_cards if c.get("metadata", {}).get("error")]
        llm_errors = [c for c in battle_cards if c.get("llm_analysis", {}).get("_error")]

        # Score distribution
        excellent = sum(1 for s in scores if s >= 80)
        good = sum(1 for s in scores if 60 <= s < 80)
        fair = sum(1 for s in scores if 40 <= s < 60)
        poor = sum(1 for s in scores if 0 < s < 40)

        lines = [
            "=" * 60,
            "DQE PIPELINE RUN REPORT",
            "=" * 60,
            f"Timestamp:      {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Task:           {task_index + 1} of {task_count}",
            f"Duration:       {elapsed_min:.1f} minutes ({elapsed:.0f}s)",
            f"",
            "--- RESULTS ---",
            f"Total rows processed:    {total}",
            f"Will appear on map:      {len(nonzero)}",
            f"Scored zero (hidden):    {zero_count}",
            f"Average score (non-zero):{avg_score:.1f}",
            f"",
            "--- SCORE DISTRIBUTION ---",
            f"Excellent (80-100):      {excellent}",
            f"Good (60-79):            {good}",
            f"Fair (40-59):            {fair}",
            f"Poor (1-39):             {poor}",
            f"Zero / failed:           {zero_count}",
            f"",
            "--- GEOCODING ---",
            f"Geocoded successfully:   {geocode_ok}",
            f"Geocode failed:          {geocode_fail}",
            f"",
            "--- MATCHING ---",
            f"HubSpot matched:         {hubspot_matched}/{total}",
            f"NetSuite matched:        {netsuite_matched}/{total}",
            f"",
            "--- ERRORS ---",
            f"Row processing errors:   {len(error_cards)}",
            f"LLM fallback (score 0):  {len(llm_errors)}",
        ]

        if step_errors:
            lines.append(f"")
            lines.append("--- STEP ERRORS ---")
            for err in step_errors:
                lines.append(f"  {err}")

        if error_cards:
            lines.append(f"")
            lines.append("--- FAILED ROWS (first 20) ---")
            for c in error_cards[:20]:
                name = c.get("ey_file_data", {}).get("Name", "Unknown")
                err = c.get("metadata", {}).get("error", "unknown")
                lines.append(f"  {name}: {err}")

        if llm_errors:
            lines.append(f"")
            lines.append("--- LLM ERRORS (first 20) ---")
            for c in llm_errors[:20]:
                name = c.get("ey_file_data", {}).get("Name", "Unknown")
                err = c.get("llm_analysis", {}).get("_error", "unknown")
                lines.append(f"  {name}: {err}")

        lines.append(f"")
        lines.append("--- TOKENS ---")
        lines.append(f"Input tokens:            {llm.total_input_tokens:,}")
        lines.append(f"Output tokens:           {llm.total_output_tokens:,}")
        lines.append(f"Total tokens:            {llm.total_input_tokens + llm.total_output_tokens:,}")

        lines.append(f"")
        lines.append("--- CHECKPOINT ---")
        for k, v in checkpoint.items():
            lines.append(f"  {k}: {v}")

        lines.append(f"")
        lines.append("=" * 60)

        report = "\n".join(lines)

        bucket = gcs_client.bucket(gcs_bucket)
        blob = bucket.blob("EY-file/last_run.txt")
        blob.upload_from_string(report, content_type="text/plain")
        logger.info(f"Run report saved to gs://{gcs_bucket}/EY-file/last_run.txt")
        logger.info(f"FINAL: {len(nonzero)}/{total} cards on map, {zero_count} hidden (score 0), {len(error_cards)} errors")

    except Exception as e:
        logger.error(f"Failed to write run report: {e}")


def main():
    pipeline_start = time.time()
    task_index = CLOUD_RUN_TASK_INDEX
    task_count = CLOUD_RUN_TASK_COUNT
    gcs_client = storage.Client()
    step_errors = []

    # Read existing checkpoint
    checkpoint = _read_checkpoint(gcs_client, GCS_BUCKET)

    # ── Step 1: Tenant enrichment (task 0 only) ───────────────────────────────
    if task_index == 0:
        if checkpoint.get("enrichment_complete"):
            logger.info("=== Step 1: Tenant Enrichment (skipped - checkpoint) ===")
        else:
            logger.info("\n=== Step 1: Tenant Enrichment ===")
            try:
                from tenant_enrichment import process_csv
                process_csv()
                logger.info("Tenant enrichment complete")
                checkpoint["enrichment_complete"] = True
                checkpoint["enrichment_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S')
                _write_checkpoint(gcs_client, GCS_BUCKET, checkpoint)
            except Exception as e:
                logger.error(f"Tenant enrichment FAILED: {e}")
                step_errors.append(f"Step 1 (Enrichment): {e}")
                # Check if enriched CSV already exists from a prior run
                bucket = gcs_client.bucket(GCS_BUCKET)
                if not bucket.blob(ENRICHED_CSV_BLOB).exists():
                    raise RuntimeError(f"Tenant enrichment failed and no prior enriched CSV exists: {e}")
                logger.warning("Using existing enriched CSV from a prior run")

        # ── Step 2: HubSpot refresh (task 0 only) ─────────────────────────────
        if checkpoint.get("hubspot_complete"):
            logger.info("=== Step 2: HubSpot Refresh (skipped - checkpoint) ===")
        else:
            logger.info("\n=== Step 2: HubSpot Refresh ===")
            try:
                from hubspot_update import refresh_hubspot_companies
                refresh_hubspot_companies()
                logger.info("HubSpot refresh complete")
                checkpoint["hubspot_complete"] = True
                checkpoint["hubspot_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S')
                _write_checkpoint(gcs_client, GCS_BUCKET, checkpoint)
            except Exception as e:
                logger.error(f"HubSpot refresh FAILED: {e}")
                step_errors.append(f"Step 2 (HubSpot): {e}")
                # Check if HubSpot data already exists from a prior run
                from pipeline_config import HUBSPOT_BLOB
                bucket = gcs_client.bucket(GCS_BUCKET)
                if not bucket.blob(HUBSPOT_BLOB).exists():
                    raise RuntimeError(f"HubSpot refresh failed and no prior data exists: {e}")
                logger.warning("Using existing HubSpot data from a prior run")

    # ── Step 3: Battle card generation (all tasks) ────────────────────────────
    logger.info(f"\n=== Step 3: Battle Card Generation (task {task_index + 1}/{task_count}) ===")
    generator    = CSVBattleCardGenerator(gcs_bucket=GCS_BUCKET, project_id=PROJECT_ID)
    battle_cards = generator.process_csv(ENRICHED_CSV_BLOB, max_workers=MAX_WORKERS, max_rows=MAX_ROWS)
    generator.save_to_gcs(battle_cards, output_name=DEFAULT_OUTPUT_NAME)

    # ── Step 4: Merge shards (last task only) ─────────────────────────────────
    if task_index == task_count - 1:
        logger.info("\n=== Step 4: Waiting for all shards before merge ===")

        # Poll GCS until all shards are present or we time out
        from pipeline_config import BATTLECARD_OUTPUT_PREFIX
        shard_prefix = f"{BATTLECARD_OUTPUT_PREFIX}/{DEFAULT_OUTPUT_NAME}_shard_"
        max_wait = 3600  # 60 minutes max
        poll_interval = 30  # check every 30 seconds
        waited = 0

        while waited < max_wait:
            bucket = gcs_client.bucket(GCS_BUCKET)
            found_shards = list(bucket.list_blobs(prefix=shard_prefix))
            found_indices = set()
            for blob in found_shards:
                name = blob.name.split("/")[-1]
                try:
                    idx_str = name.rsplit("_shard_", 1)[1].replace(".json", "")
                    found_indices.add(int(idx_str))
                except (IndexError, ValueError):
                    continue

            expected_indices = set(range(task_count))
            missing = sorted(expected_indices - found_indices)

            if not missing:
                logger.info(f"All {task_count} shards found. Proceeding with merge.")
                break

            logger.info(f"Waiting for shards: have {len(found_indices)}/{task_count} "
                        f"(missing: {missing}). Waited {waited}s/{max_wait}s...")
            time.sleep(poll_interval)
            waited += poll_interval

        if missing:
            logger.warning(f"Timed out waiting for shards after {max_wait}s. "
                           f"Missing: {missing}. Merging with what's available.")

        logger.info("\n=== Step 4: Merging Shards ===")
        try:
            from merge_shards import merge_all_shards
            success = merge_all_shards(GCS_BUCKET, DEFAULT_OUTPUT_NAME)
            if success:
                logger.info("Shard merge complete")
                checkpoint["pipeline_complete"] = True
                checkpoint["pipeline_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S')
                _write_checkpoint(gcs_client, GCS_BUCKET, checkpoint)
            else:
                logger.error("Shard merge returned failure — no shards found or merge aborted")
                step_errors.append("Step 4 (Merge): merge returned failure")
        except Exception as e:
            logger.error(f"Shard merge FAILED: {e}")
            step_errors.append(f"Step 4 (Merge): {e}")
            raise

    # ── Always write run report ───────────────────────────────────────────────
    _write_run_report(gcs_client, GCS_BUCKET, task_index, task_count,
                      battle_cards, checkpoint, generator.llm, pipeline_start,
                      step_errors)


if __name__ == "__main__":
    main()
