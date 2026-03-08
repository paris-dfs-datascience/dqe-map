import csv
import io
import json
import os
import time
from typing import Dict, List
from google.cloud import storage
from battlecard_llm import BattleCardLLM
from battlecard_processor import BattleCardProcessor
from battlecard_storage import BattleCardStorage


class CSVBattleCardGenerator:
    """Generates battle cards from enriched CSV data for map visualization."""

    def __init__(self, gcs_bucket: str, project_id: str = "lma-website-461920"):
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        self.llm = BattleCardLLM(project_id)
        self.processor = BattleCardProcessor(self.llm, project_id)
        self.storage = BattleCardStorage(gcs_bucket)
        self.gcs_client = storage.Client()
        print(f"Initialized CSVBattleCardGenerator")

    def _read_csv_from_gcs(self, blob_path: str) -> List[Dict]:
        """Read a CSV file directly from GCS and return rows as list of dicts."""
        print(f"Reading CSV from gs://{self.gcs_bucket}/{blob_path}")
        bucket = self.gcs_client.bucket(self.gcs_bucket)
        blob = bucket.blob(blob_path)
        content = blob.download_as_text(encoding='utf-8')
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        print(f"✓ Loaded {len(rows)} rows from GCS")
        return rows

    def process_csv(self, csv_blob_path: str, max_workers: int = 10, max_rows: int = None) -> List[Dict]:
        print(f"\n=== Processing CSV: gs://{self.gcs_bucket}/{csv_blob_path} ===")
        print(f"Using {max_workers} parallel workers\n")

        rows = self._read_csv_from_gcs(csv_blob_path)

        # Apply global row limit before sharding
        if max_rows is not None:
            rows = rows[:max_rows]
            print(f"⚠ MAX_ROWS={max_rows}: limiting to first {len(rows)} rows globally")

        # --- Task slicing for Cloud Run parallel tasks ---
        task_index = int(os.environ.get('CLOUD_RUN_TASK_INDEX', 0))
        task_count = int(os.environ.get('CLOUD_RUN_TASK_COUNT', 1))

        chunk_size = len(rows) // task_count
        start = task_index * chunk_size
        end = start + chunk_size if task_index < task_count - 1 else len(rows)
        rows = rows[start:end]

        print(f"Task {task_index + 1}/{task_count}: processing rows {start}–{end} ({len(rows)} records)\n")

        battle_cards = self.processor.process_rows_parallel(rows, max_workers)

        print(f"\n=== Token Usage (Task {task_index}) ===")
        print(f"Total Input Tokens: {self.llm.total_input_tokens:,}")
        print(f"Total Output Tokens: {self.llm.total_output_tokens:,}")

        return battle_cards

    def save_to_gcs(self, battle_cards: List[Dict], output_name: str = "battle_cards") -> bool:
        """Save this task's results as a shard in GCS."""
        task_index = int(os.environ.get('CLOUD_RUN_TASK_INDEX', 0))
        shard_name = f"{output_name}_shard_{task_index}"
        return self.storage.save_to_gcs(
            battle_cards,
            shard_name,
            self.llm.total_input_tokens,
            self.llm.total_output_tokens
        )

    def save_to_local(self, battle_cards: List[Dict], output_file: str = "battle_cards_output.json") -> bool:
        return self.storage.save_to_local(battle_cards, output_file)


def merge_shards(gcs_bucket: str, output_name: str, task_count: int):
    """
    After all tasks complete, merge shards into a single output file.
    Run this manually or as a follow-up step after the job finishes.
    """
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    all_cards = []
    total_input = 0
    total_output = 0

    for i in range(task_count):
        blob_path = f"csv-battle-cards/{output_name}_shard_{i}.json"
        blob = bucket.blob(blob_path)
        if not blob.exists():
            print(f"⚠ Shard {i} not found at {blob_path}, skipping")
            continue
        data = json.loads(blob.download_as_text())
        all_cards.extend(data.get("battle_cards", []))
        usage = data.get("summary", {}).get("token_usage", {})
        total_input += usage.get("input_tokens", 0)
        total_output += usage.get("output_tokens", 0)
        print(f"✓ Loaded shard {i}: {len(data.get('battle_cards', []))} records")

    storage_handler = BattleCardStorage(gcs_bucket)
    storage_handler.save_to_gcs(all_cards, output_name, total_input, total_output)
    print(f"\n✓ Merged {len(all_cards)} total records → gs://{gcs_bucket}/csv-battle-cards/{output_name}.json")


def main():
    GCS_BUCKET   = "dqe-fiber-data"
    INPUT_CSV    = "enriched-data/tenants_enriched.csv"
    OUTPUT_NAME  = "dqe_prospects"
    MAX_WORKERS  = 3
    MAX_ROWS     = None  # Set to an int (e.g. 10) to limit rows during testing; None = process all

    generator = CSVBattleCardGenerator(gcs_bucket=GCS_BUCKET)
    battle_cards = generator.process_csv(INPUT_CSV, max_workers=MAX_WORKERS, max_rows=MAX_ROWS)
    generator.save_to_gcs(battle_cards, output_name=OUTPUT_NAME)


if __name__ == "__main__":
    main()