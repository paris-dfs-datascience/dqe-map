import csv
import json
import time
from typing import Dict, List
from google.cloud import storage
from google import genai
from google.genai import types
import concurrent.futures
import threading
from battlecard_llm import BattleCardLLM
from battlecard_processor import BattleCardProcessor
from battlecard_storage import BattleCardStorage


class CSVBattleCardGenerator:
    """Generates battle cards from enriched CSV data for map visualization."""
    
    def __init__(self, gcs_bucket: str, project_id: str = "lma-website-461920"):
        """
        Initialize the CSV battle card generator.
        
        Args:
            gcs_bucket: GCS bucket name for storing results
            project_id: GCP project ID
        """
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        
        # Initialize components
        self.llm = BattleCardLLM(project_id)
        self.processor = BattleCardProcessor(self.llm, project_id)  # Pass project_id for geocoding
        self.storage = BattleCardStorage(gcs_bucket)
        
        print(f"Initialized CSVBattleCardGenerator")
    
    def process_csv(self, csv_file: str, max_workers: int = 10) -> List[Dict]:
        """
        Process enriched CSV and generate battle cards with parallel execution.
        
        Args:
            csv_file: Path to input CSV
            max_workers: Number of parallel workers (default 10)
        
        Returns list of battle cards
        """
        print(f"\n=== Processing CSV: {csv_file} ===")
        print(f"Using {max_workers} parallel workers\n")
        
        # Read CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        print(f"Found {len(rows)} records to process\n")
        
        # Process in parallel
        battle_cards = self.processor.process_rows_parallel(rows, max_workers)
        
        # Print summary
        print(f"\n=== Token Usage ===")
        print(f"Total Input Tokens: {self.llm.total_input_tokens:,}")
        print(f"Total Output Tokens: {self.llm.total_output_tokens:,}")
        print(f"Total Tokens: {self.llm.total_input_tokens + self.llm.total_output_tokens:,}")
        
        return battle_cards
    
    def save_to_gcs(self, battle_cards: List[Dict], output_name: str = "battle_cards") -> bool:
        """Save battle cards to GCS for map visualization."""
        return self.storage.save_to_gcs(
            battle_cards, 
            output_name,
            self.llm.total_input_tokens,
            self.llm.total_output_tokens
        )
    
    def save_to_local(self, battle_cards: List[Dict], output_file: str = "battle_cards_output.json") -> bool:
        """Save battle cards locally for testing."""
        return self.storage.save_to_local(battle_cards, output_file)


def main():
    """Main execution."""
    # Configuration
    GCS_BUCKET = "dqe-fiber-data"
    INPUT_CSV = "tenants_enriched.csv"
    OUTPUT_NAME = "dqe_prospects"
    MAX_WORKERS = 10
    
    # Initialize generator
    generator = CSVBattleCardGenerator(gcs_bucket=GCS_BUCKET)
    
    # Process CSV with parallel execution
    battle_cards = generator.process_csv(INPUT_CSV, max_workers=MAX_WORKERS)
    
    # Save to GCS
    generator.save_to_gcs(battle_cards, output_name=OUTPUT_NAME)
    
    # Also save locally for debugging
    generator.save_to_local(battle_cards, "battle_cards_local.json")


if __name__ == "__main__":
    main()