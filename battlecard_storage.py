"""Storage logic for battle card generation."""

import json
import time
from typing import Dict, List
from google.cloud import storage


class BattleCardStorage:
    """Handles saving battle cards to GCS and local storage."""
    
    def __init__(self, gcs_bucket: str):
        """Initialize storage client."""
        self.gcs_bucket = gcs_bucket
        self.storage_client = storage.Client()
    
    def _calculate_summary(self, battle_cards: List[Dict], 
                          input_tokens: int, output_tokens: int) -> Dict:
        """Calculate summary statistics for battle cards."""
        total = len(battle_cards)
        with_analysis = sum(1 for bc in battle_cards if bc['llm_analysis']['overall_score'] > 0)
        
        scores = [bc['llm_analysis']['overall_score'] for bc in battle_cards 
                 if bc['llm_analysis']['overall_score'] > 0]
        avg_score = round(sum(scores) / len(scores), 1) if scores else 0
        
        # Calculate average confidence
        confidences = [bc['llm_analysis']['data_confidence']['confidence_score'] 
                      for bc in battle_cards 
                      if bc['llm_analysis']['overall_score'] > 0]
        avg_confidence = round(sum(confidences) / len(confidences), 2) if confidences else 0
        
        # Score distribution
        score_ranges = {
            "80-100 (Excellent)": sum(1 for s in scores if s >= 80),
            "60-79 (Good)": sum(1 for s in scores if 60 <= s < 80),
            "40-59 (Fair)": sum(1 for s in scores if 40 <= s < 60),
            "20-39 (Poor)": sum(1 for s in scores if 20 <= s < 40),
            "0-19 (Disqualified)": sum(1 for s in scores if s < 20)
        }
        
        # Top prospects
        top_prospects = sorted(
            [
                {
                    "business_name": bc['ey_file_data']['Name'],
                    "address": bc['ey_file_data']['Address'],
                    "city": bc['ey_file_data']['City'],
                    "state": bc['ey_file_data']['State'],
                    "score": bc['llm_analysis']['overall_score'],
                    "confidence": bc['llm_analysis']['data_confidence']['confidence_score'],
                    "icp_score": bc['llm_analysis']['icp_fit']['icp_fit_score'],
                    "validated_employees": bc['llm_analysis']['data_confidence']['validated_employee_count'],
                    "priority": bc['llm_analysis']['sales_intelligence']['priority_level'],
                    "dqe_distance": bc['connectbase_data'].get('DQE_Site_Distance', 'N/A')
                }
                for bc in battle_cards if bc['llm_analysis']['overall_score'] > 0
            ],
            key=lambda x: x['score'],
            reverse=True
        )[:20]
        
        return {
            "total_records": total,
            "analyzed_records": with_analysis,
            "skipped_records": total - with_analysis,
            "generation_date": time.strftime('%Y-%m-%d %H:%M:%S'),
            "avg_score": avg_score,
            "avg_confidence": avg_confidence,
            "score_distribution": score_ranges,
            "token_usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": input_tokens + output_tokens
            },
            "top_prospects": top_prospects
        }
    
    def save_to_gcs(self, battle_cards: List[Dict], output_name: str,
                   input_tokens: int, output_tokens: int) -> bool:
        """Save battle cards to GCS for map visualization."""
        try:
            blob_path = f"csv-battle-cards/{output_name}.json"
            bucket = self.storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob(blob_path)
            
            summary = self._calculate_summary(battle_cards, input_tokens, output_tokens)
            
            output = {
                "summary": summary,
                "battle_cards": battle_cards
            }
            
            blob.upload_from_string(
                json.dumps(output, indent=2),
                content_type='application/json'
            )
            
            print(f"\n✓ Battle cards saved: gs://{self.gcs_bucket}/{blob_path}")
            print(f"\n=== SUMMARY ===")
            print(f"Total Records: {summary['total_records']}")
            print(f"Analyzed: {summary['analyzed_records']}")
            print(f"Average Score: {summary['avg_score']}/100")
            print(f"Average Confidence: {summary['avg_confidence']}")
            print(f"\nTop 5 Prospects:")
            for i, p in enumerate(summary['top_prospects'][:5], 1):
                print(f"  {i}. {p['business_name']} - Score: {p['score']}, "
                      f"Confidence: {p['confidence']:.2f}, Priority: {p['priority']}")
            
            return True
            
        except Exception as e:
            print(f"Error saving battle cards: {str(e)}")
            return False
    
    def save_to_local(self, battle_cards: List[Dict], output_file: str) -> bool:
        """Save battle cards locally for testing."""
        try:
            total = len(battle_cards)
            with_analysis = sum(1 for bc in battle_cards if bc['llm_analysis']['overall_score'] > 0)
            scores = [bc['llm_analysis']['overall_score'] for bc in battle_cards 
                     if bc['llm_analysis']['overall_score'] > 0]
            avg_score = round(sum(scores) / len(scores), 1) if scores else 0
            
            output = {
                "summary": {
                    "total_records": total,
                    "analyzed_records": with_analysis,
                    "avg_score": avg_score,
                    "generation_date": time.strftime('%Y-%m-%d %H:%M:%S')
                },
                "battle_cards": battle_cards
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2)
            
            print(f"\n✓ Battle cards saved locally: {output_file}")
            return True
            
        except Exception as e:
            print(f"Error saving locally: {str(e)}")
            return False