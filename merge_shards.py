from google.cloud import storage
import json

GCS_BUCKET = "dqe-fiber-data"
OUTPUT_NAME = "dqe_prospects"

client = storage.Client()
bucket = client.bucket(GCS_BUCKET)

# Auto-discover all shards
prefix = f"csv-battle-cards/{OUTPUT_NAME}_shard_"
shards = sorted(bucket.list_blobs(prefix=prefix), key=lambda b: b.name)

if not shards:
    print("No shards found.")
    exit(1)

all_cards = []
for blob in shards:
    data = json.loads(blob.download_as_text())
    cards = data.get("battle_cards", [])
    all_cards.extend(cards)
    print(f"✓ {blob.name.split('/')[-1]}: {len(cards)} records")

out = bucket.blob(f"csv-battle-cards/{OUTPUT_NAME}.json")
out.upload_from_string(json.dumps({"battle_cards": all_cards}, indent=2))
print(f"\n✓ Done — {len(all_cards)} total records → {OUTPUT_NAME}.json")