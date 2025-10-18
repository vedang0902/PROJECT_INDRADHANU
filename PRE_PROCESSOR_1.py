from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer
import json, logging, time, numpy as np
from datetime import datetime

app = FastAPI(title="Dynamic Preprocessing Agent")

RAW_TOPIC = "env_raw_data"
PROCESSED_TOPIC = "env_processed_data"
BROKER = "localhost:9092"

consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=[BROKER],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True
)

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

logging.basicConfig(level=logging.INFO)

def sanitize(value):
    """Convert values to float if possible, else mark as NaN"""
    try:
        return float(value)
    except (ValueError, TypeError):
        return np.nan

def flatten_json(raw, prefix=""):
    """Flatten nested JSONs dynamically"""
    items = {}
    for k, v in raw.items():
        new_key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            items.update(flatten_json(v, new_key))
        elif isinstance(v, list):
            if len(v) > 0 and isinstance(v[0], dict):
                for i, sub in enumerate(v):
                    items.update(flatten_json(sub, f"{new_key}[{i}]"))
            else:
                items[new_key] = v
        else:
            items[new_key] = v
    return items

@app.on_event("startup")
async def start_processing():
    """Start listening for incoming data"""
    logging.info("Preprocessing agent active...")
    for msg in consumer:
        raw_record = msg.value
        processed = preprocess(raw_record)
        producer.send(PROCESSED_TOPIC, processed)
        producer.flush()
        logging.info(f"Processed record for {processed['source']} → sent downstream")

def preprocess(record):
    """Generic preprocessing pipeline"""
    try:
        raw_data = record.get("raw_data", {})
        flat = flatten_json(raw_data)

        # Clean numeric fields
        cleaned = {k: sanitize(v) for k, v in flat.items()}

        processed = {
            "source": record.get("source"),
            "type": record.get("type"),
            "location": record.get("location"),
            "timestamp": record.get("timestamp"),
            "processed_at": datetime.utcnow().isoformat(),
            "data": cleaned
        }

        return processed

    except Exception as e:
        logging.error(f"Error preprocessing record: {e}")
        return record

@app.get("/health")
def health():
    return {"status": "Dynamic Preprocessing Agent running"}
