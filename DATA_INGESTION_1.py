from fastapi import FastAPI, BackgroundTasks
from kafka import KafkaProducer
import requests, json, yaml, time, logging
from datetime import datetime

app = FastAPI(title="Dynamic Data Ingestion Agent")

KAFKA_BROKER = "localhost:9092"
RAW_TOPIC = "env_raw_data"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

logging.basicConfig(level=logging.INFO)
# Dynamic source loading
def load_sources():
    with open("config/sources_config.yaml", "r") as f:
        config = yaml.safe_load(f)
    return config["sources"]

@app.get("/fetch_all")
def fetch_all(background_tasks: BackgroundTasks):
    """Fetch data from all configured sources concurrently"""
    sources = load_sources()
    for source in sources:
        background_tasks.add_task(fetch_source_data, source)
    return {"status": "Data collection tasks started"}

def fetch_source_data(source):
    """Generic data collector for any API and multiple locations"""
    try:
        for loc in source["locations"]:
            params = {**source["params"], **loc}
            params["latitude"] = loc.get("latitude")
            params["longitude"] = loc.get("longitude")

            response = requests.get(source["base_url"], params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            payload = {
                "source": source["name"],
                "type": source["type"],
                "location": loc["name"],
                "timestamp": datetime.utcnow().isoformat(),
                "raw_data": data,
            }

            producer.send(RAW_TOPIC, payload)
            producer.flush()
            logging.info(f"[{source['name']}:{loc['name']}] sent data to Kafka")

    except Exception as e:
        logging.error(f"Error fetching data from {source['name']}: {e}")

@app.get("/health")
def health():
    return {"status": "Dynamic Data Ingestion Agent running"}
