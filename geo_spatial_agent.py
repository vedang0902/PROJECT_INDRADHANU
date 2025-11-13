# geo_spatial_agent.py
from kafka import KafkaConsumer, KafkaProducer
import json
import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon, Point

KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "geospatial-data"
OUTPUT_TOPIC = "geospatial_results"

# Kafka Setup
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def analyze_deforestation(data):
    """
    Dummy spatial analysis – simulates forest cover loss detection.
    """
    region_name = data.get("region", "Unknown")
    forest_cover_prev = np.random.uniform(60, 80)
    forest_cover_current = forest_cover_prev - np.random.uniform(0, 5)
    deforestation_rate = round(((forest_cover_prev - forest_cover_current) / forest_cover_prev) * 100, 2)

    result = {
        "region": region_name,
        "forest_cover_previous": round(forest_cover_prev, 2),
        "forest_cover_current": round(forest_cover_current, 2),
        "deforestation_rate": deforestation_rate,
        "alert": deforestation_rate > 2.0
    }
    return result

print("🌍 Geo-Spatial Agent started. Listening for data...")

try:
    for msg in consumer:
        data = msg.value
        print(f"Received preprocessed data: {data}")
        result = analyze_deforestation(data)
        print(f"Geo-Spatial analysis result: {result}")

        producer.send(OUTPUT_TOPIC, result)
        producer.flush()

except KeyboardInterrupt:
    print("\n🛑 Geo-Spatial Agent stopped safely.")
finally:
    consumer.close()
    producer.close()
