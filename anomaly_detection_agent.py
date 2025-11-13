# anomaly_detection_agent.py
from kafka import KafkaConsumer, KafkaProducer
from sklearn.ensemble import IsolationForest
import json
import numpy as np

KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "geospatial_results"
OUTPUT_TOPIC = "anomaly_alerts"

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

model = IsolationForest(contamination=0.1, random_state=42)

def detect_anomalies(records):
    """
    Detect anomalies from deforestation or AQI patterns.
    """
    values = np.array([[r["deforestation_rate"]] for r in records])
    predictions = model.fit_predict(values)
    results = []
    for i, r in enumerate(records):
        anomaly = predictions[i] == -1
        results.append({
            "region": r["region"],
            "deforestation_rate": r["deforestation_rate"],
            "is_anomaly": bool(anomaly),
            "alert_message": f"⚠️ Sudden deforestation spike in {r['region']}" if anomaly else None
        })
    return results

print("🚨 Anomaly Detection Agent started. Monitoring Geo-Spatial outputs...")

buffer = []
try:
    for msg in consumer:
        record = msg.value
        buffer.append(record)

        # Simple batch detection
        if len(buffer) >= 5:
            results = detect_anomalies(buffer)
            for res in results:
                print(res)
                if res["is_anomaly"]:
                    producer.send(OUTPUT_TOPIC, res)
            producer.flush()
            buffer.clear()

except KeyboardInterrupt:
    print("\n🚫 Anomaly Detection Agent stopped safely.")
finally:
    consumer.close()
    producer.close()
