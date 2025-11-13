# send_test_processed.py
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Regions to simulate
regions = [
    "Amazon Rainforest", "Pune", "Delhi",
    "California", "Beijing", "London",
    "Tokyo", "Sydney", "Cape Town"
]

print("🌍 Sending continuous geo-spatial data to Kafka... (Press Ctrl+C to stop)\n")

try:
    while True:
        # Choose random region
        region = random.choice(regions)
        # Simulate message
        data = {
            "region": region,
            "timestamp": datetime.utcnow().isoformat(),
            "forest_cover_previous": round(random.uniform(60, 90), 2),
            "forest_cover_current": round(random.uniform(55, 89), 2)
        }

        # Send to Kafka topic
        producer.send('geospatial-data', value=data)
        producer.flush()

        print(f"📤 Sent: {data}")

        # Wait 3 seconds before next message
        time.sleep(3)

except KeyboardInterrupt:
    print("\n🛑 Stopped sending data.")
