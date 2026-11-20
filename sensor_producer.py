import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

def create_producer():
    return KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8")
    )

def simulate_telemetry():
    producer = create_producer()
    sensors = ["NODE_NORTH_01", "NODE_SOUTH_02", "NODE_CENTRAL_03"]
    print("Streaming live environmental telemetry to Kafka topic 'sensor_telemetry'...")

    try:
        while True:
            sensor = random.choice(sensors)
            is_spike = random.random() < 0.15  # 15% probability of an acute PM2.5 spike
            pm25 = random.uniform(195.0, 265.0) if is_spike else random.uniform(95.0, 135.0)

            payload = {
                "sensor_id": sensor,
                "timestamp": datetime.now().isoformat(),
                "pm25_reading": round(pm25, 2),
                "humidity": round(random.uniform(42.0, 78.0), 2)
            }
            producer.send("sensor_telemetry", value=payload)
            producer.flush()
            print(f"Published -> {payload}")
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Stopping telemetry producer.")
    finally:
        producer.close()

if __name__ == "__main__":
    simulate_telemetry()