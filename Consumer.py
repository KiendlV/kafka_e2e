"""Reads weather readings from Kafka, prints them and tracks the min/max per city."""
import json
import os

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather.readings")
GROUP_ID = os.getenv("KAFKA_GROUP", "weather-printer")
# Optional: print an alert when the temperature goes below this value (deg C).
ALERT_BELOW_C = float(os.getenv("ALERT_BELOW_C", "0"))


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])
    stats: dict[str, dict[str, float]] = {}
    print(f"Consuming {TOPIC} as group '{GROUP_ID}'. Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Error: {msg.error()}")
                continue

            reading = json.loads(msg.value())
            city = reading["city"]
            temp = reading["temperature_c"]

            s = stats.setdefault(city, {"min": temp, "max": temp})
            s["min"] = min(s["min"], temp)
            s["max"] = max(s["max"], temp)

            print(
                f"[p{msg.partition()}] {city:<8} {temp:5.1f} C  "
                f"{reading['humidity_pct']:3.0f}%  {reading['wind_kmh']:4.1f} km/h  "
                f"(min {s['min']:.1f} / max {s['max']:.1f})"
            )
            if temp < ALERT_BELOW_C:
                print(f"   ALERT: {city} is below {ALERT_BELOW_C} C")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()