"""Polls the Open-Meteo API and publishes one message per city to Kafka."""
import json
import os
import time

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather.readings")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))

# name -> (latitude, longitude). Add or change cities as you like.
CITIES = {
    "Munich": (48.14, 11.58),
    "Berlin": (52.52, 13.41),
    "Hamburg": (53.55, 9.99),
}

API_URL = "https://api.open-meteo.com/v1/forecast"


def ensure_topic(admin: AdminClient) -> None:
    """Create the topic with 3 partitions if it doesn't exist yet."""
    existing = admin.list_topics(timeout=10).topics
    if TOPIC in existing:
        return
    futures = admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])
    for _, future in futures.items():
        future.result()  # raises if creation failed
    print(f"Created topic {TOPIC}")


def fetch_weather(city: str, lat: float, lon: float) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
    }
    response = requests.get(API_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    current = data["current"]
    return {
        "city": city,
        "observed_at": current["time"],
        "temperature_c": current["temperature_2m"],
        "humidity_pct": current["relative_humidity_2m"],
        "wind_kmh": current["wind_speed_10m"],
        "fetched_at": time.time(),
    }


def on_delivery(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"-> {msg.key().decode()} (partition {msg.partition()}, offset {msg.offset()})")


def main() -> None:
    ensure_topic(AdminClient({"bootstrap.servers": BOOTSTRAP}))
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    print(f"Producing to {TOPIC} every {POLL_SECONDS}s. Ctrl+C to stop.")
    try:
        while True:
            for city, (lat, lon) in CITIES.items():
                try:
                    reading = fetch_weather(city, lat, lon)
                except (requests.RequestException, KeyError) as exc:
                    print(f"Could not fetch {city}: {exc}")
                    continue
                # Same key -> same partition, so each city's readings stay in order.
                producer.produce(
                    TOPIC,
                    key=city,
                    value=json.dumps(reading),
                    callback=on_delivery,
                )
            producer.poll(0)
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()