# kafka-weather

```
Open-Meteo API --> producer.py --> Kafka topic "weather.readings" --> consumer.py
                                   (3 partitions, key = city)
```

## Run it

You need Docker and Python 3.

```bash
# 1. Start a single-node Kafka broker (KRaft mode, no ZooKeeper)
docker compose up -d

# 2. Install the Python dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Terminal A: start the producer (creates the topic on first run)
python producer.py

# 4. Terminal B: start the consumer
python consumer.py
```