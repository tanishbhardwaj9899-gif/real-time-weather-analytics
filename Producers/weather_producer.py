import requests
import json
import time
import pandas as pd

from pathlib import Path
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError


# ==========================================
# CONFIGURATION
# ==========================================

URL = "https://api.open-meteo.com/v1/forecast"

KAFKA_TOPIC = "weather_data"

BATCH_SIZE = 50

# Refresh all 500+ cities every 2 hours
COLLECTION_INTERVAL = 7200

# Delay between API batches
BATCH_DELAY = 2


# ==========================================
# KAFKA PRODUCER
# ==========================================

producer = KafkaProducer(

    bootstrap_servers=["localhost:9092"],

    value_serializer=lambda v: json.dumps(v).encode("utf-8"),

    key_serializer=lambda k: k.encode("utf-8"),

    acks="all",

    retries=5
)


# ==========================================
# LOAD CITY DATASET
# ==========================================

cities_path = Path(__file__).resolve().parent / "europe_cities.csv"

cities = pd.read_csv(cities_path, encoding="utf-8-sig")

cities.columns = (
    cities.columns.str.strip()
    .str.lower()
    .str.replace(" ", "_")
)

cities = cities.rename(columns={
    "city_name": "city",
    "name": "city",
    "lat": "latitude",
    "lon": "longitude",
    "lng": "longitude",
})

required = {"city", "latitude", "longitude"}

missing = required - set(cities.columns)

if missing:
    raise ValueError(f"Missing columns: {missing}")


# Convert coordinates to numeric values
cities["latitude"] = pd.to_numeric(
    cities["latitude"], errors="coerce"
)

cities["longitude"] = pd.to_numeric(
    cities["longitude"], errors="coerce"
)

cities = cities.dropna(
    subset=["city", "latitude", "longitude"]
).reset_index(drop=True)


print("Total cities loaded:", len(cities))


# ==========================================
# API REQUEST WITH RETRY
# ==========================================

def fetch_weather(params, retries=5):

    for attempt in range(retries):

        try:

            response = requests.get(
                URL,
                params=params,
                timeout=60
            )

            if response.status_code == 429:

                retry_after = response.headers.get("Retry-After")

                if retry_after and retry_after.isdigit():
                    wait_time = int(retry_after)
                else:
                    wait_time = min(30 * (2 ** attempt), 300)

                print(
                    f"Rate limit reached. "
                    f"Waiting {wait_time} seconds..."
                )

                time.sleep(wait_time)

                continue

            response.raise_for_status()

            return response.json()

        except requests.exceptions.RequestException as e:

            print("API request error:", e)

            if attempt == retries - 1:
                raise

            wait_time = min(10 * (2 ** attempt), 120)

            print(f"Retrying in {wait_time} seconds...")

            time.sleep(wait_time)

    raise RuntimeError("Maximum API retries exceeded")


# ==========================================
# PROCESS ONE BATCH
# ==========================================

def process_batch(batch):

    latitudes = ",".join(
        str(x) for x in batch["latitude"]
    )

    longitudes = ",".join(
        str(x) for x in batch["longitude"]
    )

    params = [

        ("latitude", latitudes),

        ("longitude", longitudes),

        ("current", "temperature_2m"),

        ("current", "relative_humidity_2m"),

        ("current", "apparent_temperature"),

        ("current", "precipitation"),

        ("current", "wind_speed_10m"),

        ("current", "wind_direction_10m"),

        ("current", "weather_code"),

        ("timezone", "UTC")

    ]

    data = fetch_weather(params)

    # Multiple locations return a list of responses
    if isinstance(data, dict):
        data = [data]

    if len(data) != len(batch):

        raise ValueError(
            f"Expected {len(batch)} responses, "
            f"received {len(data)}"
        )

    # Create individual Kafka events
    for (_, row), weather in zip(batch.iterrows(), data):

        current = weather["current"]

        weather_data = {

            "city": row["city"],

            "latitude": row["latitude"],

            "longitude": row["longitude"],

            "temperature": current["temperature_2m"],

            "humidity": current["relative_humidity_2m"],

            "feels_like": current["apparent_temperature"],

            "precipitation": current["precipitation"],

            "wind_speed": current["wind_speed_10m"],

            "wind_direction": current["wind_direction_10m"],

            "weather_code": current["weather_code"],

            "timestamp": current["time"],

            "produced_at": datetime.now(
                timezone.utc
            ).isoformat()

        }

        try:

            producer.send(
                KAFKA_TOPIC,
                key=str(row["city"]),
                value=weather_data
            )

        except KafkaError as e:

            print(f"Kafka error for {row['city']}: {e}")

    producer.flush()

    print(
        f"Batch completed: {len(batch)} cities sent to Kafka"
    )


# ==========================================
# COLLECT WEATHER DATA
# ==========================================

def collect_weather():

    total_cities = len(cities)

    total_batches = (
        total_cities + BATCH_SIZE - 1
    ) // BATCH_SIZE

    print("\nStarting weather collection...")

    print("Total cities:", total_cities)

    print("Batch size:", BATCH_SIZE)

    print("Total batches:", total_batches)

    for start in range(0, total_cities, BATCH_SIZE):

        batch = cities.iloc[
            start:start + BATCH_SIZE
        ]

        batch_number = (
            start // BATCH_SIZE
        ) + 1

        print(
            f"\nProcessing batch "
            f"{batch_number}/{total_batches}"
        )

        try:

            process_batch(batch)

        except Exception as e:

            print(
                f"Batch {batch_number} failed: {e}"
            )

        # Avoid sending batches too quickly
        if batch_number < total_batches:
            time.sleep(BATCH_DELAY)

    producer.flush()

    print("\nWeather collection completed!")


# ==========================================
# CONTINUOUS COLLECTION
# ==========================================

try:

    while True:

        start_time = time.time()

        collect_weather()

        elapsed = time.time() - start_time

        remaining = max(
            0,
            COLLECTION_INTERVAL - elapsed
        )

        print(
            f"\nCollection finished in "
            f"{elapsed:.2f} seconds."
        )

        print(
            f"Next collection in "
            f"{remaining / 60:.2f} minutes."
        )

        time.sleep(remaining)


except KeyboardInterrupt:

    print("\nStopping Weather Producer...")


finally:

    producer.flush()

    producer.close()