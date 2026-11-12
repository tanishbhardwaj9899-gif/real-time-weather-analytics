from kafka import KafkaConsumer
import json
import pandas as pd
import os
from datetime import datetime, timezone
import time

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from Performance_monitor.monitor import PerformanceMonitor
from Database.insert_weather import insert_weather

#kafka consumer and data storage config

consumer = KafkaConsumer(
    "weather_data",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="weather_processing_group",
    value_deserializer=lambda data: json.loads(
        data.decode("utf-8")
    )
)    

monitor = PerformanceMonitor()

os.makedirs("data", exist_ok=True)

OUTPUT_FILE= "data/processed_weather.csv"

#Data processing function
def process_weather_data(records):
    
    df = pd.DataFrame(records)

    total_records = len(df)

    print("\nOriginal Data:")
    print(df.head())

    #data cleaning
    required_columns = [
        "city",
        "temperature",
        "humidity",
        "wind_speed",
        "precipitation",
        "timestamp"
    ]

    df = df.dropna(subset=required_columns)

    numeric_columns = [
        "temperature",
        "humidity",
        "wind_speed",
        "precipitation"
    ]

    for col in numeric_columns:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )

    df = df.dropna(subset=numeric_columns)

    #Data validation

    total_records = len(df)

    df = df[
    (df['temperature'].between(-90,60)) &   
    (df["humidity"].between(0, 100)) &
    (df["wind_speed"] >= 0) &
    (df["precipitation"] >= 0)
    ].copy()

    valid_records = len(df)
    invalid_records = total_records - valid_records

    monitor.record_accuracy(
        valid_records,
        invalid_records
    )

    print("valid records:", valid_records)
    print("invalid_records:",invalid_records)

    if df.empty:

        print("No valid weather records found.")

        return 0

    # Data transformation
    
    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce",
        utc=True
    )
    df = df.dropna(subset=["timestamp"])

    df["temperature_category"] = pd.cut(
        df["temperature"],
        bins=[float("-inf"), 10, 20, 30, float("inf")],
        labels=["cold", "moderate", "warm", "very hot"]
    )

    temperature_summary = df.groupby("city", as_index=False).agg(
        max_temperature=("temperature", "max"),
        min_temperature=("temperature", "min"),
        average_temperature=("temperature", "mean")
    )

    temperature_summary["average_temperature"] = (
        temperature_summary["average_temperature"].round(2)
    )

    print("\nTemperature summary by city:")
    print(temperature_summary)


    #printing coldest and hottest city
    #  
    coldest = temperature_summary.loc[
        temperature_summary["min_temperature"].idxmin()
    ]
    hottest = temperature_summary.loc[
        temperature_summary["max_temperature"].idxmax()
    ]

    print(f"\nColdest city: {coldest['city']} ({coldest['min_temperature']}°)")
    print(f"Hottest city: {hottest['city']} ({hottest['max_temperature']}°)")

     #Storing process data

    file_exsits = os.path.exists(OUTPUT_FILE)

    df.to_csv(

        OUTPUT_FILE,

        mode="a",

        header=not file_exsits,

        index=False
    )

    print("\nProcessed data saved sucessfully")
    print(df.head())

    #Store processed data in postgreSQ  

    inserted_count = insert_weather(df) 
    print(f"successfully stored{inserted_count} records in postgresql")  
        
    return inserted_count

#---KAFKA real time processing----

print("starting weather data processor....")

try:
    while True:
        messages = consumer.poll(
            timeout_ms=5000,
            max_records=100
        )
        records = []

        for partition_messages in messages.values():
            for message in partition_messages:
                records.append(message.value)

        if records:
            print("\nNew Weather Events Received!")
            monitor.record_received(len(records))

            # Calculate event latency
            for record in records:
                monitor.record_latency(record.get("produced_at"))

            start_time = time.perf_counter()

            try:
                inserted_count = process_weather_data(records)
                processing_time = time.perf_counter() - start_time

                monitor.record_processed(
                    inserted_count or 0,
                    processing_time
                )

                print(
                    "Processing time:",
                    round(processing_time, 4),
                    "seconds"
                )

                consumer.commit()
                print("Kafka offsets committed successfully!")

            except Exception as e:
                monitor.record_failed(len(records))
                print("Processing failed:", e)
                
            monitor.display_metrics()

except KeyboardInterrupt:
    print("\nStopping Weather Processor...")

finally:
    consumer.close()