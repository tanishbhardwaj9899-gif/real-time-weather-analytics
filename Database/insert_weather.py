import pandas as pd
from .db import get_connection


def insert_weather(df):
    if df.empty:
        print("No weather data to insert.")
        return 0

    required_columns = [
        "city",
        "latitude",
        "longitude",
        "temperature",
        "humidity",
        "feels_like",
        "wind_speed",
        "wind_direction",
        "precipitation",
        "weather_code",
        "temperature_category",
        "timestamp",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")

    clean_df = df[required_columns].astype(object)
    clean_df = clean_df.where(pd.notna(clean_df), None)
    records = list(clean_df.itertuples(index=False, name=None))

    query = """
        INSERT INTO weather_records (
            city, latitude, longitude, temperature, humidity, feels_like,
            wind_speed, wind_direction, precipitation, weather_code,
            temperature_category, timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.executemany(query, records)
        connection.commit()

        inserted_count = cursor.rowcount
        print(f"Successfully inserted {inserted_count} weather records")
        return inserted_count
    except Exception as e:
        if connection:
            connection.rollback()
        print("Database insertion failed:", e)
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()