import pandas as pd
from insert_weather import insert_weather

data = {
    "city": ["Hamburg", "Berlin"],
    "latitude": [53.55, 52.52],
    "longitude": [9.99, 13.40],
    "temperature": [12.5, 14.2],
    "humidity": [75, 65],
    "feels_like": [11.8, 13.5],
    "wind_speed": [10.5, 8.2],
    "wind_direction": [180, 220],
    "precipitation": [0.0, 0.2],
    "weather_code": [3, 2],
    "temperature_category": ["Cold", "Moderate"],
    "timestamp": pd.to_datetime([
        "2026-11-10 12:00:00",
        "2026-11-10 12:00:00"
    ])
}

df = pd.DataFrame(data)

insert_weather(df)