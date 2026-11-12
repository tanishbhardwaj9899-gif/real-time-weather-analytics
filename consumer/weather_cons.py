from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "weather_data",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="weather_analytics_group",

    # Convert JSON messages into Python dictionaries
    value_deserializer=lambda data: json.loads(data.decode("utf-8"))
)

print("Waiting for weather data.....")

for message in consumer:

    weather = message.value

    # Check received data type
    print("Data Type:", type(weather))

    if not isinstance(weather, dict):
        print("Invalid message:", weather)
        continue

    print("--------------------------------")
    print("City:", weather["city"])
    print("Temperature:", weather["temperature"])
    print("Humidity:", weather["humidity"])
    print("Wind Speed:", weather["wind_speed"])
    print("Precipitation:", weather["precipitation"])
    print("Timestamp:", weather["timestamp"])
