import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from Database.db import get_connection

#page configuration
st.set_page_config(
    page_title="Real-time weather analytics",
    page_icon="🌦️",
    layout="wide"
    )

st.title  = ("Real-time weather analytics")
st.caption = ("Weather moniitoring and analytic system")

#connecting to database

@st.cache_data(ttl=30)

def load_weather_data():

    connection = get_connection()

    try:

            query = """
            SELECT
                city,
                latitude,
                longitude,
                temperature,
                humidity,
                feels_like,
                wind_speed,
                wind_direction,
                precipitation,
                weather_code,
                temperature_category,
                timestamp
            FROM weather_records
            ORDER BY timestamp DESC
            LIMIT 10000
        """
            df = pd.read_sql_query(
              query,
              connection
        )   
            return df
    finally:
          connection.close() 
#loading dataset(df)

try:
    df = load_weather_data()

except Exception as e:

      st.error(f"Database connection failed:",e)
      st.stop()

if df.empty:

      st.warning("No weather records available in df")
      st.stop()

#Now adding some sidebar filters in dashboard

st.sidebar.header("Dashboard filters")

cities = sorted(df["city"].dropna().unique())

selected_cities = st.sidebar.multiselect(
   "select cities",  
    cities,
   default=cities[:10]
)

filtered_df =df[
      df['city'].isin(selected_cities)
].copy()

if filtered_df.empty:
      st.warning("Select at least one city")
      st.stop()
    
#KPI section

st.subheader("weather overview")

col1, col2, col3, col4, = st.columns(4)
total_records = len(filtered_df)
total_cities = filtered_df['city'].nunique()
average_temperature = filtered_df['temperature'].mean()
average_humidity = filtered_df['humidity'].mean()

col1.metric(
      "Total Records",
      total_records
) 

col2.metric(
      "cities", 
      total_cities
      )

col3.metric(
      "Average Temperature",
      average_temperature
)
                
col4.metric(
      "Average Humidity",
      average_humidity
) 

st.divider()

#temperature analysis

st.subheader("🌡️ Temperature Analytics")

col1, col2 = st.columns(2)

with col1:
      temperature_chart = px.bar(
            filtered_df.groupby(
                  'city',
                  as_index=False
            )['temperature'].mean().sort_values(
                  "temperature",
                  ascending=False
            ),
            x ='city',
            y = 'temperature',
            title="Averagre temperature by city",
            labels ={
                  "temperature" : "temperature (°C)",
                  "city" : "city"
            }

      )

      st.plotly_chart(
            temperature_chart,
            use_container_width=True
      )

with col2:
      temperature_trend = filtered_df.sort_values(
            "timestamp"
      )

      trend_chart = px.line(
            temperature_trend,
            x = "timestamp",
            y = "temperature",
            color= "city",
            title = "temperature Trend",
            labels= {
                  
                    "temperature": "Temperature (°C)",
                    "timestamp": "Time"
            }
      )
      st.plotly_chart(
            trend_chart,
            use_container_width=True
      )
st.divider()      

#Humidity andd wind analysis

st.subheader("💨 Humidity and Wind Analytics")

col1, col2 = st.columns(2)

with col1:
      humidity_chart = px.bar(
            filtered_df.groupby(
                  "city",
                  as_index=False
            )["humidity"].mean(),
            x="city",
            y="humidity",
            title="Average Humidity by City",
            labels={
            "humidity": "Humidity (%)"
        }

      )

      st.plotly_chart(
            humidity_chart,
            use_container_width=True
      )

with col2:
      wind_chart = px.scatter(
            filtered_df,
            x = "wind_speed",
            y = "temperature",
            color = 'city',
            size = "precipitation",
            hover_name="city",
            title="wind speed vs temperature",
            labels={
                    "wind_speed": "Wind Speed",
                    "temperature": "Temperature (°C)"
            }
                  
        )

      st.plotly_chart(
            wind_chart,
            use_container_width=True
      ) 
st.divider()

#weather category
st.subheader("🌤️ Temperature Categories")

category_chart = px.pie(
      filtered_df,
      names="temperature_category",
      title="temperature category distribution"
)

st.plotly_chart(
      category_chart,
      use_container_width=True
)

#raw weather data

st.subheader("📊 Weather Records")

st.dataframe(
      filtered_df,
      use_container_width=True
)

#to refresh dashboard

if st.button("refresh dashboards"):
            st.cache_data.clear(
            st.rerun()
          
      )

