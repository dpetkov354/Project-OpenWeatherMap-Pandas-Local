import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# OpenWeatherMap API key
api_key = "67b8af2a44ee02263e53f9863816ff8c"

# List of cities for which you want to retrieve weather data
cities = ["Milano", "Bologna", "Cagliari", "Sofia", "Plovdiv"]

# Units of measurement
units = "metric"


# Request function
def get_weather_data(key, name_city, unit):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": name_city,
        "appid": key,
        "units": unit
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch weather data. Status code:", response.status_code)
        return None


# PostgreSQL connection properties
db_password = "960352871454Vv!"
db_user = "postgres"
db_host = "localhost"
db_port = "5432"
db_name = "local_openweather_db"
postgres_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create SQLAlchemy engine
engine = create_engine(postgres_url)

# Retrieve weather data for each city
for city in cities:
    data = get_weather_data(api_key, city, units)
    if data:
        # Extract information
        city_id = data['id']
        city_name = data['name']
        longitude = data['coord']['lon']
        latitude = data['coord']['lat']
        weather_id = data['weather'][0]['id']
        weather_main = data['weather'][0]['main']
        weather_description = data['weather'][0]['description']
        weather_icon = data['weather'][0]['icon']
        base = data['base']
        temperature = data['main']['temp']
        feels_like = data['main']['feels_like']
        temp_min = data['main']['temp_min']
        temp_max = data['main']['temp_max']
        pressure = data['main']['pressure']
        humidity = data['main']['humidity']
        visibility = data['visibility']
        wind_speed = data['wind']['speed']
        wind_deg = data['wind']['deg']
        clouds_all = data['clouds']['all']
        dt = datetime.utcfromtimestamp(data['dt'])
        sys_type = data['sys']['type']
        sys_id = data['sys']['id']
        sys_country = data['sys']['country']
        sys_sunrise = datetime.utcfromtimestamp(data['sys']['sunrise'])
        sys_sunset = datetime.utcfromtimestamp(data['sys']['sunset'])
        timezone = data['timezone']
        cod = data['cod']

        # Create DataFrame
        data = [(city_id,
                 city_name,
                 longitude,
                 latitude,
                 weather_id,
                 weather_main,
                 weather_description,
                 weather_icon,
                 base,
                 temperature,
                 feels_like,
                 temp_min,
                 temp_max,
                 pressure,
                 humidity,
                 visibility,
                 wind_speed,
                 wind_deg,
                 clouds_all,
                 dt,
                 sys_type,
                 sys_id,
                 sys_country,
                 sys_sunrise,
                 sys_sunset,
                 timezone,
                 cod
                 )]

        columns = ['city_id',
                   'city_name',
                   'longitude',
                   'latitude',
                   'weather_id',
                   'weather_main',
                   'weather_description',
                   'weather_icon',
                   'base',
                   'temperature',
                   'feels_like',
                   'temp_min',
                   'temp_max',
                   'pressure',
                   'humidity',
                   'visibility',
                   'wind_speed',
                   'wind_deg',
                   'clouds_all',
                   'dt',
                   'sys_type',
                   'sys_id',
                   'sys_country',
                   'sys_sunrise',
                   'sys_sunset',
                   'timezone',
                   'cod']

        dtype = {'city_id': int,
                 'weather_id': int,
                 'temperature': float,
                 'feels_like': float,
                 'temp_min': float,
                 'temp_max': float,
                 'pressure': int,
                 'humidity': int,
                 'visibility': int,
                 'wind_speed': float,
                 'wind_deg': int,
                 'clouds_all': int,
                 'dt': 'datetime64[ns]',
                 'sys_type': int,
                 'sys_id': int,
                 'sys_sunrise': 'datetime64[ns]',
                 'sys_sunset': 'datetime64[ns]',
                 'timezone': int
                 }

        df = pd.DataFrame(data, columns=columns).astype(dtype)

        try:
            df.to_sql('local_raw', engine, if_exists='append', index=False)
            print("Data successfully written to the 'local_raw' table.")
        except Exception as e:
            print("Error:", e)

    else:
        print("Failed to retrieve weather data for", city)
