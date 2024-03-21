import psycopg2

# Database connection parameters
dbname = 'local_openweather_db'
user = 'postgres'
password = 'PASSWORD'
host = 'localhost'
port = '5432'

# SQL queries
queries = [
    """
    CREATE TABLE IF NOT EXISTS local_raw (
        measurement_id SERIAL PRIMARY KEY,
        city_id INTEGER,
        city_name VARCHAR(255),
        longitude VARCHAR(255),
        latitude VARCHAR(255),
        weather_id INTEGER,
        weather_main VARCHAR(255),
        weather_description VARCHAR(255),
        weather_icon VARCHAR(255),
        base VARCHAR(255),
        temperature FLOAT,
        feels_like FLOAT,
        temp_min FLOAT,
        temp_max FLOAT,
        pressure INTEGER,
        humidity INTEGER,
        visibility INTEGER,
        wind_speed FLOAT,
        wind_deg INTEGER,
        clouds_all INTEGER,
        dt TIMESTAMP,
        sys_type INTEGER,
        sys_id INTEGER,
        sys_country VARCHAR(255),
        sys_sunrise TIMESTAMP,
        sys_sunset TIMESTAMP,
        timezone INTEGER,
        cod VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS local_city (
        city_id INTEGER PRIMARY KEY,
        city_name VARCHAR(255),
        longitude VARCHAR(255),
        latitude VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS local_weather (
        weather_id INTEGER PRIMARY KEY,
        weather_main VARCHAR(255),
        weather_description VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS local_measurement (
        measurement_id INTEGER PRIMARY KEY,
        city_id INTEGER,
        weather_id INTEGER,
        temperature FLOAT,
        temp_min FLOAT,
        temp_max FLOAT,
        wind_speed FLOAT,
        dt TIMESTAMP,
        sys_country VARCHAR(255),
        sys_sunrise TIMESTAMP,
        sys_sunset TIMESTAMP,
        timezone INTEGER,
        FOREIGN KEY (city_id) REFERENCES local_city(city_id),
        FOREIGN KEY (weather_id) REFERENCES local_weather(weather_id)
    );
    """
]

# Connect to the database
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
cursor = conn.cursor()

# Execute each query
for query in queries:
    cursor.execute(query)

# Commit the transaction and close the connection
conn.commit()
conn.close()

print("Tables created successfully.")
