import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection properties
db_password = "960352871454Vv!"
db_user = "postgres"
db_host = "localhost"
db_port = "5432"
db_name = "local_openweather_db"
postgres_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# SQLAlchemy engine
engine = create_engine(postgres_url)

# TASK 2.1
query_2_1 = """
            SELECT DISTINCT w.weather_main
            FROM (          
                SELECT *
                FROM local_measurement
                WHERE dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10'
                ) m
            JOIN local_weather w ON m.weather_id = w.weather_id
            WHERE m.dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10'
"""

print("2.1 Distinct values of conditions (rain/snow/clear/…) for a given period.")
query_2_1_df = pd.read_sql(query_2_1, engine)
print(f"{query_2_1_df}\n")

# TASK 2.2.1
query_2_2_1 = """
                SELECT m.city_id, c.city_name, w.weather_main, COUNT(*) AS frequency
                FROM (          
                        SELECT *
                        FROM local_measurement
                        WHERE dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10'
                	  ) m
                JOIN local_city c ON m.city_id = c.city_id
                JOIN local_weather w ON m.weather_id = w.weather_id
                GROUP BY m.city_id, c.city_name, w.weather_main
                ORDER BY frequency DESC;
            """

print("2.2_1 Most common weather conditions in a certain period of time per city;")
query_2_2_1_df = pd.read_sql(query_2_2_1, engine)
print(f"{query_2_2_1_df}\n")


# TASK 2.2.1
query_2_2_2 = """
                SELECT m.city_id, c.city_name, w.weather_main, COUNT(*) AS frequency
                FROM (          
                        SELECT *
                		FROM local_measurement
                		WHERE dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10'
                	  ) m
                JOIN local_city c ON m.city_id = c.city_id
                JOIN local_weather w ON m.weather_id = w.weather_id
                GROUP BY m.city_id, c.city_name, w.weather_main
                ORDER BY frequency DESC
                LIMIT 1;
            """

print("2.2_2 Most common weather conditions in a certain period of time per city;")
query_2_2_2_df = pd.read_sql(query_2_2_2, engine)
print(f"{query_2_2_2_df}\n")

# TASK 2.3
query_2_3 = """
                SELECT m.city_id, c.city_name, CAST(AVG(m.temperature) AS DECIMAL(10,2)) AS avg_temperature
                FROM (
                	  SELECT *
                	  FROM local_measurement
                	  WHERE dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10'
                	  ) m
                JOIN local_city c ON m.city_id = c.city_id
                GROUP BY m.city_id, c.city_name;
            """

print("2.3 Temperature averages observed in a certain period per city;")
query_2_3_df = pd.read_sql(query_2_3, engine)
print(f"{query_2_3_df}\n")

# TASK 2.4
query_2_4 = """
                SELECT m.city_id, c.city_name, CAST(MAX(m.temp_max) AS DECIMAL(10,2)) AS max_temperature
                FROM (
                      SELECT *
                      FROM local_measurement
                	  WHERE dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10'
                      ) m
                JOIN local_city c ON m.city_id = c.city_id
                GROUP BY m.city_id, c.city_name
                ORDER BY max_temperature DESC
                LIMIT 1;
            """

print("2.4 City that had the highest absolute temperature in a certain period of time;")
query_2_4_df = pd.read_sql(query_2_4, engine)
print(f"{query_2_4_df}\n")

# TASK 2.5
query_2_5 = """
                WITH daily_variation_cte AS (
                    SELECT 
                        city_id, 
                        dt::date AS measurement_date,
                        MAX(temp_max) - MIN(temp_min) AS daily_variation
                    FROM 
                        local_measurement
                    WHERE 
                        dt::time >= sys_sunrise::time
                        AND dt::time <= sys_sunset::time
                        AND dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10'
                    GROUP BY 
                        city_id, 
                        dt::date
                )
                SELECT 
                    dvc.city_id, 
                    c.city_name,
                    dvc.daily_variation 
                FROM daily_variation_cte dvc
                JOIN local_ity c ON dvc.city_id = c.city_id
                ORDER BY dvc.daily_variation DESC
                LIMIT 1;
            """

print("2.5 City that had the highest daily temperature variation in a certain period of time;")
query_2_5_df = pd.read_sql(query_2_5, engine)
print(f"{query_2_5_df}\n")

# TASK 2.
query_2_6 = """
                SELECT m.measurement_id, m.city_id, c.city_name, 
                	   CAST(MAX(m.wind_speed) AS DECIMAL(10,2)) AS max_wind_speed
                FROM (
                	  SELECT *
                	  FROM local_measurement
                	  WHERE dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10'
                	  ) m
                JOIN local_city c ON m.city_id = c.city_id
                GROUP BY m.measurement_id, m.city_id, c.city_name
                ORDER BY max_wind_speed DESC
                LIMIT 1;
            """

print("2.6 City that had the strongest wing in a certain period of time.")
query_2_6_df = pd.read_sql(query_2_6, engine)
print(f"{query_2_6_df}\n")

