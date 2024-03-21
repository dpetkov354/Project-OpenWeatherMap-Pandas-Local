import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection properties
db_password = "PASSWORD"
db_user = "postgres"
db_host = "localhost"
db_port = "5432"
db_name = "local_openweather_db"
postgres_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# SQLAlchemy engine
engine = create_engine(postgres_url)

# Read weather data from local_raw
query = """
    SELECT DISTINCT weather_id, weather_main, weather_description
    FROM local_raw
"""

raw_df = pd.read_sql(query, engine)

# Read weather data from local_weather
query = """
    SELECT DISTINCT weather_id, weather_main, weather_description
    FROM local_weather
"""

weather_df = pd.read_sql(query, engine)

# Merge raw_df and weather_df to find new distinct values
merged_df = raw_df.merge(weather_df,
                         on=["weather_id", "weather_main", "weather_description"],
                         how='left', indicator=True)

# Display merged tables
print(merged_df)

# Filter rows that are only in raw_df
new_values_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns='_merge')

# Display the new distinct values
print(new_values_df)

new_values_df.to_sql('local_weather', engine, if_exists='append', index=False)
