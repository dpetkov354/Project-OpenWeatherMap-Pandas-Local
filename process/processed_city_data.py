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

# Read city data from local_raw
query = """
    SELECT DISTINCT city_id, city_name, longitude, latitude
    FROM local_raw
"""

raw_df = pd.read_sql(query, engine)

# Read city data from local_city
query = """
    SELECT DISTINCT city_id, city_name, longitude, latitude
    FROM local_city
"""

city_df = pd.read_sql(query, engine)

# Merge raw_df and city_df to find new distinct values
merged_df = raw_df.merge(city_df, on=['city_id', 'city_name', 'longitude', 'latitude'], how='left', indicator=True)

# Filter rows that are only in raw_df
new_values_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns='_merge')

new_values_df.to_sql('local_city', engine, if_exists='append', index=False)
