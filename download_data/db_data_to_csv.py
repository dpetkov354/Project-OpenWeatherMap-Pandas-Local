import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection properties
db_password = "960352871454Vv!"
db_user = "postgres"
db_host = "localhost"
db_port = "5432"
db_name = "local_openweather_db"
postgres_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create a database connection engine
engine = create_engine(postgres_url)

# SQL query to fetch data
raw_data = ["SELECT * FROM local_raw", "raw_data"]  # Update 'your_table' with the actual table name and csv file path
city_data = ["SELECT * FROM local_city", "city_data"]
weather_data = ["SELECT * FROM local_weather", "weather_data"]
measurement_data = ["SELECT * FROM local_measurement", "measurement_data"]

all_data = [raw_data, city_data, weather_data, measurement_data]


# Function to fetch data from PostgreSQL
def fetch_data(db_data):
    try:
        # Fetch data into a pandas DataFrame
        df = pd.read_sql_query(db_data, con=engine)
        return df
    except Exception as e:
        print("Error occurred while fetching data:", str(e))


# Function to write data to a CSV file
def write_to_csv(df, file_path):
    try:
        # Write the dataframe to a CSV file
        df.to_csv(file_path, index=False)
        print("Data has been written to CSV successfully.")
    except Exception as e:
        print("Error occurred while writing to CSV:", str(e))


for data in all_data:
    # Unpack the query and file name
    query, file_name = data

    # Fetch data from PostgreSQL
    data_to_download = fetch_data(query)

    if data_to_download is not None:
        # Define the path where you want to save the CSV file
        csv_file_path = f"C:\OneDriveFolder\OneDrive - DXC Production\Desktop\Miscellaneous Files\Dimitar files\OpenWeatherMapAPI APP Local Python\\csv_data\\db_data\{file_name}.csv"
        print(csv_file_path)
        print(data_to_download)
        # Write data to CSV file
        write_to_csv(data_to_download, csv_file_path)
