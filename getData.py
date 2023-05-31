import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

current_date = datetime.today()
# Calculate the date 2 days before the current date
begin_date = current_date - timedelta(days=2)
begin_timestamp = int(begin_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

# Calculate the date 1 day before the current date
end_date = current_date - timedelta(days=1)
end_timestamp = int(end_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

airport = 'KATL' # Bandara Udara Internasional Hartsfield-Jackson Atlanta

url = f'https://opensky-network.org/api/flights/departure?airport={airport}&begin={begin_timestamp}&end={end_timestamp}'

print('==> Start to get data from the API <==')

response = requests.get(url, auth=('fajartirtayasa', 'OpenSky2023'))

if response.status_code == 200:
    print(f'Status Code: {response.status_code}')
    data_unclean = json.loads(response.content)
    print(f'Number of rows: {len(data_unclean)}')
    
    df = pd.DataFrame(data_unclean)
    
    # Define the schema for the Parquet file
    schema = pa.Schema.from_pandas(df)
    
    # Create a PyArrow table from the DataFrame
    table = pa.Table.from_pandas(df, schema=schema)
    
    # Define the CSV file path
    # csv_file_path = '/home/pablo/final_project/data/flight_data3.csv'
    
    # Define the Parquet file path
    parquet_file_path = '/home/pablo/final_project/data/flight_data3.parquet'
    
    # Write the table to a Parquet file
    # df.to_csv(csv_file_path, index=False)
    
    # Write the table to a Parquet file
    pq.write_table(table, parquet_file_path)
    
    # print(f"Data saved as CSV: {csv_file_path}")
    print(f"Data saved as Parquet file: {parquet_file_path}")
    
else:
    print(f'Something went wrong! Status Code: {response.status_code}')
