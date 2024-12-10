import psycopg2
import requests
import pandas as pd
from urllib.parse import urlencode

URL = 'https://data.elexon.co.uk/bmrs/api/v1/datasets/FOU2T14D'

def set_db_connection():
    conn = psycopg2.connect(dbname="erova_db", user="postgres", password="mtu12345", host="localhost")
    return conn

#Enable dynamic retrieval of data
def fetch_data(parameters=None):
    
    try:
        full_url = f"{URL}?{urlencode(parameters)}" if parameters else URL
        
        dataset = requests.get(full_url)
        dataset.raise_for_status()
        return extract_data_into_list(dataset.json())
    
    except Exception as error:
        print(f"Error when fetching data:{error}")
        return None

def fetch_new_data():
    return fetch_data()

def fetch_historical_data_in_publishDateTime_range(start_date, end_date):
    parameters = {
        "publishDateTimeFrom": start_date,
        "publishDateTimeTo": end_date
    }
    return fetch_data(parameters)

#Extract data into a list of tuples for immutability
def extract_data_into_list(data):
    dataset_records = [(record["dataset"], record["fuelType"], record["publishTime"], record["systemZone"], record["forecastDate"], record["forecastDateTimezone"], record["outputUsable"], record["biddingZone"], record["interconnectorName"], record["interconnector"]) for record in data["data"]]
        
    return dataset_records

#To ensure efficient performance for large datasets in the future - data is fetched in a specific chunk size
def fetch_data_in_chunks(dataset):
    
    chunk_size = 100
    for chunk in range(0, len(dataset), chunk_size):
        yield dataset[chunk:chunk + chunk_size]

#Preprocess to avoid duplicates.
def deduplicate_data(dataset):
        
    df = pd.Series(dataset)
    deduplicated_data = df.drop_duplicates() 
    return deduplicated_data      
    
def insert_into_database(cursor, dataset_records):
    
    sql_query = """INSERT INTO erova_records (dataset, fuelType, publishTime, systemZone, forecastDate, forecastDateTimezone, outputUsable, biddingZone, interconnectorName, interconnector) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    cursor.executemany(sql_query, dataset_records)


def perform_each_task():
    
    new_data = fetch_new_data()
    
    try:
        conn = set_db_connection()
        
        with  conn.cursor() as cur:
            
            for chunk in fetch_data_in_chunks(new_data):
                
                deduplicated_data = deduplicate_data(chunk)
                insert_into_database(cur, deduplicated_data)
            
        print("Tasks completed!")
        conn.commit()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error with task management process: {error}")
        
if __name__ == '__main__':
    perform_each_task()



