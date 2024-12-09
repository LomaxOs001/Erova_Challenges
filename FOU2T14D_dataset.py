import psycopg2
import requests
import pandas as pd


def set_db_connection():
    conn = psycopg2.connect(dbname="evora_task_db", user="postgres", password="mtu12345", host="localhost")
    return conn
    
#fetch dataset using GET request
def fetch_new_data():
    
    URL = 'https://data.elexon.co.uk/bmrs/api/v1/datasets/FOU2T14D?format=json'
    try:
        
        dataset = requests.get(URL)
        return extract_data_into_list(dataset.json())
    
    except Exception as e:
        print(f"Error when fetch data:{e}")

#Extract data into a list of tuples for immutability
def extract_data_into_list(data):
    records = [(record["dataset"], record["fuelType"], record["publishTime"], record["systemZone"], record["forecastDate"], record["forecastDateTimezone"], record["outputUsable"], record["biddingZone"], record["interconnectorName"], record["interconnector"]) for record in data["data"]]
        
    return records

#To ensure efficient performance for large datasets in the future - data is fetched in a specific chunk size
def fetch_data_in_chunks(dataset):
    
    chunk_size = 100
    for chunk in range(0, len(dataset), chunk_size):
        yield dataset[chunk:chunk + chunk_size]

#Preprocess to avoid duplicates.
def preprocess_data(data):
        
    df = pd.Series(data)
    deduplicated_data = df.drop_duplicates() 
    return deduplicated_data      
    
def insert_into_evora_database(cursor, dataset_records):
    
    sql_query = """INSERT INTO evoraData (dataset, fuelType, publishTime, systemZone, forecastDate, forecastDateTimezone, outputUsable, biddingZone, interconnectorName, interconnector) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    cursor.executemany(sql_query, dataset_records)

#Perform the task of fetching and inserting into  the database
def perform_evora_task():
    
    new_data = fetch_new_data()
    
    try:
        conn = set_db_connection()
        
        with  conn.cursor() as cur:
            
            for chunk in fetch_data_in_chunks(new_data):
                
                deduplicated_data = preprocess_data(chunk)
                insert_into_evora_database(cur, deduplicated_data)
            
        print("Tasks completed!")
        conn.commit()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error with task management process: {error}")
        
if __name__ == '__main__':
    perform_evora_task()



