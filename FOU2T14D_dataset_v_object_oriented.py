import psycopg2
import requests
import pandas as pd
from urllib.parse import urlencode

class DatabaseManager:
    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host)

    def insert_records_in_database(self, records):
        
        sql_query = """INSERT INTO erova_records (dataset, fuelType, publishTime, systemZone, forecastDate, forecastDateTimezone, outputUsable, biddingZone, interconnectorName, interconnector) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        with self.conn.cursor() as cur:
            cur.executemany(sql_query, records)
        self.conn.commit()

class DataFetcher:
    def __init__(self, api_url):
        self.api_url = api_url

    def fetch_data(self, parameters=None):
        try:
            full_url = f"{self.api_url}?{urlencode(parameters)}" if parameters else self.api_url
            response = requests.get(full_url)
            response.raise_for_status()
            return response.json()["data"]
        except Exception as e:
            print(f"Error fetching data: {e}")
            return []

class DataProcessor:
    def extract_data_into_list(data):
        
        dataset_records = [(record["dataset"], record["fuelType"], record["publishTime"], record["systemZone"], record["forecastDate"], record["forecastDateTimezone"], record["outputUsable"], record["biddingZone"], record["interconnectorName"], record["interconnector"]) for record in data["data"]]
        
        return dataset_records

    def remove_duplicates(data):
        df = pd.Series(data)
        return df.drop_duplicates().tolist()

    def fetch_data_in_chunks(dataset,chunk_size = 100):
        for chunk in range(0, len(dataset), chunk_size):
            yield dataset[chunk:chunk + chunk_size]

class TaskManager:
    def __init__(self, db_manager, data_fetcher, data_processor):
        self.db_manager = db_manager
        self.data_fetcher = data_fetcher
        self.data_processor = data_processor

    def perform_tasks(self):
        new_data = self.data_fetcher.fetch_data()
        records = self.data_processor.extract_data(new_data)
        deduplicated_data = self.data_processor.deduplicate_data(records)

        for chunk in self.data_processor.chunk_data(deduplicated_data):
            self.db_manager.insert_records_into_database(chunk)

