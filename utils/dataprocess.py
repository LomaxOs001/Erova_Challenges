import pandas as pd

class DataProcessor:
    @staticmethod
    def extract_data_into_list(data):
        
        dataset_records = [(record["dataset"], record["fuelType"], record["publishTime"], record["systemZone"], record["forecastDate"], record["forecastDateTimezone"], record["outputUsable"], record["biddingZone"], record["interconnectorName"], record["interconnector"]) for record in data["data"]]
        
        return dataset_records

    @staticmethod
    def remove_duplicates(data):
        df = pd.Series(data)
        return df.drop_duplicates().tolist()

    @staticmethod
    def fetch_data_in_chunks(dataset, chunk_size = 100):
    
        for chunk in range(0, len(dataset), chunk_size):
            yield dataset[chunk:chunk + chunk_size]