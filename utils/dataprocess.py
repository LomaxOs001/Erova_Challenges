import pandas as pd

class DataProcessor:
    
    __dataset_records = []
    
    @classmethod
    def convert_fou_dataset_into_list(cls, data):
        
        cls.__dataset_records = [(record["dataset"], record["fuelType"], record["publishTime"], record["systemZone"], record["forecastDate"], record["forecastDateTimezone"], record["outputUsable"], record["biddingZone"], record["interconnectorName"], record["interconnector"]) for record in data["data"]]
    
    @classmethod
    def convert_nou__dataset_into_list(cls, data):
        
        cls.__dataset_records = [(record["dataset"], record["fuelType"], record["publishTime"], record["systemZone"], record["forecastDate"], record["forecastDateTimezone"], record["outputUsable"]) for record in data["data"]]
   
        
    @classmethod
    def convert_ouo_data_into_list(cls, data):
        
        cls.__dataset_records = [(record["dataset"], record["fuelType"], record["nationalGridBmUnit"], record["bmUnit"], record["publishTime"], record["week"], record["year"], record["outputUsable"]) for record in data["data"]]

    @classmethod
    def get_dataset_records(cls):
        return cls.__dataset_records
    
    @staticmethod
    def remove_duplicates(data):
        df = pd.Series(data)
        return df.drop_duplicates().tolist()

    @staticmethod
    def fetch_data_in_chunks(dataset, chunk_size = 100):
    
        for chunk in range(0, len(dataset), chunk_size):
            yield dataset[chunk:chunk + chunk_size]