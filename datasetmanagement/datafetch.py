import requests
from urllib.parse import urlencode

class DataFetcher:
    def __init__(self, api_url):
        self.api_url = api_url
        
    def __fetch_data(self, parameters=None):
        
        try:
            
            full_url = f"{self.api_url}?{urlencode(parameters)}" if parameters else self.api_url
            response = requests.get(full_url)
            response.raise_for_status()
            return response.json()
        
        except Exception as e:
            print(f"Error fetching data: {e}")
            return []
    
    def fetch_new_data(self):
        return self.__fetch_data()
    
    def fetch_historical_data_in_publishDateTime_range(self, start_date, end_date):
        
        params = {"publishDateTimeFrom": start_date, "publishDateTimeTo": end_date}
        
        return self.__fetch_data(params)