from utils.dataprocess import DataProcessor

class TaskManager:
    def __init__(self, db_manager, data_fetcher):
        
        self.db_manager = db_manager
        self.data_fetcher = data_fetcher
        
    def perform_tasks(self):
        new_data = self.data_fetcher.fetch_new_data()
        extracted_data = DataProcessor.extract_data_into_list(new_data)
        deduplicate_data = DataProcessor.remove_duplicates(extracted_data)
        
        for chunked_data in DataProcessor.fetch_data_in_chunks(deduplicate_data):  
            self.db_manager.insert_records_in_database(chunked_data)
        