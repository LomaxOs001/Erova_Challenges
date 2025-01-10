from utils.dataprocess import DataProcessor

class TaskManager:
    def __init__(self, db_manager, data_fetcher):
        
        self.db_manager = db_manager
        self.data_fetcher = data_fetcher
        
    def perform_tasks(self, data_type):
        new_data = self.data_fetcher.fetch_new_data()
        
        match data_type:
            case "fou":
                DataProcessor.convert_fou_dataset_into_list(new_data)
            case "nou":
                DataProcessor.convert_nou_dataset_into_list(new_data)
            case "uou":
                DataProcessor.convert_ouo_data_into_list(new_data)
                
        converted_data = DataProcessor.get_dataset_records()
        deduplicate_data = DataProcessor.remove_duplicates(converted_data)
        
        
        for chunked_data in DataProcessor.fetch_data_in_chunks(deduplicate_data):  
            match data_type:
                case "fou":
                    self.db_manager.insert_records(chunked_data, "fou")
                case "nou":
                    self.db_manager.insert_records(chunked_data, "nou")
                case "uou":
                    self.db_manager.insert_records(chunked_data, "uou")