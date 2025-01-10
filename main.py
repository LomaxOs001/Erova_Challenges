from dotenv import load_dotenv
import os

load_dotenv()
from database.databasemanagement import DatabaseManager
from datasetmanagement.datafetch import DataFetcher
from datasetmanagement.taskmanagement import TaskManager




if __name__ == '__main__':
    
    db_manager = DatabaseManager(
        os.environ["POSTGRES_DB"], 
        os.environ["POSTGRES_USR"], 
        os.environ["POSTGRES_PWD"], 
        os.environ["POSTGRES_LHST"])
    
    fou_data_fetcher = DataFetcher('https://data.elexon.co.uk/bmrs/api/v1/datasets/FOU2T14D')
    
    nou_data_fetcher = DataFetcher('https://data.elexon.co.uk/bmrs/api/v1/datasets/NOU2T14D')
    
    

    task_manager = TaskManager(db_manager, nou_data_fetcher)
    task_manager.perform_tasks("nou")