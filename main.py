from dotenv import load_dotenv
from datasetmanagement.database import DatabaseManager
from datasetmanagement.datafetch import DataFetcher
from datasetmanagement.taskmanagement import TaskManager

import os

load_dotenv()


if __name__ == '__main__':
    db_manager = DatabaseManager(os.environ['POSTGRES_DB'], os.environ['POSTGRES_USR'], os.environ['POSTGRES_PWD'], os.environ['POSTGRES_LHST'])
    data_fetcher = DataFetcher('https://data.elexon.co.uk/bmrs/api/v1/datasets/FOU2T14D')

    #db_manager.connect()
    task_manager = TaskManager(db_manager, data_fetcher)
    task_manager.perform_tasks()