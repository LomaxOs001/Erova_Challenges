import psycopg2
import requests
import json


def set_db_connection():
    
    c = psycopg2.connect(dbname="evora_task_db", user="postgres", password="mtu12345", host="localhost")
    return c
    
    
def insert_into_evora_dataset(dataset_list):
    
    sql_query = """INSERT INTO evora_dataset(id, name)
             VALUES(%s, %s) RETURNING *;"""

    try:
        conn = set_db_connection()
            
        with  conn.cursor() as cur:
                
            cur.executemany(sql_query, dataset_list)
            
        conn.commit()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        
if __name__ == '__main__':
    insert_into_evora_dataset([('As', 'Evora Energy'), ('Bd', 'Evora Business'), ('Sk', 'Evora Customer Service')])
        



