from database.databaseconnection import DatabaseConnector

class DatabaseManager:
    def __init__(self, dbname, user, password, host):
        
        DatabaseConnector(dbname, user, password, host).connect()
        self.conn = DatabaseConnector().conn

    def insert_fou_records(self, records):
        
        sql_query = """INSERT INTO erova_fou (dataset, fuelType, publishTime, systemZone, forecastDate, forecastDateTimezone, outputUsable, biddingZone, interconnectorName, interconnector) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        self.execute_query(sql_query, records)
        
    def insert_nou_records(self, records):
        
        sql_query = """INSERT INTO erova_nou (dataset, fuelType, publishTime, systemZone, forecastDate, forecastDateTimezone, outputUsable) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        
        self.execute_query(sql_query, records)  
    def insert_uou_records(self, records):
        
        sql_query = """INSERT INTO erova_uou (dataset, fuelType, nationalGridBmUnit, bmUnit, publishTime, week, year, outputUsable) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        
        self.execute_query(sql_query, records)
        
    def execute_query(self, query, records=None):
        with self.conn.cursor() as cur:
            cur.executemany(query, records)
        self.conn.commit()
        