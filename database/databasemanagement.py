from database.databaseconnection import DatabaseConnector

class DatabaseManager:
    def __init__(self, dbname, user, password, host):
        
        DatabaseConnector(dbname, user, password, host).connect()
        self.conn = DatabaseConnector().conn

    def insert_records(self, records, db_table_type):
        sql_query = ""
        
        match db_table_type:
            case "fou":
                sql_query = """INSERT INTO erova_fou (dataset, fuelType, publishTime, systemZone, forecastDate, forecastDateTimezone, outputUsable, biddingZone, interconnectorName, interconnector) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                
            case "nou":
                sql_query = """INSERT INTO erova_nou (dataset, publishTime, systemZone, forecastDate, forecastDateTimezone, outputUsable) VALUES (%s, %s, %s, %s, %s, %s)"""
            
            case "uou":
                sql_query = """INSERT INTO erova_uou (dataset, fuelType, nationalGridBmUnit, bmUnit, publishTime, week, year, outputUsable) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        
        with self.conn.cursor() as cur:
            cur.executemany(sql_query, records)
        self.conn.commit()
        
        