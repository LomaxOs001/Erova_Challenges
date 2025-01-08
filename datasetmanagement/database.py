import psycopg2

class DatabaseManager:
    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host)

    def insert_records_in_database(self, records):
        
        sql_query = """INSERT INTO erova_records (dataset, fuelType, publishTime, systemZone, forecastDate, forecastDateTimezone, outputUsable, biddingZone, interconnectorName, interconnector) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        with self.conn.cursor() as cur:
            cur.executemany(sql_query, records)
        self.conn.commit()
        