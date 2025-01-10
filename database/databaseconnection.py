import psycopg2

# Singleton metaclass
class DatabaseConnectorMeta(type): 
    __instances = {} 
    def __call__(cls, *args, **kwargs): 
        if cls not in cls.__instances: 
            instance = super().__call__(*args, **kwargs) 
            cls.__instances[cls] = instance 
        return cls.__instances[cls]


class DatabaseConnector(metaclass=DatabaseConnectorMeta):
    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host)