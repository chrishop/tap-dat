import mysql.connector
from mysql.connector.errors import ProgrammingError
from typing import List, Any
from tap_dat.statements import Statements


#TODO improve error catching
#TODO parse the primary key
class LocalDatabase:
    
    def __init__(self, the_host, the_username, the_password, the_database):
        self.database = mysql.connector.connect(
            host=the_host,
            user=the_username,
            passwd=the_password,
            database=the_database
        )
        
        self.cursor = self.database.cursor()
    
    def create_table(self, table_name: str, columns: List[dict]):
        """columns, a list of dicts, each dict contains a 'column_name' and 'datatype'"""
        if not self.already_exists(table_name):
            self.cursor.execute(Statements.create(table_name, columns))
      
    def insert(self, table_name: str, row: dict):
        self.cursor.execute(Statements.insert(table_name, list(row.keys())), row)
        self.database.commit()
       
    def insert_multiple(self, table_name: str, rows: List[dict]):
        #TODO this is slow, use execute many when you have time.
        for row in rows:
            self.cursor.execute(Statements.insert(table_name, list(row.keys())), row)

        self.database.commit()
        
    def drop_table(self, table_name):
        return self.cursor.execute(f"DROP TABLE {table_name};")
    
    #TODO need to write this to return a dict
    def query(self, query: str):
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def already_exists(self, table_name):
        try:
            self.query(f"SELECT * FROM {table_name}")
            return True
        except Exception:
            return False
        