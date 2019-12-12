import mysql.connector
from typing import List, Any
from statements import Statements

# TODO make this even dumber and put the logic infront of it, to make
# to make logic more testable.

class LocalDatabase:
    
    def __init__(self, the_host, the_username, the_password):
        self.database = mysql.connector.connect(
            host=the_host,
            username=the_username,
            password=(the_password)
        )
        
        self.cursor = self.database.cursor()
    
    def create_table(self, table_name: str, columns: List[dict]):
        """columns, a list of dicts, each dict contains a 'name' and 'type'"""
        self.cursor.execute(Statements.create(table_name, columns))
      
    def insert(self, table_name: str, column_names: List[str], row: dict):
        self.cursor.execute(Statements.insert(table_name, column_names), row)
        self.database.commit()
       
    def insert_multiple(self, table_name: str,
                        column_names: List[str],
                        rows: List[dict]):
        self.cursor.executemany(Statements.insert(table_name,column_names), rows)
        self.database.commit()
        