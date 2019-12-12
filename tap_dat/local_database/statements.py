from typing import List, Any

class Statements:
   
    @staticmethod
    def create(table_name: str, columns: List[dict]) -> str:
        columns_as_string = Statements.__generate_name_type_string(columns)
        return f"CREATE TABLE {table_name} ({columns_as_string})"
    
    @staticmethod
    def insert(table_name: str, column_names: List[str]) -> str:
        separated_column_names = Statements.__comma_separate(column_names)
        insert_values = Statements.__generate_insert_values(column_names)
        
        return f"INSERT INTO {table_name} ({separated_column_names}) VALUES ({insert_values})"
    
    @staticmethod
    def __generate_name_type_string(columns: List[dict]) -> str:
        return ",".join(list(map(lambda x: f"{x['name']} {x['type']}", columns)))
    
    @staticmethod
    def __comma_separate(columns_names: List[str]) -> str:
        return ",".join(columns_names)
    
    @staticmethod
    def __generate_insert_values(column_names: List[str]) -> str:
        return ",".join(list(map(
            lambda column: f"%({column})s",
            column_names)))