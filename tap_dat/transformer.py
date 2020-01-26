from numpy.ma.core import MaskedConstant
from typing import List

class Transformer:
    
    @staticmethod
    def transform(rows: List[dict]) -> List[dict]:
        return list(map(Transformer.__transform_row, rows))

    @staticmethod
    def transform_schema(schema: List[dict]) -> List[dict]:
        return list(map(Transformer.__transform_schema_row, schema))
    
    # I don't think there is anyway of doing this immutable
    @staticmethod
    def __transform_schema_row(row: dict) -> dict:
        if row['datatype'] == 'CHAR': 
            if row['size'] >= 255:
                row['datatype'] = Transformer.__attach_size(row['datatype'], '255')
            else:
                row['datatype'] = Transformer.__attach_size(row['datatype'], row['size'])
        return row
        
    # size may be some kind of integer
    @staticmethod
    def __attach_size(datatype: str, size: str) -> str:
        return f"{datatype}({size})"


    
    @staticmethod
    def __transform_row(row: dict) -> dict:
        return Transformer.__values_to_strings(
            Transformer.__remove_unused_keys(row)
        )
        
    @staticmethod
    def __values_to_strings(row: dict) -> dict:
        return dict(
            [(key, str(value)) for (key, value) in row.items()]
        )
    
    @staticmethod
    def __remove_unused_keys(row: dict) -> dict:
        return dict(
            [(key, value) for (key, value) in row.items() 
             if not isinstance(value, MaskedConstant)]
        )
        