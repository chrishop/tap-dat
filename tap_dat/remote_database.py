"""
gets table details from database
    SELECT *
    FROM TAP_SCHEMA.columns
    WHERE table_name='dr1.master'
    ORDER BY column_order

"""

from astroquery.utils.tap import TapPlus
from numpy.ma.core import MaskedConstant
from typing import List

class RemoteDatabase:
    
    def __init__(self, url: str):
        self.client = TapPlus(url)
        
    def get_batch(self, table_name: str, id_name: str,
                  min: int, max: int, columns='*') -> List[dict]:
        return self.query(f"SELECT {columns} FROM {table_name} \
                           WHERE {id_name}>={min} \
                           AND {id_name}<{max} \
                           ORDER BY {id_name}")

    def get_schema(self, table_name, columns="*"):
        return self.query(f"SELECT {columns} \
                    FROM TAP_SCHEMA.columns \
                    WHERE table_name='{table_name}' \
                    ORDER BY column_order")

    def query(self, adql_query: str) -> List[dict]:
        job = self.client.launch_job_async(adql_query)
        job.wait_for_job_end()
        return RemoteDatabase.__table_to_dicts(job.get_results())

    @staticmethod
    def __table_to_dicts(table) -> List[dict]:
        return list(
            map(
               lambda row: RemoteDatabase.__row_to_dict(table.colnames, row),
               table 
            )
        )

    @staticmethod
    def __row_to_dict(column_names, row):
        return dict(zip(column_names, RemoteDatabase.__unmask_row(row)))

    @staticmethod
    def __unmask_row(row):
        return list(map(RemoteDatabase.__unmask_item, row))

    @staticmethod
    def __unmask_item(item):
        if isinstance(item, str) and item == '': return 'NULL'
        if isinstance(item, MaskedConstant): return 'NULL'
        return item