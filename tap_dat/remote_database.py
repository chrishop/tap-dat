"""
gets table details from database
    SELECT *
    FROM TAP_SCHEMA.columns
    WHERE table_name='dr1.master'
    ORDER BY column_order

"""

from astroquery.utils.tap import TapPlus
from astropy.table import Table
from typing import List

class RemoteDatabase:
    
    def __init__(self, url: str):
        self.client = TapPlus(url)
        
    def query(self, adql_query: str) -> List[dict]:
        job = self.client.launch_job_async(adql_query)
        job.wait_for_job_end()
        return RemoteDatabase.table_to_dict(job.get_results())
    
    @staticmethod
    def table_to_dict(table) -> List[dict]:
        return list(map(lambda row: dict(zip(table.colnames, row)), table))
    
