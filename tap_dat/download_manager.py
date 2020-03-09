from typing import List
from tap_dat.remote_database import RemoteDatabase

class DownloadManager:

    def __init__(self, the_url: str, table_name: str, id_name: str): 
        # do we need min max surely we should just check the table (like a torrent check how much it has downloaded)
        self.remote = RemoteDatabase(the_url)
        self.table_name = table_name
        self.id_name = id_name

    def set_batches(self, the_min: int, the_max: int, batch_size: int):
        self.query_queue = DownloadManager.__make_query_queue(the_min,
                                                              the_max,
                                                              batch_size)

    def next(self):
        if self.query_queue == []:
            return []
        a_min, a_max = self.query_queue.pop(0)
        return self.remote.get_batch(self.table_name,
                                     self.id_name,
                                     a_min, a_max)

    def get_schema(self):
        return self.remote.get_schema(self.table_name)

    # TODO there should be some functional way of doing this
    @staticmethod
    def __make_query_queue(the_min: int, the_max: int, batch_size: int):
        query_queue = []
        row_number = the_min
        while row_number < the_max:
            query_queue.append([row_number, row_number + (batch_size -1)])
            row_number += batch_size
        return query_queue
