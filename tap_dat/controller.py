
from local_database import LocalDatabase
from download_manager import DownloadManager

class Controller:
    
    def __init__(local_database: LocalDatabase,
                 download_manager: DownloadManager):
        self.local_database = local_database
        self.download_manager = download_manager.get_schema()
        
    def create_local_table(self):
        # what type are we getting in vs what type do we need
        schema = self.download_manager.get_schema()
        self.local_database.create_table('tap_dat_prod', )
        
    def process_batch(self):

    def run(self):
        