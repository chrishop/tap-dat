
from tap_dat.local_database import LocalDatabase
from tap_dat.download_manager import DownloadManager
from tap_dat.transformer import Transformer
import traceback

class Controller:
    __instance = None
    
    @staticmethod
    def getInstance():
        if Controller.__instance == None:
            Controller()
        return Controller.__instance
    
    def __init__(self):
        if Controller.__instance != None:
            raise("There should only be one of these >:(")
        else:
            self.url = None
            self.remote_table = None
            self.remote_table_id = None
            
            self.host = None
            self.user = None
            self.db_name = None
            self.local_table_name = None
            
            self.min = None
            self.max = None
            self.batch_size = None
            
            self.remote_setup = False
            self.local_setup = False
            
            Controller.__instance = self
            
            
    def setup_remote_connection(self, url, remote_table, table_id):
        self.url = url
        self.remote_table = remote_table
        self.remote_table_id = table_id
        try:
            self.download_manager = DownloadManager(self.url, self.remote_table, self.remote_table_id)
            self.remote_schema = Transformer.transform_schema(self.download_manager.get_schema())
            self.remote_setup = True
        except Exception:
            traceback.print_exc()        


    def setup_local_connection(self, host, user, password, db_name, table_name):
        self.host = host
        self.user = user
        self.db_name = db_name
        self.local_table_name = table_name
        if self.remote_setup:
            try:

                self.local_database = LocalDatabase(self.host,
                                                    self.user,
                                                    password,
                                                    self.db_name)
                self.local_database.create_table(self.local_table_name, self.remote_schema)
                self.local_setup = True
            except Exception:
                traceback.print_exc()
        else:
            raise AttributeError("You first need to setup the remote connection")

    

    def download(self, min, max, batch_size):
        self.min = min
        self.max = max
        self.batch_size = batch_size
        if self.remote_setup and self.local_setup:
            self.download_manager.set_batches(self.min, self.max, self.batch_size)
        is_finished = False
        batch = None
        while not is_finished:
            batch = self.download_manager.next()
            if batch == []:
                is_finished = True
            else:
                self.local_database.insert_multiple(self.local_table_name,
                                                    Transformer.transform(batch))

    def info(self):
        return f"""
Remote Connection
    URL: {self.url}
    Remote Table: {self.remote_table}
    Remote Table Id: {self.remote_table_id}
    
Local Connection
    Host: {self.host}
    User: {self.user}
    Database: {self.db_name}
    Table Name: {self.local_table_name}
    
Batch
    Starting ID: {self.min}
    Ending ID: {self.max}
    Batch Size: {self.batch_size}
"""