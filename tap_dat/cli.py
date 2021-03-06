
import click
from tap_dat.local_database import LocalDatabase
from tap_dat.remote_database import RemoteDatabase
from tap_dat.controller import Controller
from tap_dat.default_config import default_json
import json
import os
import traceback


@click.group()
def cli():
    pass
    
@click.command()
def init():
    with open(os.getcwd() + "/tap-dat.conf", "w+") as new_config:
        new_config.write(default_json)
    print("new config file created")


@click.command()
@click.argument('config-file-path')
def run(config_file_path):
    config = None
    try:
        with open(config_file_path, 'r') as config_file:
            config = json.loads(config_file.read())
    except Exception:
        print("Error reading config, check syntax and keys")
   
    controller = Controller.getInstance()
    # setup remote connection
    try:
        controller.setup_remote_connection(
            config["remote_url"],
            config["remote_table"],
            config["remote_table_id"]
        )
        print("remote setup done!")
    except Exception:
        print("Issue setting up remote connection")
    
    # setup local connection
    
    try:
        controller.setup_local_connection(
            config["local_host"],
            config["local_user"],
            config["local_password"],
            config["local_db_name"],
            config["local_table_name"]
        )
        print("local setup done!")
    except Exception:
        print("Issue setting up local connection")
       
    # start downloading
    print("starting download")
    try:
        controller.download(
            config["remote_table_id_min"],
            config["remote_table_id_max"],
            config["download_batch_size"]
        )
        print("downloading finished!")
    except Exception as e:
        traceback.print_exc()
        print("error in downloading")
        
cli.add_command(init)
cli.add_command(run)

if __name__ == '__main__':
    cli()
