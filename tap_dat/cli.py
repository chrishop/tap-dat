
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
def info():
    print(Controller.getInstance().info())

@click.command()
@click.argument('url')
@click.argument('remote-table')
@click.argument('table-id')
def remote(url, remote_table, table_id):
    Controller.getInstance().setup_remote_connection(url, remote_table, table_id)
    print(Controller.getInstance())

@click.command()
@click.option('-h', '--host', default='localhost')
@click.option('-u', '--user')
@click.option('-p', '--password')
@click.argument('db-name')
@click.argument('table-name')
def local(host, user, password, db_name, table_name):
    try:
        Controller.getInstance().setup_local_connection(host, user, password, db_name, table_name)
    except AttributeError as e:
        click.echo(e)
    except Exception:
        print("unknown error setting up local database")

@click.command()
@click.argument('min')
@click.argument('max')
@click.option('-b', '--batch-size', default=10000)
def download(min, max, batch_size):
    Controller.getInstance().download(min, max, batch_size)
    
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
        
        

cli.add_command(info)
cli.add_command(local)
cli.add_command(remote)
cli.add_command(download)

cli.add_command(init)
cli.add_command(run)

if __name__ == '__main__':
    cli()
