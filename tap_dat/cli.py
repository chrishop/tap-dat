
import click
from tap_dat.local_database import LocalDatabase
from tap_dat.remote_database import RemoteDatabase
from tap_dat.controller import Controller


@click.group()
def cli():
    pass


@click.command()
def info():
    click.echo(f"info")


@click.command()
@click.option('-h', '--host', default='localhost')
@click.option('-u', '--user')
@click.option('-p', '--password')
@click.argument('db-name')
@click.argument('table-name')
def local(host, user, password, db_name, table_name):
    click.echo(f"local {host}, {user}, {password}, {db_name}, {table_name}")


@click.command()
@click.argument('url')
@click.argument('remote-table')
@click.argument('table-id')
def remote(url, remote_table, table_id):
    click.echo(f"remote {url}, {remote_table}, {table_id}")


@click.command()
@click.argument('min')
@click.argument('max')
@click.option('-b', '--batch-size', default=10000)
def download(min, max, batch_size):
    click.echo(f"{min}, {max}, {batch_size}")


cli.add_command(info)
cli.add_command(local)
cli.add_command(remote)
cli.add_command(download)


if __name__ == '__main__':
    cli()
