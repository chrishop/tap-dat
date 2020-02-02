
import click

@click.group()
def cli():
    pass

@click.command()
@click.option('-u', '--user', 'user')
@click.option('-p', '--password', 'password')
@click.argument('db-name')
def init(db_name, user, password):
    click.echo(f"init {db_name}, {user}, {password}")
    
@click.command()
@click.argument('local-db-name')
@click.argument('tap-url')
@click.argument('table-name')
@click.argument('id-column-name')
@click.option('-b', '--batch-size', default=10000)
def download(local_db_name, tap_url, table_name, id_column_name, batch_size):
    click.echo(f"local_db_name: {local_db_name}, tap_url: {tap_url}, table_name: {table_name}, id_column_name: {id_column_name}, batch_size: {batch_size}")
    
cli.add_command(init)
cli.add_command(download)
    
if __name__ == '__main__':
    cli()
