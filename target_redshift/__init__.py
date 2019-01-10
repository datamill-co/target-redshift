from singer import utils
import psycopg2

from singer_target_postgres.postgres import PostgresTarget
from singer_target_postgres import target_tools

REQUIRED_CONFIG_KEYS = [
    'postgres_database'
]

def main(config, input_stream=None):
    with psycopg2.connect(
            host=config.get('redshift_host', 'localhost'),
            port=config.get('redshift_port', 5432),
            dbname=config.get('redshift_database'),
            user=config.get('redshift_username'),
            password=config.get('redshift_password')
    ) as connection:
        postgres_target = PostgresTarget(
            connection,
            postgres_schema=config.get('redshift_schema', 'public'))

        if input_stream:
            target_tools.stream_to_target(input_stream, postgres_target, config=config)
        else:
            target_tools.main(postgres_target)

def cli():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    main(args.config)
