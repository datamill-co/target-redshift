import psycopg2
import singer
from singer import utils
from target_postgres import target_tools

from target_redshift.redshift import RedshiftTarget

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'redshift_database',
    'redshift_password'
]


def main(config, input_stream=None):
    with psycopg2.connect(
            host=config.get('redshift_host', 'localhost'),
            port=config.get('redshift_port', 5439),
            dbname=config.get('redshift_database'),
            user=config.get('redshift_username'),
            password=config.get('redshift_password')
    ) as connection:
        redshift_target = RedshiftTarget(
            connection,
            postgres_schema=config.get('redshift_schema', 'public'))

        if input_stream:
            target_tools.stream_to_target(input_stream, redshift_target, config=config)
        else:
            target_tools.main(redshift_target)


def cli():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    main(args.config)
