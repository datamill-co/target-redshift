import psycopg2
import singer
from singer import utils
from target_postgres import target_tools
from target_postgres.postgres import MillisLoggingConnection

from target_redshift.redshift import RedshiftTarget
from target_redshift.s3 import S3

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'redshift_host',
    'redshift_database',
    'redshift_username',
    'redshift_password',
    'target_s3'
]


def main(config, input_stream=None):
    with psycopg2.connect(
            connection_factory=MillisLoggingConnection,
            host=config.get('redshift_host'),
            port=config.get('redshift_port', 5439),
            dbname=config.get('redshift_database'),
            user=config.get('redshift_username'),
            password=config.get('redshift_password')
    ) as connection:
        s3_config = config.get('target_s3')
        s3 = S3(s3_config.get('aws_access_key_id'),
                s3_config.get('aws_secret_access_key'),
                s3_config.get('bucket'),
                s3_config.get('key_prefix'))

        redshift_target = RedshiftTarget(
            connection,
            s3,
            redshift_schema=config.get('redshift_schema', 'public'),
            logging_level=config.get('logging_level'),
            default_column_length=config.get('default_column_length', 1000),
            persist_empty_tables=config.get('persist_empty_tables'),
            accept_inv_characters = config.get('accept_inv_chars', False),
            escape = config.get('escape', False)
        )

        if input_stream:
            target_tools.stream_to_target(input_stream, redshift_target, config=config)
        else:
            target_tools.main(redshift_target)


def cli():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help = 'Config file',
        required = True)

    parser.add_argument(
        '-s', '--state',
        help = 'State file')

    parser.add_argument(
        '-p', '--properties',
        help = 'Property selections: DEPRECATED, Please use --catalog instead')

    parser.add_argument(
        '--catalog',
        help = 'Catalog file')

    parser.add_argument(
        '-d', '--discover',
        action = 'store_true',
        help = 'Do schema discovery')

    parser.add_argument(
       '--accept_inv_chars',
        action = 'store_true',
        help = 'Accept invalid characters in the Redshift tables.')

    parser.add_argument(
        '--escape',
        action = 'store_true',
        help = 'Escape characters in the Redshift tables.')

    args = parser.parse_args()
    if args.config:
        args.config = load_json(args.config)
    if args.state:
        args.state = load_json(args.state)
    else:
        args.state = {}
    if args.properties:
        args.properties = load_json(args.properties)
    if args.catalog:
        args.catalog = Catalog.load(args.catalog)

    utils.check_config(args.config, required_config_keys)

    main(args.config)
