import psycopg2
import singer
from singer import utils
from target_postgres import target_tools

from target_redshift.redshift import RedshiftTarget
from target_redshift.s3 import S3

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
        s3_config = config.get('target_s3')
        s3 = S3(s3_config.get('aws_access_key_id'),
                s3_config.get('aws_secret_access_key'),
                s3_config.get('bucket'),
                s3_config.get('key_prefix'))

        redshift_target = RedshiftTarget(
            connection,
            s3,
            postgres_schema=config.get('redshift_schema', 'public'))

        if input_stream:
            target_tools.stream_to_target(input_stream, redshift_target, config=config)
        else:
            target_tools.main(redshift_target)


def cli():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    main(args.config)
