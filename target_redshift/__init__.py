import psycopg2
import singer
from singer import utils
import sshtunnel
from target_postgres import target_tools
from target_postgres.postgres import MillisLoggingConnection

from target_redshift.redshift import RedshiftTarget
from target_redshift.s3 import S3

LOGGER = singer.get_logger()

def required_config_keys(use_ssh_tunnel=False):
    keys = [
        'redshift_host',
        'redshift_database',
        'redshift_username',
        'redshift_password',
        'target_s3'
    ]
    if use_ssh_tunnel:
        keys += [
            'ssh_jump_server',
            'ssh_jump_server_port',
            'ssh_private_key_path',
            'ssh_username'
        ]
    return keys



def main(config, input_stream=None):
    tunnel = None
    try:
        LOGGER.info(config)
        if bool(config.get('use_ssh_tunnel')) == True:
            LOGGER.info(f"use_ssh_tunnel is set to true; connecting to {config['redshift_host']}:{config['redshift_port']} via {config['ssh_jump_server']}:{config['ssh_jump_server_port']}")
            tunnel = sshtunnel.open_tunnel(
                (config['ssh_jump_server'], int(config['ssh_jump_server_port'])),
                ssh_username=config['ssh_username'],
                ssh_pkey=config['ssh_private_key_path'],
                ssh_private_key_password=config['ssh_private_key_password'] if 'ssh_private_key_password' in config else None,
                remote_bind_address=(config['redshift_host'], int(config['redshift_port']))
            )
            tunnel.start()
            config['redshift_host'] = '127.0.0.1' # rewrite the config to go through the tunnel
            config['redshift_port'] = tunnel.local_bind_port
        else:
            LOGGER.debug(f"use_ssh_tunnel is not set or is false; connecting directly to {config['redshift_host']}:{config['redshift_port']}")

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
                    s3_config.get('key_prefix'),
                    aws_session_token=s3_config.get('aws_session_token'))

            redshift_target = RedshiftTarget(
                connection,
                s3,
                redshift_schema=config.get('redshift_schema', 'public'),
                logging_level=config.get('logging_level'),
                default_column_length=config.get('default_column_length', 1000),
                persist_empty_tables=config.get('persist_empty_tables')
            )

            if input_stream:
                target_tools.stream_to_target(input_stream, redshift_target, config=config)
            else:
                target_tools.main(redshift_target)
            
    finally:
        if tunnel is not None:
            tunnel.stop()



def cli():
    args = utils.parse_args(required_config_keys())
    if bool(args.config.get('use_ssh_tunnel')) == True:
        args = utils.parse_args(required_config_keys(True))
    

    main(args.config)
