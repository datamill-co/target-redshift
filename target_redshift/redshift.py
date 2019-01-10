import re
import uuid

import boto3
from psycopg2 import sql
from target_postgres import json_schema
from target_postgres.postgres import PostgresError, PostgresTarget
from target_postgres.singer_stream import (
    SINGER_LEVEL
)
from target_postgres.sql_base import SEPARATOR


class RedshiftError(PostgresError):
    """
    Raise this when there is an error with regards to Redshift streaming
    """


class _BinaryCSV(object):
    def __init__(self, csv_rows):
        self.csv_rows = csv_rows

    def read(self, *args, **kwargs):
        if len(args) > 0:
            max_bytes = args[0]
        else:
            max_bytes = None
        output = b''
        while (max_bytes is not None and len(output) < max_bytes) or True:  ## TODO: overflow?
            line = self.csv_rows.read()
            if line == '':
                return output
            output += line.encode('utf-8')
        return output


class RedshiftTarget(PostgresTarget):
    """
    Placeholder for specific Redshift implementation of a Singer Target.
    """

    MAX_VARCHAR = 65535

    def __init__(self, connection, *args, redshift_schema='public', **kwargs):
        self.LOGGER.info(
            'RedshiftTarget created with established connection: `{}`, PostgreSQL schema: `{}`'.format(connection.dsn,
                                                                                                       redshift_schema))

        PostgresTarget.__init__(self, connection, postgres_schema=redshift_schema)

    def sql_type_to_json_schema(self, sql_type, is_nullable):
        if sql_type == 'character varying':
            schema = {'type': [json_schema.STRING]}
            if is_nullable:
                return json_schema.make_nullable(schema)
            return schema

        return PostgresTarget.sql_type_to_json_schema(self, sql_type, is_nullable)

    def json_schema_to_sql_type(self, schema):
        psql_type = PostgresTarget.json_schema_to_sql_type(self, schema)

        max_length = schema.get('maxLength', self.MAX_VARCHAR)
        if max_length > self.MAX_VARCHAR:
            max_length = self.MAX_VARCHAR

        if psql_type.upper() == 'TEXT':
            return 'varchar({})'.format(max_length)
        elif psql_type.upper() == 'TEXT NOT NULL':
            return 'varchar({}) NOT NULL'.format(max_length)

        return psql_type

    def persist_csv_rows(self,
                         cur,
                         remote_schema,
                         temp_table_name,
                         columns,
                         csv_rows):
        s3_client = boto3.client(
            's3',
            aws_access_key_id=self.s3_config.get('aws_access_key_id'),
            aws_secret_access_key=self.s3_config.get('aws_secret_access_key'))

        bucket = self.s3_config.get('bucket')
        if not bucket:
            raise RedshiftError('`target_s3.bucket` required')
        prefix = self.s3_config.get('key_prefix', '')
        key = prefix + temp_table_name + SEPARATOR + str(uuid.uuid4()).replace('-', '')

        s3_client.upload_fileobj(
            _BinaryCSV(csv_rows),
            bucket,
            key)

        source = 's3://{}/{}'.format(bucket, key)
        credentials = 'aws_access_key_id={};aws_secret_access_key={}'.format(
            self.s3_config.get('aws_access_key_id'),
            self.s3_config.get('aws_secret_access_key'))

        copy_sql = sql.SQL('COPY {}.{} ({}) FROM {} CREDENTIALS {} FORMAT AS CSV').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(temp_table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.Literal(source),
            sql.Literal(credentials))

        cur.execute(copy_sql)

        pattern = re.compile(SINGER_LEVEL.format('[0-9]+'))
        subkeys = list(filter(lambda header: re.match(pattern, header) is not None, columns))

        update_sql = self.get_update_sql(remote_schema['name'],
                                         temp_table_name,
                                         remote_schema['key_properties'],
                                         subkeys)
        cur.execute(update_sql)
