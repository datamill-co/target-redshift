import re

from psycopg2 import sql
from target_postgres import json_schema
from target_postgres.postgres import PostgresError, PostgresTarget
from target_postgres.singer_stream import (
    SINGER_LEVEL
)
from target_postgres.sql_base import SEPARATOR

from target_redshift import s3


class RedshiftError(PostgresError):
    """
    Raise this when there is an error with regards to Redshift streaming
    """


class RedshiftTarget(PostgresTarget):
    """
    Placeholder for specific Redshift implementation of a Singer Target.
    """

    MAX_VARCHAR = 65535

    def __init__(self, connection, *args, redshift_schema='public', **kwargs):
        self.LOGGER.info(
            'RedshiftTarget created with established connection: `{}`, schema: `{}`'.format(connection.dsn,
                                                                                            redshift_schema))

        self.conn = connection
        self.postgres_schema = redshift_schema

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
        key_prefix = self.s3_config.get('key_prefix', '') + temp_table_name + SEPARATOR

        bucket, key = s3.persist(self.s3_config,
                                 csv_rows,
                                 key_prefix=key_prefix)

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
