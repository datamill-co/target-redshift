from copy import deepcopy
import re

from psycopg2 import sql
from target_postgres import json_schema
from target_postgres.postgres import PostgresError, PostgresTarget, RESERVED_NULL_DEFAULT
from target_postgres.singer_stream import (
    SINGER_LEVEL
)
from target_postgres.sql_base import SEPARATOR

class RedshiftError(PostgresError):
    """
    Raise this when there is an error with regards to Redshift streaming
    """

def _make_schema_nullable(schema):
    # Redshift does not allow for creation of columns which are non null without a default
    nullable_schema = deepcopy(schema)
    nullable_properties = nullable_schema['properties']

    for field in nullable_properties:
        nullable_properties[field] = json_schema.make_nullable(nullable_properties[field])
    return nullable_schema


class RedshiftTarget(PostgresTarget):
    """
    Placeholder for specific Redshift implementation of a Singer Target.
    """
    # https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
    IDENTIFIER_FIELD_LENGTH = 127
    MAX_VARCHAR = 65535
    CREATE_TABLE_INITIAL_COLUMN = '_sdc_target_redshift_create_table_placeholder'
    CREATE_TABLE_INITIAL_COLUMN_TYPE = 'BOOLEAN'

    def __init__(self, connection, s3, *args, redshift_schema='public', logging_level=None, **kwargs):
        self.LOGGER.info(
            'RedshiftTarget created with established connection: `{}`, schema: `{}`'.format(connection.dsn,
                                                                                            redshift_schema))

        self.s3 = s3
        PostgresTarget.__init__(self, connection, postgres_schema=redshift_schema, logging_level=logging_level)

    def write_batch(self, stream_buffer):
        # WARNING: Using mutability here as there's no simple way to copy the necessary data over
        nullable_stream_buffer = stream_buffer
        nullable_stream_buffer.schema = _make_schema_nullable(stream_buffer.schema)
        stream_buffer = nullable_stream_buffer
        if stream_buffer.count == 0:
            return None

        with self.conn.cursor() as cur:
            try:
                self._validate_identifier(stream_buffer.stream)

                cur.execute('BEGIN;')

                current_table_schema = self.get_table_schema(cur,
                                                             (stream_buffer.stream,),
                                                             stream_buffer.stream)

                current_table_version = None

                if current_table_schema:
                    current_table_version = current_table_schema.get('version', None)

                    if set(stream_buffer.key_properties) \
                            != set(current_table_schema.get('key_properties')):
                        raise PostgresError(
                            '`key_properties` change detected. Existing values are: {}. Streamed values are: {}'.format(
                                current_table_schema.get('key_properties'),
                                stream_buffer.key_properties
                            ))
                    for key in stream_buffer.key_properties:
                        """
                        "key", as it is in the stream_buffer object, migth contain uppercase letters, but in Redshift columns are created lowercase.
                        As a consequence, in order to check that the current table has same type as the one in the current schema, we must use 
                        the lowercase key for the Redshift table and the key name as it is in the current schema.
                        E.g.: Let`s say we have the following key:'orderID'.For  this key, a new column will be created in a Redshift table: "orderid".
                              To check that both of them have the same sql type, we call the json_schema_to_sql_type method as it follows:
                              current_table_schema['schema']['properties']['orderid'] and stream_buffer.schema['properties']['orderID'].
                        """
                        current_table_key = key.lower()
                        stream_buffer_key = key
                        if self.json_schema_to_sql_type(current_table_schema['schema']['properties'][current_table_key]) \
                                != self.json_schema_to_sql_type(stream_buffer.schema['properties'][stream_buffer_key]):
                            raise PostgresError(
                                ('`key_properties` type change detected for "{}". ' +
                                 'Existing values are: {}. ' +
                                 'Streamed values are: {}, {}, {}').format(
                                    key,
                                    json_schema.get_type(current_table_schema['schema']['properties'][current_table_key]),
                                    json_schema.get_type(stream_buffer.schema['properties'][stream_buffer_key]),
                                    self.json_schema_to_sql_type(current_table_schema['schema']['properties'][current_table_key]),
                                    self.json_schema_to_sql_type(stream_buffer.schema['properties'][stream_buffer_key])
                                ))

                root_table_name = stream_buffer.stream
                target_table_version = current_table_version or stream_buffer.max_version

                if current_table_version is not None and \
                        stream_buffer.max_version is not None:
                    if stream_buffer.max_version < current_table_version:
                        self.LOGGER.warning('{} - Records from an earlier table version detected.'
                                            .format(stream_buffer.stream))
                        cur.execute('ROLLBACK;')
                        return None

                    elif stream_buffer.max_version > current_table_version:
                        root_table_name = stream_buffer.stream + SEPARATOR + str(stream_buffer.max_version)
                        target_table_version = stream_buffer.max_version

                self._validate_identifier(root_table_name)
                written_batches_details = self.write_batch_helper(cur,
                                                                  root_table_name,
                                                                  stream_buffer.schema,
                                                                  stream_buffer.key_properties,
                                                                  stream_buffer.get_batch(),
                                                                  {'version': target_table_version})

                cur.execute('COMMIT;')

                return written_batches_details
            except Exception as ex:
                cur.execute('ROLLBACK;')
                message = 'Exception writing records'
                self.LOGGER.exception(message)
                raise PostgresError(message, ex)

    def upsert_table_helper(self, connection, table_schema, metadata, log_schema_changes=True):
        nullable_table_schema = deepcopy(table_schema)
        nullable_table_schema['schema'] = _make_schema_nullable(nullable_table_schema['schema'])
        return PostgresTarget.upsert_table_helper(self,
                                                  connection,
                                                  nullable_table_schema,
                                                  metadata,
                                                  log_schema_changes=log_schema_changes)

    def add_table(self, cur, name, metadata):
        self._validate_identifier(name)

        create_table_sql = sql.SQL('CREATE TABLE {}.{} ({} {})').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(name),
            # Redshift does not allow for creation of tables with no columns
            sql.Identifier(self.CREATE_TABLE_INITIAL_COLUMN),
            sql.SQL(self.CREATE_TABLE_INITIAL_COLUMN_TYPE))

        cur.execute(sql.SQL('{};').format(
            create_table_sql))

        self._set_table_metadata(cur, name, {'version': metadata.get('version', None)})

        self.add_column_mapping(cur,
                                name,
                                (self.CREATE_TABLE_INITIAL_COLUMN,),
                                self.CREATE_TABLE_INITIAL_COLUMN,
                                json_schema.make_nullable({'type': json_schema.BOOLEAN}))

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
        key_prefix = temp_table_name + SEPARATOR

        bucket, key = self.s3.persist(csv_rows,
                                      key_prefix=key_prefix)

        credentials = self.s3.credentials()

        copy_sql = sql.SQL('COPY {}.{} ({}) FROM {} CREDENTIALS {} FORMAT AS CSV NULL AS {}').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(temp_table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.Literal('s3://{}/{}'.format(bucket, key)),
            sql.Literal('aws_access_key_id={};aws_secret_access_key={}'.format(
                credentials.get('aws_access_key_id'),
                credentials.get('aws_secret_access_key'))),
            sql.Literal(RESERVED_NULL_DEFAULT))

        cur.execute(copy_sql)

        pattern = re.compile(SINGER_LEVEL.format('[0-9]+'))
        subkeys = list(filter(lambda header: re.match(pattern, header) is not None, columns))

        update_sql = self._get_update_sql(remote_schema['name'],
                                          temp_table_name,
                                          remote_schema['key_properties'],
                                          columns,
                                          subkeys)

        cur.execute(update_sql)
