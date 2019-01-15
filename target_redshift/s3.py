import uuid

import boto3

SEPARATOR = '__'


def client(config):
    return boto3.client(
        's3',
        aws_access_key_id=config.get('aws_access_key_id'),
        aws_secret_access_key=config.get('aws_secret_access_key'))


def persist(config, readable, key_prefix=''):
    if not config.get('bucket'):
        raise Exception('`config.bucket` required')
    if not config.get('aws_access_key_id'):
        raise Exception('`config.aws_access_key_id` required')
    if not config.get('aws_secret_access_key'):
        raise Exception('`config.aws_secret_access_key` required')

    key = key_prefix + str(uuid.uuid4()).replace('-', '')

    target_client = client(config)

    target_client.upload_fileobj(
        _EncodeBinaryReadable(readable),
        config.get('bucket'),
        key)

    return [config.get('bucket'), key]


class _EncodeBinaryReadable:
    def __init__(self, readable_obj):
        self.input = readable_obj

    def readable(self):
        return True

    def read(self, *args, **kwargs):
        if len(args) > 0:
            max_bytes = args[0]
        else:
            max_bytes = None
        output = b''
        while (max_bytes is not None and len(output) < max_bytes) or True:  ## TODO: overflow?
            line = self.input.read()
            if line == '':
                return output
            output += line.encode('utf-8')
        return output
