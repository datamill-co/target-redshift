import uuid

import boto3

SEPARATOR = '__'


class S3:
    def __init__(self, aws_access_key_id, aws_secret_access_key, bucket, key_prefix=''):
        self._credentials = {'aws_access_key_id': aws_access_key_id,
                             'aws_secret_access_key': aws_secret_access_key}
        self.client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)
        self.bucket = bucket
        self.key_prefix = key_prefix

    def credentials(self):
        return self._credentials

    def persist(self, readable, key_prefix=''):
        key = self.key_prefix + key_prefix + str(uuid.uuid4()).replace('-', '')

        self.client.upload_fileobj(
            _EncodeBinaryReadable(readable),
            self.bucket,
            key)

        return [self.bucket, key]


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
