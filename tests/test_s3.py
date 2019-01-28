import json
import os

import pytest

from fixtures import CONFIG
from target_redshift.s3 import S3


class Readable:
    def __init__(self, iterable):
        self.iter = iter(iterable)

    def read(self):
        try:
            return json.dumps(next(self.iter)) + '\n'
        except StopIteration:
            return ''


class DownloadableS3(S3):
    def download(self, key):
        self.client.download_file(self.bucket, key, key)

        original = []
        with open(key) as tmp_file:
            for line in tmp_file.readlines():
                original.append(json.loads(line))

        os.remove(key)
        return original


def downloadableS3():
    return DownloadableS3(CONFIG['target_s3']['aws_access_key_id'],
                          CONFIG['target_s3']['aws_secret_access_key'],
                          CONFIG['target_s3']['bucket'],
                          CONFIG['target_s3']['key_prefix'])


def test_persist():
    to_persist = []
    for i in range(100):
        to_persist.append({'a': 123, 'b': 'cdef', 'g': i})

    s3 = downloadableS3()
    bucket, key = s3.persist(Readable(to_persist))
    result = s3.download(key)

    assert to_persist == result


def test_persist__empty():
    s3 = downloadableS3()
    bucket, key = s3.persist(Readable([]))
    result = s3.download(key)

    assert [] == result
