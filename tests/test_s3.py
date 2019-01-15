import json
import os

import pytest

from fixtures import CONFIG
from target_redshift import s3


class Readable:
    def __init__(self, iterable):
        self.iter = iter(iterable)

    def read(self):
        try:
            return json.dumps(next(self.iter)) + '\n'
        except StopIteration:
            return ''


def simple_download(bucket, key):
    s3_client = s3.client(CONFIG['target_s3'])

    s3_client.download_file(bucket, key, key)

    original = []
    with open(key) as tmp_file:
        for line in tmp_file.readlines():
            original.append(json.loads(line))

    os.remove(key)
    return original


def test_persist():
    to_persist = []
    for i in range(100):
        to_persist.append({'a':123, 'b': 'cdef', 'g': i})
    bucket, key = s3.persist(CONFIG['target_s3'], Readable(to_persist))
    result = simple_download(bucket, key)

    assert to_persist == result


def test_persist__empty():
    bucket, key = s3.persist(CONFIG['target_s3'], Readable([]))
    assert [] == simple_download(bucket, key)
