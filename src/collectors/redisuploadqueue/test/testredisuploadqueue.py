#!/usr/bin/python
# coding=utf-8
###############################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import unittest

from redisuploadqueue import RedisUploadQueueCollector


###############################################################################

class TestRedisUploadQueueCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('RedisUploadQueueCollector', {
        })
        self.collector = RedisUploadQueueCollector(config, None)

    def test_import(self):
        self.assertTrue(RedisUploadQueueCollector)

###############################################################################
if __name__ == "__main__":
    unittest.main()
