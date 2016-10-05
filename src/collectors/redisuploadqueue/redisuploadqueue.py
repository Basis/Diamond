# coding=utf-8

"""
Collects upload queue size for celery out of redis

#### Dependencies

 * redis

Example config file RedisUploadQueueCollector.conf

```
enabled=True
host=localhost
port=6379
```

"""

import diamond.collector
import redis


class RedisUploadQueueCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(RedisUploadQueueCollector, self).get_default_config_help()
        config_help.update({
            'path': 'redisuploadqueue',
            'host': 'Redis server host',
            'port': 'Redis server port',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(RedisUploadQueueCollector, self).get_default_config()
        config.update({
            'host':     'localhost',
            'port':     '6379'
        })
        return config

    def collect(self):
        """
        Overrides the Collector.collect method
        """

        host = self.config['host']
        port = self.config['port']

        try:
            redis_ = redis.StrictRedis(host=host,port=port)
            self.publish('length', int(redis_.llen('celery')))
            self.publish('data-export-length', int(redis_.llen('data-export')))
            self.publish('data-export-research-length', int(redis_.llen('data-export-research')))
        except Exception as ex:
            self.log.error("RedisUploadQueueCollector: failed to connect to %s:%i. %s.",
                           host, port, ex)


