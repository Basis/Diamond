# coding=utf-8

"""
Collects all number values from the db.serverStatus() command, other
values are ignored.

#### Dependencies

 * pymongo

#### Example Configuration

MongoDBCollector.conf

```
    enabled = True
```
"""

import diamond.collector
from diamond.collector import str_to_bool
import re
import zlib
from pprint import pformat

try:
    import pymongo
except ImportError:
    pymongo = None

try:
    from pymongo import ReadPreference
except ImportError:
    ReadPreference = None


class MongoDBCollector(diamond.collector.Collector):
    MAX_CRC32 = 4294967295

    def __init__(self, *args, **kwargs):
        self.__totals = {}
        super(MongoDBCollector, self).__init__(*args, **kwargs)

    def get_default_config_help(self):
        config_help = super(MongoDBCollector, self).get_default_config_help()
        config_help.update({
            'databases': 'A regex of which databases to gather metrics for.'
                         ' Defaults to all databases.',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(MongoDBCollector, self).get_default_config()
        config.update({
            'path':      'mongo',
            'databases': '.*',
        })
        return config

    def collect(self):
        """Collect number values from db.serverStatus()"""

        if pymongo is None:
            self.log.error('Unable to import pymongo')
            return

        try:
            client = pymongo.MongoClient()
        except Exception, e:
            self.log.error('Couldnt connect to mongodb: %s', e)
            return

        db_name_filter = re.compile(self.config['databases'])

        # connections
        server_status = client['admin'].command('serverStatus')
        self.publish(
            'connections.current',
            server_status['connections']['current'])

        # lock %
        locks = server_status.get('locks')
        if locks:
            if '.' in locks:
                locks['_global_'] = locks['.']
                del (locks['.'])
            key_prefix = 'percent'
            interval = self.compute_interval(server_status, 'uptimeMillis')
            for db_name in locks:
                if not db_name_filter.search(db_name):
                    continue
                r = self.get_dotted_value(
                    locks,
                    '%s.timeLockedMicros.r' % db_name)
                R = self.get_dotted_value(
                    locks,
                    '.%s.timeLockedMicros.R' % db_name)
                value = float(r + R) / 10
                if value:
                    self.publish_counter(
                        '%s.locks.%s.read' % (key_prefix, db_name),
                        value, time_delta=bool(interval),
                        interval=interval)
                w = self.get_dotted_value(
                    locks,
                    '%s.timeLockedMicros.w' % db_name)
                W = self.get_dotted_value(
                    locks,
                    '%s.timeLockedMicros.W' % db_name)
                value = float(w + W) / 10
                if value:
                    self.publish_counter(
                        '%s.locks.%s.write' % (key_prefix, db_name),
                        value, time_delta=bool(interval), interval=interval)

        for db_name in client.database_names():
            if not db_name_filter.search(db_name):
                continue
            db = client[db_name]

            # currentOp stuff
            current_ops = filter(
                    lambda x: x['active'] and db_name in x['ns'],
                    db.current_op()['inprog'])
            num = 0
            num_over_one_sec = 0
            slow_query_fields = {
                'secs_running',
                'op',
                'ns',
                'query',
                'client',
                'connectionId',
                'lockStats',
            }
            for op in current_ops:
                num += 1
                if op['secs_running'] >= 1:
                    num_over_one_sec += 1
                    fields = {k:op.get(k, '')
                        for k in slow_query_fields}

                    # we want to preserve the pretty dict output
                    fields['query'] = pformat(fields['query'])
                    fields['lockStats'] = pformat(fields['lockStats'])

                    self.create_publish_event(
                        'slow_query',
                        fields=fields,
                    )
            self.publish('current_ops.count', num)
            self.publish('slow_query.count', num)


    def get_dotted_value(self, data, key_name):
        key_name = key_name.split('.')
        for i in key_name:
            data = data.get(i, {})
            if not data:
                return 0
        return data

    def compute_interval(self, data, total_name):
        current_total = self.get_dotted_value(data, total_name)
        last_total = self.__totals.get(total_name, current_total)
        interval = current_total - last_total
        self.__totals[total_name] = current_total
        return interval
