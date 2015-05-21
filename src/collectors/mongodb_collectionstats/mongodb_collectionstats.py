# coding=utf-8

"""
Collects all number values from the db.serverStatus() command, other
values are ignored.

#### Dependencies

 * pymongo

#### Example Configuration

MongoDBCollectionStatsCollector.conf

```
    enabled = True
    hosts = localhost:27017, alias1@localhost:27018, etc
```
"""

import diamond.collector
from diamond.collector import str_to_bool
import re
import zlib

try:
    import pymongo
except ImportError:
    pymongo = None

try:
    from pymongo import ReadPreference
except ImportError:
    ReadPreference = None

IGNORED_COLLKEYS = (
    'ok',
    'paddingFactor',
    'lastExtentSize',
    'numExtents',
    'nindexes',
    'userFlags',
    'systemFlags',
    'nindexes',
)

class MongoDBCollectionStatsCollector(diamond.collector.Collector):
    def __init__(self, *args, **kwargs):
        self.__totals = {}
        super(MongoDBCollectionStatsCollector, self).__init__(*args, **kwargs)

    def get_default_config_help(self):
        config_help = super(MongoDBCollectionStatsCollector, self).get_default_config_help()
        config_help.update({
            'hosts': 'Array of hostname(:port) elements to get metrics from'
                     'Set an alias by prefixing host:port with alias@',
            'host': 'A single hostname(:port) to get metrics from'
                    ' (can be used instead of hosts and overrides it)',
            'user': 'Username for authenticated login (optional)',
            'passwd': 'Password for authenticated login (optional)',
            'databases': 'A regex of which databases to gather metrics for.'
                         ' Defaults to all databases.',
            'ignore_collections': 'A regex of which collections to ignore.'
                                  ' MapReduce temporary collections (tmp.mr.*)'
                                  ' are ignored by default.',
            'network_timeout': 'Timeout for mongodb connection (in seconds).'
                               ' There is no timeout by default.',
            'translate_collections': 'Translate dot (.) to underscores (_)'
                                     ' in collection names.',
            'ssl': 'True to enable SSL connections to the MongoDB server.'
                    ' Default is False'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(MongoDBCollectionStatsCollector, self).get_default_config()
        config.update({
            'path':      'mongo',
            'hosts':     ['localhost'],
            'user':      None,
            'passwd':      None,
            'databases': '.*',
            'ignore_collections': '^tmp\.mr\.',
            'network_timeout': None,
            'translate_collections': 'False',
            'ssl': False
        })
        return config

    def collect(self):
        """Collect number values from db.serverStatus()"""

        if pymongo is None:
            self.log.error('Unable to import pymongo')
            return

        hosts = self.config.get('hosts')

        # Convert a string config value to be an array
        if isinstance(hosts, basestring):
            hosts = [hosts]

        # we need this for backwards compatibility
        if 'host' in self.config:
            hosts = [self.config['host']]

        # convert network_timeout to integer
        if self.config['network_timeout']:
            self.config['network_timeout'] = int(
                self.config['network_timeout'])

        # use auth if given
        if 'user' in self.config:
            user = self.config['user']
        else:
            user = None

        if 'passwd' in self.config:
            passwd = self.config['passwd']
        else:
            passwd = None

        for host in hosts:
            matches = re.search('((.+)\@)?(.+)?', host)
            alias = matches.group(2)
            host = matches.group(3)

            if alias is None:
                if len(hosts) == 1:
                    # one host only, no need to have a prefix
                    base_prefix = []
                else:
                    base_prefix = [re.sub('[:\.]', '_', host)]
            else:
                base_prefix = [alias]

            try:
                # Ensure that the SSL option is a boolean.
                if type(self.config['ssl']) is str:
                    self.config['ssl'] = str_to_bool(self.config['ssl'])

                if ReadPreference is None:
                    conn = pymongo.Connection(
                        host,
                        network_timeout=self.config['network_timeout'],
                        ssl=self.config['ssl'],
                        slave_okay=True
                    )
                else:
                    conn = pymongo.Connection(
                        host,
                        network_timeout=self.config['network_timeout'],
                        ssl=self.config['ssl'],
                        read_preference=ReadPreference.SECONDARY,
                    )
            except Exception, e:
                self.log.error('Couldnt connect to mongodb: %s', e)
                continue

            # try auth
            if user:
                try:
                    conn.admin.authenticate(user, passwd)
                except Exception, e:
                    self.log.error('User auth given, but could not autheticate'
                                   + ' with host: %s, err: %s' % (host, e))
                    return{}

            db_name_filter = re.compile(self.config['databases'])
            ignored_collections = re.compile(self.config['ignore_collections'])
            for db_name in conn.database_names():
                if not db_name_filter.search(db_name):
                    continue
                db_stats = conn[db_name].command('dbStats')
                db_prefix = base_prefix + ['databases', db_name]
                self._publish_dict_with_prefix(db_stats, db_prefix)
                for collection_name in conn[db_name].collection_names():
                    if ignored_collections.search(collection_name):
                        continue
                    collection_stats = conn[db_name].command('collstats',
                                                             collection_name)
                    for key in IGNORED_COLLKEYS:
                        if key in collection_stats:
                            del collection_stats[key]
                    if str_to_bool(self.config['translate_collections']):
                        collection_name = collection_name.replace('.', '_')
                    collection_prefix = db_prefix + [collection_name]
                    self._publish_dict_with_prefix(collection_stats,
                                                   collection_prefix)

    def _publish_dict_with_prefix(self, dict, prefix, publishfn=None):
        for key in dict:
            self._publish_metrics(prefix, key, dict, publishfn)

    def _publish_metrics(self, prev_keys, key, data, publishfn=None):
        """Recursively publish keys"""
        if key not in data:
            return
        value = data[key]
        keys = prev_keys + [key]
        if not publishfn:
            publishfn = self.publish
        if isinstance(value, dict):
            for new_key in value:
                self._publish_metrics(keys, new_key, value)
        elif isinstance(value, int) or isinstance(value, float):
            publishfn('.'.join(keys), value)
        elif isinstance(value, long):
            publishfn('.'.join(keys), float(value))
