# coding=utf-8

"""
Collects nodetool tpstats

#### Dependencies

 * nodetool (script)

#### Example Configuration

CassandraCollector.conf
```
    enabled = True
```
"""

import diamond.collector
import subprocess

class CassandraCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        self.__totals = {}
        super(CassandraCollector, self).__init__(*args, **kwargs)

    def get_default_config_help(self):
        config_help = super(CassandraCollector, self).get_default_config_help()
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(CassandraCollector, self).get_default_config()
        config.update({
            'path':      'cassandra',
        })
        return config

    def collect(self):
        """Collect nodetool tpstats

        Nodetool tpstats output format:

        Pool Name                    Active   Pending      Completed   Blocked All time blocked
        Native-Transport-Requests         0         0      395874760         0 0
        ReadRepairStage                   0         0       13360046         0 0
        ReadStage                         0         0      228563885         0 0
        RequestResponseStage              0         0      384938875         0 0
        CounterMutationStage              0         0              0         0 0
        MutationStage                     0         0      211452572         0 0
        MemtablePostFlush                 0         0          27800         0 0
        GossipStage                       0         0        5010566         0 0
        MiscStage                         0         0              0         0 0
        PendingRangeCalculator            0         0             43         0 0
        AntiEntropyStage                  0         0              0         0 0
        CacheCleanupExecutor              0         0              0         0 0
        MigrationStage                    0         0              0         0 0
        HintedHandoff                     0         0             43         0 0
        ValidationExecutor                0         0              0         0 0
        MemtableFlushWriter               0         0          10228         0 0
        InternalResponseStage             0         0          31875         0 0
        Sampler                           0         0              0         0 0
        MemtableReclaimMemory             0         0          10228         0 0
        CompactionExecutor                0         0        1507255         0 0

        Message type           Dropped
        RANGE_SLICE                  0
        READ_REPAIR                  0
        PAGED_RANGE                  0
        READ                         0
        MUTATION                     0
        _TRACE                       0
        REQUEST_RESPONSE             0
        COUNTER_MUTATION             0

        """
        output = subprocess.check_output(['nodetool','tpstats'])
        sections = output.split('\n\n')
        pool_stats = sections[0].split('\n')[1:]
        message_stats = sections[1].split('\n')[1:]

        pool_stats_col_names = [
          'Active',
          'Pending',
          'Completed',
          'Blocked',
        ]
        for line in pool_stats:
            cols = line.split()
            for i, col in enumerate(cols[1:-1]):
                metric_name = '%s.%s' % (cols[0], pool_stats_col_names[i])
                self.publish(metric_name, int(col))

        for line in message_stats:
            if not line:
                continue
            cols = line.split()
            metric_name = '%s.%s' % (cols[0], 'dropped')
            self.publish(metric_name, int(cols[1]))
