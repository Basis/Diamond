# coding=utf-8

"""
Collects RDS Cloudwatch metrics

#### Setup
 * aws credentials should be configured in diamond's home directory

#### Dependencies

 * boto3

#### Example Configuration

RDSCollector.conf
```
    enabled = True
    DBInstanceIdentifier = rds1-prod
```
"""

import diamond.collector
from diamond.metric import Metric
from datetime import datetime, timedelta
import boto3

RDS_METRICS = {
    'CPUUtilization',
    'DatabaseConnections',
    'DiskQueueDepth',
    'FreeStorageSpace',
    'FreeableMemory',
    'ReadIOPS',
    'WriteIOPS',
    'ReadLatency',
    'WriteLatency',
    'ReadThroughput',
    'WriteThroughput',
    'NetworkReceiveThroughput',
    'NetworkTransmitThroughput',
}

class RDSCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        self.client = boto3.client('cloudwatch')
        # CPUUtilization seems to be delayed by 3-5 minutes, so have to keep
        # track of the last seen datapoint
        self.last_datapoints = {x:None for x in RDS_METRICS}
        super(RDSCollector, self).__init__(*args, **kwargs)

    def get_default_config_help(self):
        config_help = super(RDSCollector, self).get_default_config_help()
        config_help.update({
            'DBInstanceIdentifier': 'AWS identifier for the rds instance',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(RDSCollector, self).get_default_config()
        config.update({
            'path':      'rds',
        })
        return config

    def _publish_rds_metric(self, metric_name):
        """
        This method can obviously skip metrics if the api calls take a long
        time, and the later metrics might even be calling the next minute, but
        over time it should be fine as long it takes less than 1 minute to query
        all the metrics
        """
        if self.last_datapoints[metric_name]:
            start_time = self.last_datapoints[metric_name] + timedelta(seconds=1)
        else:
            self.last_datapoints[metric_name] = datetime.now()-timedelta(seconds=int(self.config['interval']))
            start_time = self.last_datapoints[metric_name]
        response = self.client.get_metric_statistics(
            Namespace='AWS/RDS',
            MetricName=metric_name,
            Dimensions=[{
                'Name': 'DBInstanceIdentifier',
                'Value': self.config['DBInstanceIdentifier']
            }],
            StartTime=start_time,
            EndTime=datetime.now(),
            Period=60,
            Statistics=['Average'],
        )
        try:
            path = self.get_metric_path(metric_name)
            for datapoint in response['Datapoints']:
                no_tz_ts = datapoint['Timestamp'].replace(tzinfo=None)
                ts = (no_tz_ts - datetime.utcfromtimestamp(0)).total_seconds()
                m = Metric(path, datapoint['Average'], timestamp=ts)
                self.publish_metric(m)
                self.last_datapoints[metric_name] = no_tz_ts
        except Exception as e:
            self.log.error(response)
            self.log.error(e)
            

    def collect(self):
        if 'DBInstanceIdentifier' not in self.config:
            return
        for metric_name in RDS_METRICS:
            self._publish_rds_metric(metric_name)
