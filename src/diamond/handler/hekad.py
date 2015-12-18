# coding=utf-8

"""
Heka

Sends via UDP, Protobuf, HekaFramingSplitter required.
Doesn't queue or buffer at all.

#### Dependencies

 * [heka-py](https://github.com/kalail/heka-py)

#### Configuration

Enable this handler

 * handlers = diamond.handler.hekahandler.HekaHandler

 * logger = [optional | 'diamond'] Heka Logger field

 * include_filters = [optional | '^.*'] A list of regex patterns.
     Only measurements whose path matches a filter will be submitted.
     Useful for limiting usage to *only* desired measurements, e.g.
       include_filters = "^diskspace\..*\.byte_avail$", "^loadavg\.01"
       include_filters = "^sockets\.",
                                     ^ note trailing comma to indicate a list

"""

from Handler import Handler
import logging
import time
import re
from datetime import datetime
try:
    import heka as heka
except ImportError:
    heka = None

class HekaHandler(Handler):

    def __init__(self, config=None):
        """
        Create a new instance of the HekaHandler class
        """
        # Initialize Handler
        Handler.__init__(self, config)
        logging.debug("Initialized Heka handler.")

        if heka is None:
            logging.error("Failed to import heka")
            return

        # Initialize Heka
        self.conn = heka.connections.HekaConnection('%s:%d' % (
            self.config['host'], self.config['port']))

        # If a user leaves off the ending comma, cast to a array for them
        include_filters = self.config['include_filters']
        if isinstance(include_filters, basestring):
            include_filters = [include_filters]

        self.include_reg = re.compile(r'(?:%s)' % '|'.join(include_filters))

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(HekaHandler, self).get_default_config_help()

        config.update({
            'logger': 'Heka Logger field',
            'host': 'heka host, default: 127.0.0.1',
            'port': 'heka port, default: 26000',
            'include_filters': '',
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """
        config = super(HekaHandler, self).get_default_config()

        config.update({
            'logger': 'diamond',
            'host': '127.0.0.1',
            'port': 26000,
            'include_filters': ['^.*'],
        })

        return config

    def process(self, metric):
        """
        Process a metric by sending it to Heka
        """
        path = metric.getCollectorPath()
        path += '.'
        path += metric.getMetricPath()

        if self.include_reg.match(path):
            msg = heka.Message(
                    type=path,
                    logger=self.config['logger'],
                    severity=heka.severity.INFORMATIONAL,
                    fields={'value': float(metric.value)},
                    hostname=metric.host,                    
                    timestamp=metric.timestamp,
                )
            self.conn.send_message(msg)

        else:
            self.log.debug("HekaHandler: Skip %s, no include_filters match",
                           path)

    def flush(self):
        """ Since we don't queue, this doesn't do anything """
        pass
