# coding=utf-8

import time
import re
import logging
from error import DiamondException


class Event(object):

    def __init__(self, path, fields={}, timestamp=None, host=None):
        """
        Create new instance of the Event class

        Takes:
            path=string: string the specifies the path of the metric
            fields=dict: the fields the event will have. needs to be serializable
            timestamp=[float|int]: the timestamp, in seconds since the epoch (as from time.time())
        """

        # Validate the path
        if path is None:
            raise DiamondException("Event requires non-None path")

        # If no timestamp was passed in, set it to the current time
        if timestamp is None:
            timestamp = int(time.time())
        else:
            # If the timestamp isn't an int, then make it one
            if not isinstance(timestamp, int):
                try:
                    timestamp = int(timestamp)
                except ValueError, e:
                    raise DiamondException(("Invalid timestamp when "
                                            "creating new Event %r: %s")
                                           % (path, e))

        # The field needs to be a dict. Needs to be serializable
        if not isinstance(fields, (dict)):
            raise DiamondException(("field kwarg not dict when creating "
                                    "Event %r") % path)

        self.path = path
        self.fields = fields
        self.timestamp = timestamp
        self.host = host

    def __repr__(self):
        """
        Return the Event as a string
        """
        return "%s %s %i\n" % (self.path, self.fields, self.timestamp)

    def getPathPrefix(self):
        """
            Returns the path prefix path
            servers.host.cpu.total.idle
            return "servers"
        """
        # If we don't have a host name, assume it's just the first part of the
        # metric path
        if self.host is None:
            return self.path.split('.')[0]

        offset = self.path.index(self.host) - 1
        return self.path[0:offset]

    def getCollectorPath(self):
        """
            Returns collector path
            servers.host.cpu.total.idle
            return "cpu"
        """
        # If we don't have a host name, assume it's just the third part of the
        # metric path
        if self.host is None:
            return self.path.split('.')[2]

        offset = self.path.index(self.host)
        offset += len(self.host) + 1
        endoffset = self.path.index('.', offset)
        return self.path[offset:endoffset]

    def getMetricPath(self):
        """
            Returns the metric path after the collector name
            servers.host.cpu.total.idle
            return "total.idle"
        """
        # If we don't have a host name, assume it's just the fourth+ part of the
        # metric path
        if self.host is None:
            path = self.path.split('.')[3:]
            return '.'.join(path)

        prefix = '.'.join([self.getPathPrefix(), self.host,
                           self.getCollectorPath()])

        offset = len(prefix) + 1
        return self.path[offset:]
