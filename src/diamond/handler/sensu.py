# coding=utf-8

"""
Send metrics to a [sensu](http://http://sensuapp.org/) using the client socket 
interface.

"""

from Handler import Handler
import socket
import json

class SensuHandler(Handler):
    """
    Implements the abstract Handler class, sending data to sensu
    """

    def __init__(self, config=None):
        """
        Create a new instance of the SensuHandler class
        """
        # Initialize Handler
        Handler.__init__(self, config)

        # Initialize Data
        self.socket = None

        # Initialize Options
        self.proto = self.config['proto'].lower().strip()
        self.host = self.config['host']
        self.port = int(self.config['port'])
        self.timeout = int(self.config['timeout'])
        self.keepalive = bool(self.config['keepalive'])
        self.keepaliveinterval = int(self.config['keepaliveinterval'])
        self.batch_size = int(self.config['batch'])
        self.max_backlog_multiplier = int(
            self.config['max_backlog_multiplier'])
        self.trim_backlog_multiplier = int(
            self.config['trim_backlog_multiplier'])
        self.metrics = []

        # Connect
        self._connect()

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(SensuHandler, self).get_default_config_help()

        config.update({
            'host': 'Hostname',
            'port': 'Port',
            'proto': 'udp or tcp',
            'timeout': '',
            'batch': 'How many to store before sending to the sensu server',
            'max_backlog_multiplier': 'how many batches to store before '
                'trimming',
            'trim_backlog_multiplier': 'Trim down how many batches',
            'keepalive': 'Enable keepalives for tcp streams',
            'keepaliveinterval': 'How frequently to send keepalives'
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """
        config = super(SensuHandler, self).get_default_config()

        config.update({
            'host': 'localhost',
            'port': 2003,
            'proto': 'tcp',
            'timeout': 15,
            'batch': 1,
            'max_backlog_multiplier': 5,
            'trim_backlog_multiplier': 4,
            'keepalive': 0,
            'keepaliveinterval': 10,
        })

        return config

    def __del__(self):
        """
        Destroy instance of the SensuHandler class
        """
        self._close()

    def process(self, metric):
        """
        Process a metric by sending it to sensu
        """
        # Append the data to the array as a string
        things = str(metric).split( );
        self.metrics.append(json.dumps({'output': things[1], 'issued': things[2], 'name': things[0]}) + "\n")
        if len(self.metrics) >= self.batch_size:
            self._send()

    def flush(self):
        """Flush metrics in queue"""
        self._send()

    def _send_data(self, data):
        """
        Try to send all data in buffer.
        """
        try:
            self.socket.sendall(data)
        except:
            self._close()
            self.log.error("SensuHandler: Socket error, trying reconnect.")
            self._connect()
            self.socket.sendall(data)

    def _send(self):
        """
        Send data to sensu. Data that can not be sent will be queued.
        """
        # Check to see if we have a valid socket. If not, try to connect.
        try:
            try:
                if self.socket is None:
                    self.log.debug("SensuHandler: Socket is not connected. "
                                   "Reconnecting.")
                    self._connect()
                if self.socket is None:
                    self.log.debug("SensuHandler: Reconnect failed.")
                else:
                    # Send data to socket
                    self._send_data(''.join(self.metrics))
                    self.metrics = []
            except Exception:
                self._close()
                self.log.error("SensuHandler: Error sending metrics.")
                raise
        finally:
            if len(self.metrics) >= (
                self.batch_size * self.max_backlog_multiplier):
                trim_offset = (self.batch_size
                               * self.trim_backlog_multiplier * -1)
                self.log.warn('SensuHandler: Trimming backlog. Removing'
                              + ' oldest %d and keeping newest %d metrics',
                              len(self.metrics) - abs(trim_offset),
                              abs(trim_offset))
                self.metrics = self.metrics[trim_offset:]

    def _connect(self):
        """
        Connect to the sensu server
        """
        if (self.proto == 'udp'):
            stream = socket.SOCK_DGRAM
        else:
            stream = socket.SOCK_STREAM

        # Create socket
        self.socket = socket.socket(socket.AF_INET, stream)
        if self.socket is None:
            # Log Error
            self.log.error("SensuHandler: Unable to create socket.")
            # Close Socket
            self._close()
            return
        # Enable keepalives?
        if self.proto != 'udp' and self.keepalive:
            self.log.error("SensuHandler: Setting socket keepalives...")
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE,
                                   self.keepaliveinterval)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL,
                                   self.keepaliveinterval)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        # Set socket timeout
        self.socket.settimeout(self.timeout)
        # Connect to sensu server
        try:
            self.socket.connect((self.host, self.port))
            # Log
            self.log.debug("SensuHandler: Established connection to "
                           "sensu server %s:%d.",
                           self.host, self.port)
        except Exception, ex:
            # Log Error
            self.log.error("SensuHandler: Failed to connect to %s:%i. %s.",
                           self.host, self.port, ex)
            # Close Socket
            self._close()
            return

    def _close(self):
        """
        Close the socket
        """
        if self.socket is not None:
            self.socket.close()
        self.socket = None
