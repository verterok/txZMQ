"""
ZeroMQ PUB-SUB wrappers.
"""

from twisted.internet import defer, error
from zmq.core import constants

from txZMQ.connection import ZmqConnection


class ZmqXREQConnection(ZmqConnection):
    """
    A XREQ connection.
    """
    socketType = constants.XREQ
    timeout = 30
    reactor = None

    def __init__(self, factory, *endpoints):
        ZmqConnection.__init__(self, factory, *endpoints)
        # set the default reactor if there none
        if self.reactor is None:
            from twisted.internet import reactor
            self.reactor = reactor
        self._requests = {}
        self._timeouts = {}

    def shutdown(self):
        """Override shutdown to stop the timeout loop."""
        # cancel all timeouts
        for msg_id in self._requests:
            timeout = self._timeouts[msg_id]
            if timeout.active:
                timeout.cancel()
        return ZmqConnection.shutdown(self)

    def get_next_id(self):
        """Returns an unique id."""
        raise NotImplementedError(self)

    def _requestTimeout(self, msg_id):
        """Timeout a specific request."""
        # remove the timeout from the _timeouts dict
        self._timeouts.pop(msg_id, None)
        # remove the deferred from the _requests dict and call the errback
        d = self._requests.pop(msg_id, None)
        if d:
            d.errback(error.TimeoutError(string='Request timeout.'))

    def sendMsg(self, *message_parts, **kwargs):
        """
        Send L{message} with specified L{tag}.

        @param message_parts: message data
        @type message: C{tuple}
        """
        timeout = kwargs.pop('timeout', self.timeout)
        d = defer.Deferred()
        message_id = self.get_next_id()
        timeoutCall = self.reactor.callLater(timeout,
                                             self._requestTimeout, message_id)
        self._timeouts[message_id] = timeoutCall
        self._requests[message_id] = d
        message = [message_id, '']
        message.extend(message_parts)
        self.send(message)
        return d

    def messageReceived(self, message):
        """
        Called on incoming message from ZeroMQ.

        @param message: message data
        """
        msg_id, _, msg = message[0], message[1], message[2:]
        timeout = self._timeouts.pop(msg_id, None)
        # remove the timeout from the _timeouts dict and cancel it
        if timeout and timeout.active:
            timeout.cancel()
        # only callback if the request is still there, it might have been
        # removed by timeout
        d = self._requests.pop(msg_id, None)
        if d:
            d.callback(msg)


class ZmqXREPConnection(ZmqConnection):
    """
    A XREP connection.
    """
    socketType = constants.XREP

    def __init__(self, factory, *endpoints):
        ZmqConnection.__init__(self, factory, *endpoints)
        self._routing_info = {} # keep track of routing info

    def reply(self, message_id, *message_parts):
        """
        Send L{message} with specified L{tag}.

        @param message_id: message uuid
        @type message_id: C{str}
        @param message: message data
        @type message: C{str}
        """
        routing_info = self._routing_info[message_id]
        msg = []
        msg.extend(routing_info)
        msg.append(message_id)
        msg.append('')
        msg.extend(message_parts)
        self.send(msg)

    def messageReceived(self, message):
        """
        Called on incoming message from ZeroMQ.

        @param message: message data
        """
        for i, msg in enumerate(message):
            if msg == '':
                break
        routing_info, msg_id, payload = message[:i-1], message[i-1], message[i+1:]
        msg_parts = payload[0:]
        self._routing_info[msg_id] = routing_info
        self.gotMessage(msg_id, *msg_parts)

    def gotMessage(self, message_id, *message_parts):
        """
        Called on incoming message.

        @param message_parts: message data
        @param tag: message tag
        """
        raise NotImplementedError(self)
