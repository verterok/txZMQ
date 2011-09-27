"""
Tests for L{txZMQ.xreq_xrep}.
"""
from twisted.trial import unittest
from twisted.internet import defer, task, reactor, error, base

from txZMQ.factory import ZmqFactory
from txZMQ.connection import ZmqEndpointType, ZmqEndpoint
from txZMQ.xreq_xrep import ZmqXREQConnection, ZmqXREPConnection
from txZMQ.test import _wait


class ZmqTestXREPConnection(ZmqXREPConnection):
    identity = 'service'
    def gotMessage(self, message_id, *message_parts):
        if not hasattr(self, 'messages'):
            self.messages = []
        self.messages.append([message_id, message_parts])
        self.reply(message_id, *message_parts)


class ZmqConnectionTestCase(unittest.TestCase):
    """
    Test case for L{zmq.twisted.connection.Connection}.
    """

    def setUp(self):
        self.factory = ZmqFactory()
        ZmqXREQConnection.identity = 'client'
        self.r = ZmqTestXREPConnection(self.factory,
                      ZmqEndpoint(ZmqEndpointType.Bind, "ipc://test-sock"))
        self.s = ZmqXREQConnection(self.factory,
                      ZmqEndpoint(ZmqEndpointType.Connect, "ipc://test-sock"))
        self.count = 0
        def get_next_id():
            self.count += 1
            return 'msg_id_%d' % (self.count,)
        self.s.get_next_id = get_next_id

    def tearDown(self):
        ZmqXREQConnection.identity = None
        self.factory.shutdown()

    def test_send_recv(self):
        self.s.sendMsg('aaa', 'aab')
        self.s.sendMsg('bbb')

        return _wait(0.01).addCallback(
                lambda _: self.failUnlessEqual(getattr(self.r, 'messages', []),
                    [['msg_id_1', ('aaa', 'aab')], ['msg_id_2', ('bbb',)]], "Message should have been received"))

    def test_send_recv_reply(self):
        d = self.s.sendMsg('aaa')
        def check_response(response):
            self.assertEqual(response, ['aaa'])
        d.addCallback(check_response)
        return d

    def test_lot_send_recv_reply(self):
        deferreds = []
        for i in range(10):
            msg_id = "msg_id_%d" % (i,)
            d = self.s.sendMsg('aaa')
            def check_response(response, msg_id):
                self.assertEqual(response, ['aaa'])
            d.addCallback(check_response, msg_id)
            deferreds.append(d)
        return defer.DeferredList(deferreds)

    @defer.inlineCallbacks
    def test_cleanup_requests(self):
        """The request dict is cleanedup properly."""
        yield self.s.sendMsg('aaa')
        self.assertEqual(self.s._requests, {})


class ZmqSlowTestXREPConnection(ZmqXREPConnection):

    identity = 'service'
    reactor = None
    delay = 0.5

    def gotMessage(self, message_id, *message_parts):
        if not hasattr(self, 'messages'):
            self.messages = []
        self.messages.append([message_id, message_parts])
        self.reactor.callLater(
            self.delay, self.reply, message_id, *message_parts)


class ZmqXREQConnectionTimeoutTestCase(unittest.TestCase):
    """
    Test case for timeouts in ZmqXREQConnection.
    """

    def setUp(self):
        self.r_clock = task.Clock()
        self.s_clock = task.Clock()
        self.factory = ZmqFactory()
        # set self.clock as the reactor in ZmqSlowTestXREPConnection
        ZmqSlowTestXREPConnection.reactor = self.r_clock
        self.r = ZmqSlowTestXREPConnection(self.factory,
                      ZmqEndpoint(ZmqEndpointType.Bind, "ipc://test-sock"))
        # set self.clock as the reactor in ZmqXREQConnection
        ZmqXREQConnection.identity = 'client'
        ZmqXREQConnection.reactor = self.s_clock
        self.s = ZmqXREQConnection(self.factory,
                      ZmqEndpoint(ZmqEndpointType.Connect, "ipc://test-sock"))
        self.count = 0
        def get_next_id():
            self.count += 1
            return 'msg_id_%d' % (self.count,)
        self.s.get_next_id = get_next_id

    def tearDown(self):
        ZmqXREQConnection.identity = None
        self.factory.shutdown()

    def iterate(self, delay=0.01):
        """Allow the reactor to spin a bit."""
        return task.deferLater(reactor, delay, lambda: defer.succeed(None))

    @defer.inlineCallbacks
    def test_timeout_set(self):
        """Test that timeout is set."""
        now = self.r_clock.seconds()
        d = self.s.sendMsg('aaa', timeout=5)
        request = self.s._requests.values()[0]
        self.assertIsInstance(request, defer.Deferred)
        timeout = self.s._timeouts.values()[0]
        self.assertIsInstance(timeout, base.DelayedCall)
        self.assertEqual(timeout.time, 5)
        yield self.iterate()
        self.r_clock.advance(1)
        yield d

    @defer.inlineCallbacks
    def test_timeout_called(self):
        """Test that timeout is called."""
        d = self.s.sendMsg('aaa', timeout=2)
        self.assertFalse(d.called)
        self.s_clock.advance(3)
        self.assertTrue(d.called)
        try:
            yield d
        except error.TimeoutError:
            self.assertEqual({}, self.s._timeouts)
        else:
            self.fail("Should fail with TimeoutError.")

    @defer.inlineCallbacks
    def test_timeout_called_and_reply(self):
        """Test that timeout is called, but we also get a reply."""
        # patch messageReceived in order to know if it's called
        called = []
        msg_recv = self.s.messageReceived
        def intercept(msg):
            """Collect calls."""
            msg_recv(msg)
            called.append(msg)
        self.patch(self.s, 'messageReceived', intercept)
        d = self.s.sendMsg('aaa', timeout=2)
        self.assertFalse(d.called)
        self.s_clock.advance(3)
        yield self.iterate()
        self.assertTrue(d.called)
        self.r_clock.advance(1)
        yield self.iterate()
        # check that messageReceived was called
        self.assertEqual(called[0][2:], ['aaa'])
        try:
            yield d
        except error.TimeoutError:
            self.assertEqual({}, self.s._timeouts)
        else:
            self.fail("Should fail with TimeoutError.")

    @defer.inlineCallbacks
    def test_lots_of_timeouts(self):
        """Test that it also works for more than one timeout."""
        now = self.r_clock.seconds()
        deferreds = []
        times = {}
        for i in range(100):
            self.s_clock.advance(0.1)
            d = self.s.sendMsg('aaa_%d' % (i,), timeout=100)
            times[d] = self.s_clock.seconds()
            deferreds.append(d)
        self.assertEqual(100, len(self.s._requests))
        for msg_id, request in self.s._requests.items():
            self.assertIsInstance(request, defer.Deferred)
            timeout = self.s._timeouts[msg_id]
            self.assertIsInstance(timeout, base.DelayedCall)
            self.assertEqual(timeout.time, times[request]+100)

        yield self.iterate()
        self.r_clock.advance(0.1*101)
        yield defer.DeferredList(deferreds)
        self.assertEqual({}, self.s._timeouts)
        self.assertEqual({}, self.s._requests)

