"""
Tests for L{txZMQ.factory}.
"""

from twisted.trial import unittest

from txZMQ.factory import ZmqFactory

from zmq.core.context import Context


class ZmqFactoryTestCase(unittest.TestCase):
    """
    Test case for L{zmq.twisted.factory.Factory}.
    """

    def setUp(self):
        self.factory = ZmqFactory()

    def test_shutdown(self):
        self.factory.shutdown()

    def test_optional_context(self):
        context = Context()
        factory = ZmqFactory(context)
        self.addCleanup(factory.shutdown)
        self.assertTrue(self.factory.context, context)
