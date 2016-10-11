import gate
import random
import collections
import unittest


class RecordServer(gate.DealerRouterGate):

	def __init__(self, ip, port, max_per_poll=100):
		super(RecordServer, self).__init__(ip, port, max_per_poll)
		self._recv_queue = collections.deque()

	def _dispatch_rpc(self, source_addr, payload):
		# print '[%s][recv] addr=%s payload=%s' % (self.connect_addr, source_addr, payload)
		self._recv_queue.append((source_addr, payload))

	@property
	def recv_len(self):
		return len(self._recv_queue)


class EchoServer(RecordServer):
	def _dispatch_rpc(self, source_addr, payload):
		super(EchoServer, self)._dispatch_rpc(source_addr, payload)
		self.send(source_addr, payload)


class EchoTest(unittest.TestCase):
	def __init__(self, methodName='runTest'):
		super(EchoTest, self).__init__(methodName)
		self._servers = []

	def _destroy_servers(self):
		for s in self._servers:
			s.destroy()
		self._servers = []

	def _init_servers(self, iter_serv_clazz):
		self._destroy_servers()
		base_port = random.randint(10000, 20000)
		self._servers = [
			clazz('127.0.0.1', base_port + idx) for idx, clazz in enumerate(iter_serv_clazz)
		]
		return self._servers

	def tearDown(self):
		self._destroy_servers()
		super(EchoTest, self).tearDown()

	def testSingleSend(self):
		self._init_servers((RecordServer, EchoServer))
		self._servers[0].send(self._servers[1].connect_addr, 'hello world')
		self._servers[0].send(self._servers[1].connect_addr, 'hello world')
		self._servers[0].send(self._servers[1].connect_addr, 'hello world')
		for _ in xrange(10):
			for s in self._servers:
				s.poll(10)

		for idx, s in enumerate(self._servers):
			self.assertEqual(3, s.recv_len, '%d-%s' % (idx, s))
