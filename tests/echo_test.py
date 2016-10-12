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
		# initialize servers using the given classes
		self._destroy_servers()
		base_port = random.randint(10000, 20000)
		self._servers = [
			clazz('127.0.0.1', base_port + idx) for idx, clazz in enumerate(iter_serv_clazz)
			]
		return self._servers

	def _conn_addr(self, server_idx):
		return self._servers[server_idx].connect_addr

	def _poll_all(self, timeout=1):
		for s in self._servers:
			s.poll(timeout)

	def _poll_n_times(self, n, timeout=1):
		for _ in xrange(n):
			self._poll_all(timeout)

	def _send(self, iter_from_to_idx, msg_or_func='hello world', n_send=1):
		from_to = list(iter_from_to_idx)
		for _ in xrange(n_send):
			for from_idx, to_idx in from_to:
				msg = msg_or_func if isinstance(msg_or_func, basestring) else msg_or_func(from_idx, to_idx)
				self._servers[from_idx].send(self._conn_addr(to_idx), msg)

	def tearDown(self):
		self._destroy_servers()
		super(EchoTest, self).tearDown()

	def _testSend(self, iter_servers, send_func, poll_func):
		self._init_servers(iter_servers)
		send_func()
		poll_func()

	def testEchoSingleSend(self):
		n_send = 3
		self._testSend(
			(RecordServer, EchoServer),
			lambda: self._send(((0, 1),), n_send=n_send),
			lambda: self._poll_n_times(10)
		)
		for idx, s in enumerate(self._servers):
			self.assertEqual(n_send, s.recv_len, '%d-%s' % (idx, s))

	def testEchoDoubleSideSend(self):
		n_send = 3
		self._testSend(
			(RecordServer, EchoServer),
			lambda: self._send(((0, 1), (1, 0)), n_send=n_send),
			lambda: self._poll_n_times(10)
		)
		self.assertEqual(2 * n_send, self._servers[0].recv_len)
		self.assertEqual(n_send, self._servers[1].recv_len)

	def testEchoManyToOne(self):
		n = 10
		n_send = 3
		self._testSend(
			(RecordServer,) * n + (EchoServer,),
			lambda: self._send(zip(range(n), (n,) * n), n_send=n_send),
			lambda: self._poll_n_times(10)
		)
		for idx, s in enumerate(self._servers):
			if idx < n:
				self.assertEqual(n_send, s.recv_len, '%d-%s: %s!=%s' % (idx, s, n_send, s.recv_len))
			else:
				self.assertEqual(n * n_send, s.recv_len, '%d-%s: %s!=%s' % (idx, s, n * n_send, s.recv_len))

	def testRingSend(self):
		n = 10
		n_send = 3

		def ring_iterable():
			for x in xrange(n):
				yield x, (x + 1) % n

		self._testSend(
			(RecordServer,) * n,
			lambda: self._send(ring_iterable(), n_send=n_send),
			lambda: self._poll_n_times(10)
		)
		for idx, s in enumerate(self._servers):
			self.assertEqual(n_send, s.recv_len)

	def testStarSend(self):
		n = 10
		n_send = 3

		def star_iterable():
			for x in xrange(n):
				for y in xrange(n):
					if x == y:
						continue
					yield x, y

		self._testSend(
			(RecordServer,) * n,
			lambda: self._send(star_iterable(), n_send=n_send),
			lambda: self._poll_n_times(10, 10)
		)
		for idx, s in enumerate(self._servers):
			self.assertEqual(n_send * (n - 1), s.recv_len)
