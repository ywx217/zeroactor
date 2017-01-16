from zeroactor import gate
import collections
import echo_test


class RecordServer(gate.RouterRouterGate):
	def __init__(self, ip, port, max_per_poll=10):
		super(RecordServer, self).__init__(ip, port, max_per_poll, 5)
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


class RREchoTest(echo_test.EchoTest):
	def __init__(self, methodName='runTest'):
		super(RREchoTest, self).__init__(methodName)

	def _connect_to_each_other(self, from_to_list=None):
		import itertools
		if from_to_list is None:
			from_to_list = itertools.permutations(xrange(len(self._servers)))
		for i1, i2 in from_to_list:
			self._servers[min(i1, i2)].connect(self._conn_addr(max(i1, i2)))
		for _ in xrange(2 * len(from_to_list) + 2):
			self._poll_all(1)

	def _record_server_class(self):
		return RecordServer

	def _echo_server_class(self):
		return EchoServer

	def _send(self, iter_from_to_idx, msg_or_func='hello world', n_send=1):
		from_to = list(iter_from_to_idx)
		# self._connect_to_each_other(from_to)
		for _ in xrange(n_send):
			for from_idx, to_idx in from_to:
				msg = msg_or_func if isinstance(msg_or_func, basestring) else msg_or_func(from_idx, to_idx)
				self._servers[from_idx].send(self._conn_addr(to_idx), msg)
				if self._servers[from_idx].get_queue_size():
					self._servers[from_idx].poll(1)
					self._servers[to_idx].poll(1)
