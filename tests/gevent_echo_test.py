import gevent
import gevent.pool
import gevent.event
import gevent.queue
import echo_test

STEP_PRINT_ENABLED = False


class Step(object):
	def __init__(self, name):
		super(Step, self).__init__()
		self._name = name

	def __enter__(self):
		if STEP_PRINT_ENABLED:
			print '--- %s' % self._name

	def __exit__(self, exc_type, exc_val, exc_tb):
		if STEP_PRINT_ENABLED:
			print '+++ %s' % self._name


class GeventEchoTest(echo_test.EchoTest):
	# Do the same tests in EchoTest, but using greenlet for parallel send and recv
	def _testSend(self, iter_servers, send_func, poll_func):
		self._greenlets = gevent.pool.Group()
		self._send_finish_event = gevent.event.Event()
		self._init_servers(iter_servers)
		send_func()
		poll_func()
		self._greenlets.join()
		self._greenlets = None

	def _send(self, iter_from_to_idx, msg_or_func='hello world', n_send=1):
		_finish_signal = gevent.queue.Channel()

		def single_send(from_idx, to_idx):
			with Step('send %d->%d' % (from_idx, to_idx)):
				for _ in xrange(n_send):
					msg = msg_or_func if isinstance(msg_or_func, basestring) else msg_or_func(from_idx, to_idx)
					self._servers[from_idx].send(self._conn_addr(to_idx), msg)
					gevent.sleep(0.01)
			_finish_signal.put(True)

		def send_complete(count):
			with Step('send complete watch'):
				for _ in xrange(count):
					_finish_signal.get()
			self._send_finish_event.set()

		c = 0
		for fi, ti in iter_from_to_idx:
			self._greenlets.add(gevent.spawn(lambda f=fi, t=ti: single_send(f, t)))
			c += 1
		self._greenlets.add(gevent.spawn(lambda: send_complete(c)))

	def _poll_n_times(self, n, timeout=1):
		def single_poll(s_idx, serv):
			with Step('poll-%s' % s_idx):
				while 1:
					serv.poll(0)
					gevent.sleep(0.01)
					if self._send_finish_event.ready():
						break
				for _ in xrange(5):
					serv.poll(0)
					gevent.sleep(0.01)

		for idx, server in enumerate(self._servers):
			self._greenlets.add(gevent.spawn(lambda i=idx, s=server: single_poll(i, s)))
