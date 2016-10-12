import zmq


def _destroy_socket(sock):
	sock.setsockopt(zmq.LINGER, 0)
	sock.close()


def _recv_from_socket(socket, max_per_poll):
	# no block receive from a zmq socket
	try:
		for _ in xrange(max_per_poll):
			msg_parts = socket.recv_multipart(zmq.NOBLOCK)
			yield msg_parts
	except zmq.ZMQError:
		# no more msg
		pass


class ZeroGate(object):
	"""
	Do both send to and recv from another zero gate.
	"""

	def __init__(self, ip, port, max_per_poll=100):
		super(ZeroGate, self).__init__()
		self._is_destroied = False
		# zmq context
		self._context = zmq.Context()
		self._poller = zmq.Poller()
		# address used for socket.bind
		self._bind_addr = 'tcp://*:%d' % port
		# address used for building mailbox for the callee
		self._connect_addr = 'tcp://%s:%d' % (ip, port)
		# sockets
		self._server_socket = self._create_server_socket()
		self._client_socket_map = {}  # target_addr: sock
		# max receive msg nums per poll
		self._max_per_poll = max_per_poll

	def set_max_per_poll(self, max_per_poll):
		self._max_per_poll = max_per_poll

	@property
	def is_destroied(self):
		return self._is_destroied

	@property
	def connect_addr(self):
		return self._connect_addr

	def _create_server_socket(self):
		# serve caller rpc
		raise NotImplementedError

	def _create_client_socket(self, target_socket_addr):
		# create a client socket connected to the callee gate
		raise NotImplementedError

	def _remove_client_socket(self, target_socket_addr):
		raise NotImplementedError

	def destroy(self):
		if self._is_destroied:
			return
		for client_sock in self._client_socket_map.itervalues():
			_destroy_socket(client_sock)
		self._client_socket_map.clear()
		_destroy_socket(self._server_socket)
		self._context.destroy()
		self._is_destroied = True

	def poll(self, timeout_millisecond=1):
		# zmq.poll for receiving new rpc
		raise NotImplementedError

	def send(self, target_socket, payload):
		# send payload to the target socket
		raise NotImplementedError


class DealerRouterGate(ZeroGate):
	"""
	zeromq rpc gateway based on dealer and router
	"""

	def _create_server_socket(self):
		sock = self._context.socket(zmq.ROUTER)
		sock.bind(self._bind_addr)
		self._poller.register(sock, zmq.POLLIN)
		return sock

	def _create_client_socket(self, target_socket_addr):
		if target_socket_addr in self._client_socket_map:
			return False
		sock = self._context.socket(zmq.DEALER)
		sock.connect(target_socket_addr)
		self._poller.register(sock, zmq.POLLIN)
		self._client_socket_map[target_socket_addr] = sock
		return True

	def _remove_client_socket(self, target_socket_addr):
		if target_socket_addr not in self._client_socket_map:
			return False
		_destroy_socket(self._client_socket_map[target_socket_addr])
		del self._client_socket_map[target_socket_addr]

	def _get_client_socket(self, target_socket_addr, auto_create=True):
		socket = self._client_socket_map.get(target_socket_addr)
		if not socket:
			if not auto_create:
				return None
			self._create_client_socket(target_socket_addr)
		return self._client_socket_map.get(target_socket_addr)

	def _dispatch_recv(self, msg_parts):
		if len(msg_parts) < 4:
			return
		identity, addr, control, payload = msg_parts[:4]
		if control == 'RPC':
			self._dispatch_rpc(addr, payload)
		else:
			pass

	def _dispatch_rpc(self, source_addr, payload):
		raise NotImplementedError

	def poll(self, timeout_millisecond=1):
		sockets = dict(self._poller.poll(max(timeout_millisecond, 0)))
		if not sockets:
			# no socket has data to receive
			return
		if sockets.get(self._server_socket) == zmq.POLLIN:
			# server recv
			for msg_parts in _recv_from_socket(self._server_socket, self._max_per_poll):
				self._dispatch_recv(msg_parts)
		for socket, event in sockets.iteritems():
			# client recv
			if sockets == self._server_socket or event != zmq.POLLIN:
				continue
			# dump all msg received by client socket
			filter(None, _recv_from_socket(socket, self._max_per_poll))

	def send(self, target_socket_addr, payload):
		target_socket = self._get_client_socket(target_socket_addr)
		if not target_socket:
			return
		target_socket.send_multipart([self._connect_addr, 'RPC', payload])
