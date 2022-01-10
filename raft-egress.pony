use "collections"

interface tag Egress[P: Any val]
	"""
	An egress fabric that will route the given message.
	"""
	be emit(msg: P) =>
		"""
		Emit a packet into the egress fabric.

		Once in the egress fabric, the egress routing will direct the
		packet based on the routing rules and the packet contents.

		Note, egress routing can be source specific, but this is
		transparent to the emitting component.
		"""
		None

type RaftEgress[T: Any val, U: Any val] is Egress[(RaftServerSignal[T]|U)]

actor IntraProcessRaftServerEgress[T: Any val, U: Any val] is RaftEgress[T,U]
	"""
	An egress for raft servers via which they can reach peers and clients.

	T = the state-machine commands.
	U = the state-machine response.
	"""

	let _monitor: NetworkMonitor
	let _registry_peer: Map[NetworkAddress, RaftEndpoint[T] tag]
	let _registry_client: Map[NetworkAddress, Endpoint[U] tag]

	new create(monitor: NetworkMonitor = NopNetworkMonitor) =>
		_monitor = monitor
		_registry_peer = Map[NetworkAddress, RaftEndpoint[T] tag]
		_registry_client = Map[NetworkAddress, Endpoint[U] tag]

	be register_peer(id: NetworkAddress, server: RaftEndpoint[T] tag) =>
		_registry_peer(id) = server

	be register_client(id: NetworkAddress, client: Endpoint[U] tag) =>
		_registry_client(id) = client

	be emit(msg: (RaftServerSignal[T] | U)) =>
		match consume msg
		| (let m: RaftServerSignal[T]) => _handle_peer(consume m)
		| (let m: U) => _handle_client(consume m)
		end

	fun ref _handle_peer(m: RaftServerSignal[T]) =>
		let id = match m
			| (let v: RaftTarget val) => v.target()
			else
				_monitor.dropped(NetworkAddresses.unknown())
				return
			end
		_route_peer(id, m)

	fun ref _route_peer(id: NetworkAddress, m: RaftServerSignal[T]) =>
		try
			_registry_peer(id)?.apply(consume m)
			_monitor.sent(id)
		else
			_monitor.dropped(id)
		end

	fun ref _handle_client(m: U) =>
		None // TODO this can be client specific and should be delegated to the implementation
