use "collections"

interface tag Egress[P: Any val]
	// TODO revist use of 'val' and consider expanding to '#send'
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

interface val EgressMonitor
	"""
	A monitor for network activity.
	"""
	fun val dropped(id: NetworkAddress) => None
	fun val sent(id: NetworkAddress) => None

class val NopEgressMonitor is EgressMonitor

interface tag RaftEgress[T: Any val, U: Any val] is Egress[(RaftServerSignal[T]|U)]
	be register_peer(id: RaftId, server: RaftEndpoint[T] tag) => None

actor NopEgress[P: Any val] is Egress[P]

actor IntraProcessRaftServerEgress[T: Any val, U: Any val] is RaftEgress[T,U]
	"""
	An egress for raft servers via which they can reach peers and clients.

	T = the state-machine commands.
	U = the state-machine response.
	"""

	let _monitor: EgressMonitor
	let _registry_peer: Map[RaftId, RaftEndpoint[T] tag]
	let _client_delegate: Egress[U] tag

	new create(monitor: EgressMonitor = NopEgressMonitor, delegate: Egress[U] = NopEgress[U]) =>
		_monitor = monitor
		_registry_peer = Map[RaftId, RaftEndpoint[T] tag]
		_client_delegate = delegate

	be register_peer(id: RaftId, server: RaftEndpoint[T] tag) =>
		_registry_peer(id) = server

	be emit(msg: (RaftServerSignal[T] | U)) =>
		match consume msg
		| (let m: RaftServerSignal[T]) => _handle_peer(consume m)
		| (let m: U) => _handle_client(consume m)
		end

	fun ref _handle_peer(m: RaftServerSignal[T]) =>
		let id = match m
			| (let v: RaftTarget val) => v.target()
			else
				_monitor.dropped(RaftIdentifiers.unknown())
				return
			end
		_route_peer(id, m)

	fun ref _route_peer(id: RaftId, m: RaftServerSignal[T]) =>
		try
			_registry_peer(id)?.apply(consume m)
			_monitor.sent(id)
		else
			_monitor.dropped(id)
		end

	fun ref _handle_client(m: U) =>
		_client_delegate.emit(consume m)

interface val MapToKey[K: U16 val, P: Any val]
	fun box apply(m: P): (K,P!) ? => error

class TrivalMapToKey[K: U16 val, P: Any val] is MapToKey[K,P]

actor IntraProcessEgress[K: U16 val, P: Any val] is Egress[P]
	"""
	A general egress with the ability to register endpoints.

	K = the identifier of the endpoint
	P = the type being routed
	"""

	let _monitor: EgressMonitor
	let _mapper: MapToKey[K,P]
	let _registry: Map[K, Endpoint[P] tag]

	new create(mapper: MapToKey[K,P] = TrivalMapToKey[K,P], monitor: EgressMonitor = NopEgressMonitor) =>
		_mapper = mapper
		_monitor = monitor
		_registry= Map[K, Endpoint[P] tag]

	be register(id: K, endpoint: Endpoint[P] tag) =>
		_registry(id) = endpoint

	be emit(msg: P) =>
		try
			(let k: K, let m: P) = _mapper.apply(msg)?
			_registry(k)?.apply(consume m)
			_monitor.sent(k)
		else
			_monitor.dropped(RaftIdentifiers.unknown())
		end
