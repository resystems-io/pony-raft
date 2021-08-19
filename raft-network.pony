use "collections"
use "time"


// TODO consider being more explicit by defining Endpoint relative to Raft and RaftServer

interface tag Stoppable is DisposableActor
	be stop() => None
	be dispose() => stop()

interface tag Endpoint[T: Any #send] is Stoppable
	"""
	Logically and endpoing absorbes a message.

	The implementation might then deleage the message to some
	other element.
	"""
	be apply(msg: T) => None

actor NopEndpoint[T: Any #send] is Endpoint[T]
	"""
	A dead-end endpoint that doesn't send a message.
	"""
	be apply(msg: T) => None
	be stop() => None

type NetworkAddress is U16
	"""
	An address to identify processes attached to the network.

	In the parlance of the Internet this would be the combination: IP:port.
	"""

interface tag Transport[T: Any #send]
	"""
	A network linking endpoints (servers and clients)

	The endpoints are identified via a single identifier.
	"""
	be unicast(id: NetworkAddress, msg: T) => None

	be broadcast(msg: T) => None

	be anycast(msg: T) => None

interface val NetworkMonitor
	"""
	A monitor for network activity.
	"""
	fun val dropped(id: NetworkAddress) => None
	fun val sent(id: NetworkAddress) => None

class val NopNetworkMonitor is NetworkMonitor

actor Network[T: Any #send] is Transport[T]
	"""
	A network linking endpoints (servers and clients)

	The endpoints are identified via a single identifier.
	"""

	// TODO implement mechanisms to produce backpressure from the network
	// TODO decide how backpressure at different layers translates and combines

	let _monitor: NetworkMonitor
	let _registry: Map[NetworkAddress, Endpoint[T] tag]

	new create(monitor: NetworkMonitor = NopNetworkMonitor) =>
		_monitor = monitor
		_registry = Map[NetworkAddress, Endpoint[T] tag]

	be register(id: NetworkAddress, server: Endpoint[T] tag) =>
		_registry(id) = server

	be unicast(id: NetworkAddress, msg: T) =>
		try
			_registry(id)?.apply(consume msg)
			_monitor.sent(id)
		else
			// TODO implement a network monitor
			_monitor.dropped(id)
		end

actor SpanningEndpoint[A: Any val, B: Any val] is Endpoint[(A|B)]
	"""
	An endpoint that can process two types and dispatch accordingly.

	Precedence is given to the type 'A' over type 'B' in the case that both match.
	"""

	let _enda: Endpoint[A]
	let _endb: Endpoint[B]

	new create(enda: Endpoint[A], endb: Endpoint[B]) =>
		_enda = consume enda
		_endb = consume endb

	be apply(msg: (A|B)) =>
		// Give explicit precedence to type 'A' in the case that both A and B match
		match msg
		| let m: A => _enda.apply(m)
		else
			match msg
			| let m: B => _endb.apply(m)
			else
				None // not actualy reachable
			end
		end

actor SpanningNetwork[A: Any val, B: Any val] is Transport[(A|B)]
	"""
	A transport that can differentiate between two network classes.
	"""

	let _transa: Transport[A]
	let _transb: Transport[B]

	new create(transa: Transport[A], transb: Transport[B]) =>
		_transa = consume transa
		_transb = consume transb

	be unicast(id: U16, msg: (A|B)) =>

		// Note, this is ambiguous with respect to the case that A <: B (A is a subtype of B)
		match msg
		| let m: A => _transa.unicast(id, m)
		| let m: B => _transb.unicast(id, m)
		end
