use "collections"
use "time"


// TODO consider being more explicit by defining Endpoint relative to Raft and RaftServer

interface tag Stoppable
	be stop() => None

interface tag Endpoint[T: Any #send] is Stoppable
	be apply(msg: T) => None

actor NopEndpoint[T: Any #send] is Endpoint[T]
	be apply(msg: T) => None
	be stop() => None

interface tag Transport[T: Any #send]
	"""
	A network linking endpoints (servers and clients)

	The endpoints are identified via a single U16 identifier.
	"""
	be send(id: U16, msg: T) => None

actor Network[T: Any #send] is Transport[T]
	"""
	A network linking endpoints (servers and clients)

	The endpoints are identified via a single U16 identifier.
	"""

	// TODO implement mechanisms to produce backpressure from the network
	// TODO decide how backpressure at different layers translates and combines

	let _registry: Map[U16, Endpoint[T] tag]

	new create() =>
		_registry = Map[U16, Endpoint[T] tag]

	be register(id: U16, server: Endpoint[T] tag) =>
		_registry(id) = server

	be send(id: U16, msg: T) =>
		try
			_registry(id)?.apply(consume msg)
		else
			// TODO implement a network monitor
			None // dropped
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
		// Give explicit precedent to type 'A' in the case that both A and B match
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

	be send(id: U16, msg: (A|B)) =>

		// Note, this is ambiguous with respect to the case that A <: B (A is a subtype of B)
		match msg
		| let m: A => _transa.send(id, m)
		| let m: B => _transb.send(id, m)
		end
