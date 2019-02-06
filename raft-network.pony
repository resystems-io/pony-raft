use "collections"
use "time"


// TODO consider being more explicit by defining Endpoint relative to Raft and RaftServer

interface tag Endpoint[T: Any #send]
	be accept(msg: T) => None
	be stop() => None

actor NopEndpoint[T: Any #send] is Endpoint[T]
	be accept(msg: T) => None
	be stop() => None

actor Network[T: Any #send]
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
			_registry(id)?.accept(consume msg)
		else
			// TODO implement a network monitor
			None // dropped
		end
