use "collections"
use "time"

actor Network[T: Any #send]
	"""
	A network linking servers.

	The servers are identified via a single U16 identifier.
	"""

	// TODO implement mechanisms to produce backpressure from the network
	// TODO decide how backpressure at different layers translates and combines

	let _registry: Map[U16, RaftServer[T]]

	new create() =>
		_registry = Map[U16, RaftServer[T]]

	be register(id: U16, server: RaftServer[T]) =>
		_registry(id) = server

	be send(id: U16, msg: T) =>
		try
			_registry(id)?.accept(consume msg)
		else
			// TODO implement a network monitor
			None // dropped
		end
