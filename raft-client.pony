use "collections"
use "time"

primitive RaftClientStage
primitive RaftServerStage

type RaftStage is (RaftClientStage | RaftServerStage)

interface tag RaftMonitor[T: Any #send]

	// TODO should replies be sent via the monitor? Or directly or via something else?

	be dropped(message: T, stage: RaftStage) =>
		"""
		Signaled when the dispatched message is known to not have been delivered.

		Receipt of 'dropped' signals is not guaranteed.
		"""
		None

	be backpressure(factor: U16) =>
		"""
		Signaled when the raft should adjust its transmission rate.

		Zero if the full rate can be handled, higher if the client
		should start backing off.
		"""
		None

	be reconfigure(size: U16, term: U16, leader: U16) =>
		"""
		Signaled when the raft is informed of a change in the number or
		location of the backing servers.
		"""
		None

	be accept(message: T) =>
		"""
		Signaled when the raft has a response for the client.
		"""
		None

actor Raft[T: Any #send]
	"""
	The raft acts as a gateway for sending commands to a server.

	There will be a 1-to-1 relationship between clients and "Rafts".
	The "Raft" has knowledge of the raft servers.
	"""

	// The strategy for selecting a server can be one of round robin,
	// or following of redirects.
	//
	// More specifically, we will contact the last-known-good server
	// based on redirects. However, if there is no "good" server, then
	// we will round robin.
	//
	// Note, we are _not_ attempting to handle retries. That is, while
	// the servers may have failed comms between them, we are not trying
	// to handle the case where the client can not reach a given server.
	//
	// The messages may simply be dropped. However, the raft may notify
	// the clients, asynchronously of:
	//   - leader updates
	//   - backpressure
	//   - dropped messages?

	new create() =>
		None

	be accept(command: T, ttlMillis: U32 = 0) =>
		"""
		Accept a client message to delegate to a replica.

		A TTL is provided as the budget for queuing.
		"""
		None

	// TODO add a register/unregister mechanism for adding replicas to the raft

	be monitor(m: RaftMonitor[T]) =>
		None

	be unmonitor(m: RaftMonitor[T]) =>
		None
