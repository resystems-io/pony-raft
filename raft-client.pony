use "collections"
use "time"

interface tag RaftMonitor[T: Any #send]

	// TODO should replies be sent via the monitor? Or directly or via something else?

	be dropped(message: T) => None

	be backpressure(factor: U16) => None // zero if full rate can be handled, higher otherwise

	be reconfigure(term: U64, size: U16) => None // number of servers in the raft and leader term
		// (signals that a new vote was carried out)

actor Raft[T: Any #send]
	"""
	The raft acts as a gateway for sending commands to a server.

	Each client will maintain its own local "Raft".
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

	new create() => None

	be accept(command: T, ttlMillis: U32 = 0) => None

	// TODO add a register/unregister mechanism for adding replicas to the raft

	be monitor(m: RaftMonitor[T]) => None

	be unmonitor(m: RaftMonitor[T]) => None
