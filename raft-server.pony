use "collections"
use "time"

// # Raft state transitions
//
//   _start_   |---[starts up]---------------------------------> Follower
//   Follower  |---[times out, starts election]----------------> Candidate
//   Candidate |---[times out, new election]-------------------> Candidate
//   Candidate |---[discovers current leader or new term]------> Follower
//   Candidate |---[receives votes from majority of servers]---> Leader
//   Leader    |---[discovers server with higher term]---------> Follower
//
// # Raft consensus pipeline
//
// Order | Log | Transmit | Commit | Execute
//
// # Raft architecture
//
// a. clients
// b. server
//   i. consensus module
//   ii. log
//   iii. state machine
//
// clients -> server.consensus_module
// server.consensus_module -> (other) server.consensus_module
// server.consensus_module -> server.log
// server.log -> server.state_machine
// server.state_machine -> clients -> ø

interface StateMachine[T: Any #send]
	"""
	A statemachine manages the internal state transitions that are specific to the
	application logic. A raft essentially drives the state machine once the event
	messages are committed to the journal.
	"""
	fun ref accept(command: T) => None

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

	be monitor(m: RaftMonitor[T]) => None

	be unmonitor(m: RaftMonitor[T]) => None

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
			None // dropped
		end

primitive Follower
primitive Candidate
primitive Leader

type RaftMode is (Follower | Candidate | Leader)

primitive ElectionTimeout

type RaftTimeout is (ElectionTimeout)

actor RaftServer[T: Any #send]
	"""
	Each raft server runs concurrently and coordinates with the other servers in the raft.
	This coordination then manages the server consensus states (leader/follower/candidate).
	Additionally, the server maintains the log in the persistent state. Finally, the server
	delegates committed log entries to the application specific state machine.

	The servers in the raft communicate via the network.

	The application state machine can then communicate directly with clients. Whereas,
	clients would communicate with one of the raft servers (and not with the application
	state machine).
	"""

	let _network: Network[T]
	let _id: U16
	let _peers: Array[U16]
	let _machine: StateMachine[T] iso					// implements the application logic
	let persistent: PersistentServerState[T]	// holds the log
	let volatile: VolatileServerState
	var leader: (VolatileLeaderState | None)

	var _mode: RaftMode
	var _lastKnownLeader: U16

	new create(id: U16, machine: StateMachine[T] iso, network: Network[T], peers: Array[U16] val) =>
		_id = id
		_network = network

		// copy peers but remove self
		_peers = Array[U16](peers.size() - 1)
		try
			var i: USize = 0
			var j: USize = 0
			while (i < peers.size()) do
				if peers(j)? != id then
					_peers(i)? = peers(j)?
					i = i+1
				end
				j = j+1
			end
		end

		_machine = consume machine
		persistent = PersistentServerState[T]
		volatile = VolatileServerState
		leader = None
		_lastKnownLeader = 0
		_mode = Follower
		_start_follower()

	fun ref _start_follower() =>
		// set up timeout for append-entries heart beat
		// TODO
		//
		_mode = Follower
		None

	fun ref _start_candidate() =>
		_mode = Candidate

	fun ref _start_leader() =>
		_mode = Leader

	be accept(command: T) =>
		""" Accept a new command from a client. """
		match _mode
		| Follower	=> _accept_follower(consume command)
		| Candidate	=> _accept_candidate(consume command)
		| Leader		=> _accept_leader(consume command)
		end

	fun ref _accept_follower(command: T) =>
		// follower  - redirect command to the leader
		//             (leader will reply, leader may provide backpressure)
		None

	fun ref _accept_candidate(command: T) =>
		// candidate - queue the command until transitioning to being a leader or follower
		//             (honour ttl and generate dropped message signals)
		//             (send backpressure if the queue gets too large)
		//             (batch queued message to the leader)
		None

	fun ref _accept_leader(command: T) =>
		// leader    - apply commands to the journal log and distribute them to followers
		//             (may generate backpressure if the nextIndex vs matchedIndex vs log
		//              starts to get too large)
		None

	be append(update: AppendEntriesRequest[T] val) =>
		None

	be vote(select: VoteRequest val) =>
		None
