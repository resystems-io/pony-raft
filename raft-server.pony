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
//   i. raft proxy
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
	delegates committed log entries, containing commands of type T, to the application
	specific state machine.

	The servers in the raft communicate via the network.

	The application state machine can then communicate directly with clients. Whereas,
	clients would communicate with one of the raft servers (and not with the application
	state machine).
	"""

	let _timers: Timers
	let _network: Network[T]
	let _id: U16
	let _peers: Array[U16]
	let _machine: StateMachine[T] iso					// implements the application logic
	let persistent: PersistentServerState[T]	// holds the log
	let volatile: VolatileServerState
	var leader: (VolatileLeaderState | None)

	var _mode: RaftMode
	var _lastKnownLeader: U16
	var _mode_timer: Timer tag

	// FIXME need to provide a way for registering replicas with each other (fixed at first, cluster changes later)

	new create(id: U16, machine: StateMachine[T] iso, timers: Timers, network: Network[T], peers: Array[U16] val) =>
		_id = id
		_timers = timers
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
		// raft servers start as followers
		let mt: Timer iso = Timer(object iso is TimerNotify end, 200_000_000, 200_000_000) // 200ms
		_mode_timer = mt
		_mode = Follower
		_start_follower()

	fun ref _start_follower() =>
		_mode = Follower
		// cancel any previous timers
		_timers.cancel(_mode_timer)

		// create a timer to become a candidate if no append-entries heart beat is received
		// TODO need to randomise the timeout
		let mt: Timer iso = Timer(object iso is TimerNotify end, 200_000_000, 200_000_000) // 200ms
		_mode_timer = mt
		_timers(consume mt)
		None

	fun ref _start_candidate() =>
		_mode = Candidate
		// cancel any previous timers
		_timers.cancel(_mode_timer)

		// create a timer for an election timeout to start a new election
		// TODO need to randomise the timeout
		let mt: Timer iso = Timer(object iso is TimerNotify end, 200_000_000, 200_000_000) // 200ms
		_mode_timer = mt
		_timers(consume mt)

	fun ref _start_leader() =>
		_mode = Leader
		// cancel any previous timers
		_timers.cancel(_mode_timer)

		// set up timer to send out for append-entries heart beat
		// TODO
		let mt: Timer iso = Timer(object iso is TimerNotify end, 500_000_000, 500_000_000) // 500ms
		_mode_timer = mt
		_timers(consume mt)

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
