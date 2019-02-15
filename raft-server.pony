use "collections"
use "random"
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

// -- parameterise the server

interface StateMachine[T: Any #send]
	"""
	A statemachine manages the internal state transitions that are specific to the
	application logic. A raft essentially drives the state machine once the event
	messages are committed to the journal.
	"""
	fun ref accept(command: T) => None

// -- keep track of server mode

primitive Follower
primitive Candidate
primitive Leader

type RaftMode is (Follower | Candidate | Leader)

// -- trigger timeout logic

primitive ElectionTimeout
	""" Raised in a follower when it does not receive heartbeats and should start its own election. """
primitive HeartbeatTimeout
	""" Raised in a leader when it should publish heartbeats to its followers. """
primitive CanvasTimeout
	""" Raised in a candidate when it fails to canvas enough votes. """

type RaftTimeout is (ElectionTimeout | HeartbeatTimeout | CanvasTimeout)

// -- the transport

type RaftNetwork[T: Any val] is Network[RaftSignal[T]]
type RaftEndpoint[T: Any val] is Endpoint[RaftSignal[T]]

actor NopRaftEndpoint[T: Any val] is RaftEndpoint[T]
	be apply(msg: RaftSignal[T]) => None
	be stop() => None

// -- tracing

interface val RaftServerMonitor

	// -- follow incoming chatter
	fun val vote_req(id: NetworkAddress, signal: VoteRequest val) => None
	fun val vote_res(id: NetworkAddress, signal: VoteResponse) => None
	fun val append_req(id: NetworkAddress) => None
	fun val append_res(id: NetworkAddress) => None
	fun val command_req(id: NetworkAddress) => None
	fun val command_res(id: NetworkAddress) => None
	fun val install_req(id: NetworkAddress) => None
	fun val install_res(id: NetworkAddress) => None

	// -- follow internal state changes and timeouts
	fun val timeout_raised(timeout: RaftTimeout) => None
	fun val state_changed(mode: RaftMode, term: U64) => None

class val NopRaftServerMonitor is RaftServerMonitor

interface tag RaftRaisable
	be raise(timeout: RaftTimeout) => None

// -- the server

actor RaftServer[T: Any val] is RaftEndpoint[T]

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

	let _lower_election_timeout: U64 = 150_000_000 // 150 ms
	let _upper_election_timeout: U64 = 300_000_000 // 300 ms
	let _repeat_election_timeout: U64 = 200_000_000 // 300 ms

	let _rand: Random
	let _monitor: RaftServerMonitor
	let _timers: Timers
	let _network: RaftNetwork[T]
	let _id: NetworkAddress
	let _peers: Array[NetworkAddress]
	let _machine: StateMachine[T] iso					// implements the application logic

	let persistent: PersistentServerState[T]	// holds the log
	let volatile: VolatileServerState
	var leader: (VolatileLeaderState | None)

	var _mode: RaftMode
	var _mode_timer: Timer tag
	var _lastKnownLeader: NetworkAddress

	// FIXME need to provide a way for registering replicas with each other (fixed at first, cluster changes later)

	new create(id: NetworkAddress, machine: StateMachine[T] iso
		, timers: Timers
		, network: RaftNetwork[T]
		, peers: Array[NetworkAddress] val
		, monitor: RaftServerMonitor = NopRaftServerMonitor
		) =>

		// seed the random number generator
		(let sec: I64, let nsec: I64) = Time.now()
		_rand = Rand(sec.u64(), nsec.u64())

		_monitor = monitor
		_id = id
		_timers = timers
		_network = network

		// copy peers but remove self
		_peers = Array[NetworkAddress](peers.size() - 1)
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

		// set up basic internal state and the state machine
		_machine = consume machine
		persistent = PersistentServerState[T]
		volatile = VolatileServerState
		leader = None
		_lastKnownLeader = 0
		_mode = Follower
		_mode_timer = Timer(object iso is TimerNotify end, 1, 1)
		persistent.current_term = 0

		// start in follower mode
		_start_follower()

	be stop() =>
		_timers.cancel(_mode_timer)

	be apply(signal: RaftSignal[T]) =>
		match consume signal
		| let s: CommandEnvelope[T] => _process_command(consume s)
		| let s: VoteRequest => _process_vote_request(consume s)
		| let s: VoteResponse => _process_vote_response(consume s)
		| let s: AppendEntriesRequest[T] => _process_append_entries_request(consume s)
		| let s: AppendEntriesResult => _process_append_entries_result(consume s)
		| let s: InstallSnapshotRequest => _process_install_snapshot_request(consume s)
		| let s: InstallSnapshotResponse => _process_install_snapshot_response(consume s)
		end

	be raise(timeout: RaftTimeout) =>
		// TODO consider just ignoring timeout signals that don't match the current mode
		_monitor.timeout_raised(timeout)
		match timeout
		| (let t: ElectionTimeout) => None
		| (let t: CanvasTimeout) => None
		| (let t: HeartbeatTimeout) => None
		end

	// -- internals

	// -- -- client command
	fun ref _process_command(command: CommandEnvelope[T]) =>
		_monitor.command_req(_id)
		let c: CommandEnvelope[T] = consume command
		let cmd: T val = c.command
		""" Accept a new command from a client. """
		match _mode
		| Follower	=> _accept_follower(consume cmd)
		| Candidate	=> _accept_candidate(consume cmd)
		| Leader		=> _accept_leader(consume cmd)
		end

	// -- -- votes

	fun ref _process_vote_request(votereq: VoteRequest) =>
		""" See Raft §5.2 """
		_monitor.vote_req(_id, votereq)
		let ires: VoteResponse iso = recover iso VoteResponse end
		ires.term = persistent.current_term
		if votereq.term < persistent.current_term then
			ires.vote_granted = false
		else
			// check if we could potentially vote for this candidate
			let could_vote: Bool = match persistent.voted_for
				| let s: None => true
				| let s: NetworkAddress => s == votereq.candidate_id
				end
			ires.vote_granted = if could_vote then
					// check if the candidate's log is as up-to-date as what we have here
					if (votereq.last_log_term >= persistent.current_term)
						and (votereq.last_log_index >= volatile.commit_index) then
							persistent.voted_for = votereq.candidate_id
							true
					else
						// candidate is not up to date
						false
					end
				else
					// already voted for someone else
					false
				end
		end
		let res: VoteResponse val = consume ires
		_network.send(votereq.candidate_id, res)

	fun ref _process_vote_response(voteres: VoteResponse) =>
		_monitor.vote_res(_id, voteres)
		None

  // -- -- apending
	fun ref _process_append_entries_request(appendreq: AppendEntriesRequest[T]) =>
		_monitor.append_req(_id)
		// decide if this request should be honoured (we might be ahead in a new term)
		// if accepted, the perform mode changes (we might be behind and should bow to a new leader)
		// if accepted, reset timers (we might have received a heartbeat so we can chill out for now)
		// if accepted, then actually append and process the log agains the state machine
		None

	fun ref _process_append_entries_result(appendreq: AppendEntriesResult) =>
		_monitor.append_res(_id)
		None

	// -- -- snapshots
	fun ref _process_install_snapshot_request(snapshotreq: InstallSnapshotRequest) =>
		_monitor.install_res(_id)
		None

	fun ref _process_install_snapshot_response(snapshotres: InstallSnapshotResponse) =>
		_monitor.install_req(_id)
		None

	// -- -- consensus module

	fun ref _set_mode(mode: RaftMode) =>
		_mode = mode
		_monitor.state_changed(_mode, persistent.current_term)

	fun ref _start_follower() =>
		"""
		Follower state:

		The follower will honour heartbeats and log updates, but will also set a timer to potentially
		start its own election.
		"""
		_set_mode(Follower)

		// randomise the timeout between [150,300) ms
		let swash = _swash(_lower_election_timeout, _upper_election_timeout)

		// create a timer to become a candidate if no append-entries heart beat is received
		let mt: Timer iso = Timer(_Timeout(this, ElectionTimeout), swash, _repeat_election_timeout)
		_timers.cancel(_mode_timer) // cancel any previous timers
		_mode_timer = mt
		_timers(consume mt)

	fun ref _swash(lower: U64, upper: U64): U64 =>
		// randomise the timeout between [150,300) ms
		lower + _rand.int(upper - lower)

	fun ref _start_candidate() =>
		_set_mode(Candidate)
		// cancel any previous timers
		_timers.cancel(_mode_timer)

		// create a timer for an election timeout to start a new election
		// TODO need to randomise the timeout
		let mt: Timer iso = Timer(object iso is TimerNotify end, 200_000_000, 200_000_000) // 200ms
		_mode_timer = mt
		_timers(consume mt)

	fun ref _start_leader() =>
		_set_mode(Leader)
		// cancel any previous timers
		_timers.cancel(_mode_timer)

		// set up timer to send out for append-entries heart beat
		// TODO
		let mt: Timer iso = Timer(object iso is TimerNotify end, 500_000_000, 500_000_000) // 500ms
		_mode_timer = mt
		_timers(consume mt)

	// -- command ingress

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

class _Timeout is TimerNotify
	""" A common timeout handler that will raise a signal back with the replica. """

	let _raisable: RaftRaisable
	let _signal: RaftTimeout

	new iso create(raisable: RaftRaisable, signal: RaftTimeout) =>
		_raisable = raisable
		_signal = signal

	fun ref apply(timer: Timer, count: U64): Bool =>
		_raisable.raise(_signal)
		true
