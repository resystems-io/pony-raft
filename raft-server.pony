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
	fun text():String val => "follower"
primitive Candidate
	fun text():String val => "candidate"
primitive Leader
	fun text():String val => "leader"

type RaftMode is (Follower | Candidate | Leader)
type RaftTerm is U64
type RaftIndex is USize

// -- trigger timeout logic

primitive ElectionTimeout
	""" Raised in a follower when it does not receive heartbeats and should start its own election. """
primitive HeartbeatTimeout
	""" Raised in a leader when it should publish heartbeats to its followers. """
primitive CanvasTimeout
	""" Raised in a candidate when it fails to canvas enough votes. """

type RaftTimeout is (ElectionTimeout | HeartbeatTimeout | CanvasTimeout)

// -- the transport

type RaftEndpoint[T: Any val] is Endpoint[RaftSignal[T]]

actor NopRaftEndpoint[T: Any val] is RaftEndpoint[T]
	be apply(msg: RaftSignal[T]) => None
	be stop() => None

// -- tracing

interface iso RaftServerMonitor

	// -- follow incoming chatter
	fun ref vote_req(id: NetworkAddress, signal: VoteRequest val) => None
	fun ref vote_res(id: NetworkAddress, signal: VoteResponse) => None
	fun ref append_req(id: NetworkAddress) => None
	fun ref append_res(id: NetworkAddress) => None
	fun ref command_req(id: NetworkAddress) => None
	fun ref command_res(id: NetworkAddress) => None
	fun ref install_req(id: NetworkAddress) => None
	fun ref install_res(id: NetworkAddress) => None

	// -- follow internal state changes and timeouts
	fun ref timeout_raised(timeout: RaftTimeout) => None
	fun ref state_changed(mode: RaftMode, term: RaftTerm) => None
	fun ref append_accepted(leader_id: NetworkAddress, term: RaftTerm , last_index: RaftIndex, success: Bool) =>
		"""last_index: the highest index seen by the replica, but not necessarily applied or committed. """
		None

class iso NopRaftServerMonitor is RaftServerMonitor

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

	/*
		§9.3 "The leader was crashed uniformly randomly within
		its heartbeat interval, which was half of the minimum
		election timeout for all tests."
	*/
	let _hearbeat_timeout: U64 = 75_000_000 // 75 ms

	let _rand: Random
	let _monitor: RaftServerMonitor iso
	let _timers: Timers
	let _network: Network[RaftSignal[T]]
	let _id: NetworkAddress
	let _majority: USize
	let _peers: Array[NetworkAddress]
	let _machine: StateMachine[T] iso					// implements the application logic

	let persistent: PersistentServerState[T]	// holds the log
	let volatile: VolatileServerState

	var candidate: VolatileCandidateState
	var leader: (VolatileLeaderState | None)

	var _mode: RaftMode
	var _mode_timer: Timer tag
	var _lastKnownLeader: NetworkAddress

	// FIXME need to provide a way for registering replicas with each other (fixed at first, cluster changes later)

	new create(id: NetworkAddress, machine: StateMachine[T] iso
		, timers: Timers
		, network: Network[RaftSignal[T]]
		, peers: Array[NetworkAddress] val
		, start_command: T // used to put the zeroth entry into the log (Raft officially starts at 1)
		, monitor: RaftServerMonitor iso = NopRaftServerMonitor
		, initial_term: RaftTerm = 0 // really just for testing
		) =>

		// seed the random number generator
		(let sec: I64, let nsec: I64) = Time.now()
		_rand = Rand(sec.u64(), nsec.u64())

		_monitor = consume monitor
		_id = id
		_timers = timers
		_network = network

		// copy peers but remove self
		_majority = peers.size().shr(1) + 1 // we assume that the peer set odd in size
		_peers = try ArrayWithout[NetworkAddress].without(id, peers)? else [as NetworkAddress: id] end

		// record the initial state machine
		_machine = consume machine

		// set up basic internal state
		persistent = PersistentServerState[T]
		persistent.log.push(recover val Log[T](0,start_command) end)
		volatile = VolatileServerState
		candidate = VolatileCandidateState
		leader = None

		_lastKnownLeader = 0
		_mode = Follower
		_mode_timer = Timer(object iso is TimerNotify end, 1, 1)
		persistent.current_term = initial_term

		// start in follower mode
		_start_follower(initial_term)

	be stop() =>
		_timers.cancel(_mode_timer)

	be apply(signal: RaftSignal[T]) => // FIXME this should be limited to RaftServerSignal[T]
		match signal
		| (let s: RaftServerSignal[T]) => absorb(s)
		else
			None // ignore non-server signals
		end

	fun ref absorb(signal: RaftServerSignal[T]) =>
		// if any RPC has a larger 'term' then convert to and continue as a follower (§5.1)
		match signal
		| (let ht: HasTerm) =>
			if persistent.current_term < ht.signal_term() then
				_start_follower(ht.signal_term())
			end
		end

		match consume signal
		// signal from the client with a command
		| let s: CommandEnvelope[T] => _process_command(consume s) // note, no matching ResponseEnvelope

		// raft coordindation singals
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
		| (let t: ElectionTimeout) => _start_candidate()
		| (let t: CanvasTimeout) => _start_election()
		| (let t: HeartbeatTimeout) => _emit_heartbeat()
		end

	// -- internals

	// -- -- client command

	fun ref _process_command(command: CommandEnvelope[T]) =>
		""" Accept a new command from a client. """
		_monitor.command_req(_id)
		match _mode
		| Follower	=> _accept_follower(consume command)
		| Candidate	=> _accept_candidate(consume command)
		| Leader		=> _accept_leader(consume command)
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
					// check if the candidate's log is at least as up-to-date as what we have here
					if _peer_up_to_date(votereq.last_log_term, votereq.last_log_index) then
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
		if (_mode isnt Candidate) then return end // ignore late vote responses
		candidate.vote(voteres.vote_granted)
		// see if we got a majority
		if candidate.votes >= _majority then
			_start_leader()
		end

  // -- -- apending

	/*
	 * See §5.3 Log Replication
	 *
	 * "To bring a follower’s log into consistency with its own,
	 * the leader must find the latest log entry where the two
	 * logs agree, delete any entries in the follower’s log after
	 * that point, and send the follower all of the leader’s entries
	 * after that point. All of these actions happen in response
	 * to the consistency check performed by AppendEntries
	 * RPCs. The leader maintains a nextIndex for each follower,
	 * which is the index of the next log entry the leader will
	 * send to that follower. When a leader first comes to power,
	 * it initializes all nextIndex values to the index just after the
	 * last one in its log (11 in Figure 7). If a follower’s log is
	 * inconsistent with the leader’s, the AppendEntries consistency
	 * check will fail in the next AppendEntries RPC. After a
	 * rejection, the leader decrements nextIndex and retries
	 * the AppendEntries RPC. Eventually nextIndex will reach
	 * a point where the leader and follower logs match. When
	 * this happens, AppendEntries will succeed, which removes
	 * any conflicting entries in the follower’s log and appends
	 * entries from the leader’s log (if any). Once AppendEntries
	 * succeeds, the follower’s log is consistent with the leader’s,
	 * and it will remain that way for the rest of the term."
	 */

	fun ref _process_append_entries_request(appendreq: AppendEntriesRequest[T]) =>
		_monitor.append_req(_id)
		// decide if we should actually just become a follower (and bow to a new leader)
		let convert_to_follower: Bool = if
			((_mode is Candidate) and (appendreq.term >= persistent.current_term)) then
				// candidate saw a new leader in the same term
				true
			elseif (appendreq.term > persistent.current_term) then
				// non-candidate saw a higher term
				true
			else
				false
			end

		if convert_to_follower then
			// convert to a follower and continue to process othe signal
			_start_follower(appendreq.term)
		end

		// decide if this request should be honoured (we might be ahead in a new term)
		if (appendreq.term < persistent.current_term) then
			_emit_append_res(appendreq.leader_id, false)
			return
		end

		// check if we should reply false if log _doesn't_ contain an entry at .prev_log_index
		// whose term matches .prev_log_term
		(let has_prev_index: Bool, let has_prev_term: Bool) = try
				let t: Log[T] val = persistent.log(appendreq.prev_log_index)?
				(true, t.term == appendreq.prev_log_term) // true or false here
			else
				(false, false)
			end

		if not has_prev_term then
			_emit_append_res(appendreq.leader_id, false)
			return
		end

		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it (§5.3).
		// (Note we can assume that conflicts can not happen bellow the current commit index.)
		// (Note, we've already checked that there is no "conflict" at `prev_log_index`)
		var idx: RaftIndex = appendreq.prev_log_index + 1
		var log_end: RaftIndex = persistent.log.size()
		let append_len: USize = appendreq.entries.size()
		let append_end: RaftIndex = idx + append_len
		try
			while (idx < append_end) and (idx < log_end) do
					let off: RaftIndex = idx - append_len - 1
					let al: Log[T] val = appendreq.entries(off)?
					let rl: Log[T] val = persistent.log(idx)?
					if (al.term == rl.term) then
						idx = idx + 1
					else
						// conflict detected drop the remainder from the follower's log
						persistent.log.truncate(idx)
						break
					end
			end
		end

		// Append any new entries not already in the log.
		// (these may be committed or uncomitted i.e. we may get ahead of the commit index)
		// (at this point there should be no conflicting entries, so we can just append)
		// (we compute the overlap relative to the shared 'prev_log_index')
		// (note persistent log size is ≥ 1 since we start with one element)
		let overlap = persistent.log.size() - appendreq.prev_log_index - 1 // always >= 0
		// check that there will be anything to append
		if (overlap < appendreq.entries.size()) then
			let remaining = appendreq.entries.size() - overlap
			appendreq.entries.copy_to(persistent.log, overlap, persistent.log.size(), remaining)
		end

		// if .leader_commit > commit_index,
		// set commit_index = min(.leader_commit, index of last new entry)
		if (appendreq.leader_commit > volatile.commit_index) then
			// TODO review calculation of 'last_new_one'
			let last_new_one: USize = appendreq.prev_log_index + appendreq.entries.size()
			volatile.commit_index = appendreq.leader_commit.min(last_new_one)
		end

		// respond 'true' to the leader
		// (FIXME since we are processing asynchronously we need a correlation ID or more state)
		// (might be able to use the follower ID and log index values?)
		_emit_append_res(appendreq.leader_id, true)

		// if accepted, reset timers (we might have received a heartbeat so we can chill out for now)
		if not convert_to_follower then
			// no need to do this if we already converted to a follower (since that resets the timers)
			// but now we still want to reset the timers
			_start_follower_timer()
		end

		// now update the state machine if need be
		_apply_logs_to_state_machine()

	fun ref _apply_logs_to_state_machine() =>
		"""
		Update the state machine by applying logs from after 'last_applied' up to and including
		'commit_index'. However, only the leader can reply to the client.
		"""
		// TODO _machine.accept(...)
		None

	fun ref _emit_append_res(leader_id: NetworkAddress, success: Bool) =>
		// notify the leader
		let reply: AppendEntriesResult iso = recover iso AppendEntriesResult end
		reply.term = persistent.current_term
		reply.success = success
		let msg: AppendEntriesResult val = consume reply
		_network.send(leader_id, msg)
		// notify the monitor
		_monitor.append_accepted(leader_id, msg.term, persistent.log.size()-1, msg.success)

	fun ref _process_append_entries_result(appendreq: AppendEntriesResult) =>
		_monitor.append_res(_id)
		// TODO

	// -- -- snapshots

	fun ref _process_install_snapshot_request(snapshotreq: InstallSnapshotRequest) =>
		_monitor.install_res(_id)
		// TODO

	fun ref _process_install_snapshot_response(snapshotres: InstallSnapshotResponse) =>
		_monitor.install_req(_id)
		// TODO

	// -- -- consensus module

	fun ref _emit_heartbeat() =>
		// here we simply emit empty append entry logs
		// (we could actually maintain timers per peer
		// and squash hearbeats if non-trival appends are sent)
		for p in _peers.values() do
			let append: AppendEntriesRequest[T] iso = recover iso AppendEntriesRequest[T] end
			append.term = persistent.current_term
			append.prev_log_index = 0 // FIXME needs to be tracked per peer
			append.prev_log_term = 0 // FIXME
			append.leader_commit = volatile.commit_index
			append.leader_id = _id
			append.entries.clear() // Note, entries is `iso`

			_network.send(p, consume append)
		end

	fun box _peer_up_to_date(peer_last_log_term: RaftTerm, peer_last_log_index: RaftIndex): Bool =>
		"""
		Determine if the peer log is more up-to-date than this replica.

		§5.4.1 "Raft determines which of two logs is more up-to-date
			by comparing the index and term of the last entries in the
			logs. If the logs have last entries with different terms, then
			the log with the later term is more up-to-date. If the logs
			end with the same term, then whichever log is longer is
			more up-to-date."

		"""
		try
			// fetch our last log entry
			let last_log: Log[T] box = persistent.log(volatile.commit_index)?
			// check how has seen the latest term
			if (last_log.term < peer_last_log_term) then
				true
			elseif (last_log.term > peer_last_log_term) then
				false
			else
				// last terms are equal so check the commit index
				if (peer_last_log_index >= volatile.commit_index) then
					true
				else
					false
				end
			end
		else
			// hmmm, our log probably empty (maybe we should correct our commit index?)
			true
		end

	fun ref _start_follower(term: RaftTerm) =>
		"""
		Follower state:

		The follower will honour heartbeats and log updates, but will also set a timer to potentially
		start its own election.
		"""
		leader = None // clear any potential leader state
		persistent.current_term = term
		_set_mode(Follower)
		_start_follower_timer()

	fun ref _start_follower_timer() =>
		// randomise the timeout between [150,300) ms
		let swash = _swash(_lower_election_timeout, _upper_election_timeout)

		// create a timer to become a candidate if no append-entries heart beat is received
		let mt: Timer iso = Timer(_Timeout(this, ElectionTimeout), swash, _repeat_election_timeout)
		_set_timer(consume mt)

	fun ref _start_candidate() =>
		// check that we are not already a candidate
		if (_mode is Candidate) then return end

		leader = None // clear any potential leader state (should have been cleared as a follower)
		_set_mode(Candidate)

		// start a new election
		_start_election()

	fun ref _start_election() =>
		// only candidates can run elections
		if (_mode isnt Candidate) then return end

		// new term, reinitialise the vote register and vote for self
		persistent.current_term = persistent.current_term + 1
		persistent.voted_for = _id
		candidate = VolatileCandidateState
		candidate.vote()
		// send vote requests to other replicas
		for p in _peers.values() do
			let canvas: VoteRequest iso = recover iso VoteRequest end
			canvas.term = persistent.current_term
			canvas.candidate_id = _id

			// TODO review are the log_term and log_index being set correctly?
			canvas.last_log_term = try persistent.log(volatile.commit_index.usize())?.term else RaftTerm(0) end
			canvas.last_log_index = volatile.commit_index

			_network.send(p, consume canvas)
		end

		// randomise the timeout between [150,300) ms
		let swash = _swash(_lower_election_timeout, _upper_election_timeout)

		// create a timer for an election timeout to start a new election
		let mt: Timer iso = Timer(_Timeout(this, CanvasTimeout), swash, _repeat_election_timeout)
		_set_timer(consume mt)

	fun ref _start_leader() =>
		leader = VolatileLeaderState
		_set_mode(Leader)

		// set up timer to send out for append-entries heart beat
		let mt: Timer iso = Timer(_Timeout(this, HeartbeatTimeout), 0, _hearbeat_timeout)
		_set_timer(consume mt)

	fun ref _set_timer(mt: Timer iso) =>
		_timers.cancel(_mode_timer) // cancel any previous timers before recording a new one
		_mode_timer = mt
		_timers(consume mt)

	fun ref _set_mode(mode: RaftMode) =>
		_mode = mode
		_monitor.state_changed(_mode, persistent.current_term)

	fun ref _swash(lower: U64, upper: U64): U64 =>
		// randomise the timeout between [150,300) ms
		lower + _rand.int(upper - lower)

	// -- command ingress

	fun ref _accept_follower(command: CommandEnvelope[T]) =>
		// follower  - redirect command to the leader
		//             (leader will reply, leader may provide backpressure)
		// TODO
		None

	fun ref _accept_candidate(command: CommandEnvelope[T]) =>
		// candidate - queue the command until transitioning to being a leader or follower
		//             (honour ttl and generate dropped message signals)
		//             (send backpressure if the queue gets too large)
		//             (batch queued message to the leader)
		// TODO
		None

	fun ref _accept_leader(command: CommandEnvelope[T]) =>
		// leader    - apply commands to the journal log and distribute them to followers
		//             (may generate backpressure if the nextIndex vs matchedIndex vs log
		//              starts to get too large)
		// TODO
		let c: CommandEnvelope[T] = consume command
		let cmd: T val = c.command
		let source: NetworkAddress val = c.source
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

primitive ArrayWithout[T: Equatable[T val] val]

	fun val without(t: T, v: Array[T] val): Array[T] ? =>
		let w: Array[T] iso = recover iso Array[T](v.size() - 1) end
		var i: USize = 0
		while (i < v.size()) do
			let v': T = v(i)?
			if v' != t then
				w.push(v')
			end
			i = i+1
		end
		consume w
