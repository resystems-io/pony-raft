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

// -- keep track of server mode

primitive Follower
	fun text():String val => "follower"
primitive Candidate
	fun text():String val => "candidate"
primitive Leader
	fun text():String val => "leader"

type RaftMode is (Follower | Candidate | Leader)

primitive Paused
	"""
	Signaled when a raft server stops processing events.

	The server will stop processing any events:
	- internal timers will be reset and ignored
	- client commands will be ignored
	- peer messages will be ignored

	The server's volatile and persistent state will be left as-is.
	"""
	fun text():String val => "paused"
primitive Resumed
	"""
	Signaled when a raft server starts processing events again.

	The server will now start handling messages again, in accordance
	with whatever volatile and persistent state it has.
	"""
	fun text():String val => "resumed"
primitive ResetVolatile
	"""
	Signaled when a raft server resets its volatile state.

	- All timers are reset.
	- All volatile state is cleared
	  - (including the state-machine).
	"""
	fun text():String val => "reset-volatile"
primitive ResetPersistent
	"""
	Signaled with a raft server performs a reset of stored logs.

	- Implies a volatile reset.
	- All persistent, non-snapshot, state is cleared.
	- Recovery is relative to the potential snapshot data, together with
	  data from other replicas.
	"""
	fun text():String val => "reset-persistent"
primitive ResetSnapshot
	"""
	Signaled with a raft server performs a reset of its snapshots.

	- Implies a persistent reset (and therefore also a volitile reset).
	- All snapshot data is removed.
	- Recovery depends fully on the data available from other replicas.
	"""
	fun text():String val => "reset-snapshot"

type RaftReset is (ResetVolatile | ResetPersistent | ResetSnapshot)

type RaftProcessing is (Paused | Resumed)

type RaftControl is (RaftReset | RaftProcessing)

// -- trigger timeout logic

primitive ElectionTimeout
	"""
	Raised in a follower when it does not receive heartbeats
	and it should become a candidate and start its own election.
	"""
primitive CanvasTimeout
	"""
	Raised in a candidate when it fails to canvas enough votes
	and it should run a new election.
	"""
primitive HeartbeatTimeout
	"""
	Raised in a leader when it should publish heartbeats to its followers,
	pottentially also appending log entries.
	"""
primitive StuckTimeout
	"""
	[Not a Raft Protocal Timeout] - raised when the leader failes to make progress.

	Raised in a leader if the commit index is not progressed for too long after
	the log being extended.
	"""

type RaftTimeout is (ElectionTimeout | HeartbeatTimeout | CanvasTimeout)

// -- the transport

type RaftEndpoint[T: Any val] is Endpoint[RaftSignal[T]]

actor NopRaftEndpoint[T: Any val] is RaftEndpoint[T]
	be apply(msg: RaftSignal[T]) => None
	be stop() => None

// -- tracing

interface iso RaftServerMonitor[T: Any val]
	"""
	A monitor to trace raft server processing.

	This will be informed of processing steps made by the raft server.
	"""

	// -- follow incoming chatter that is recevied by a server
	fun ref vote_req(id: NetworkAddress, signal: VoteRequest val) => None
	fun ref vote_res(id: NetworkAddress, signal: VoteResponse) => None
	fun ref append_req(id: NetworkAddress, signal: AppendEntriesRequest[T] val) => None
	fun ref append_res(id: NetworkAddress, signal: AppendEntriesResult) => None
	fun ref install_req(id: NetworkAddress, signal: InstallSnapshotRequest val) => None
	fun ref install_res(id: NetworkAddress, signal: InstallSnapshotResponse) => None

	// -- follow client chatter
	fun ref command_req(id: NetworkAddress) => None
	fun ref command_res(id: NetworkAddress) => None

	// -- follow internal state changes and timeouts
	fun ref timeout_raised(id: NetworkAddress, timeout: RaftTimeout) => None
	fun ref mode_changed(id: NetworkAddress, mode: RaftMode, term: RaftTerm) =>
		"""
		Raised when the server's Raft mode changes.

		This can be one of follower, candiate or leader.
		"""
		None
	fun ref append_accepted(id: NetworkAddress
		, current_term: RaftTerm
		, last_applied_index: RaftIndex
		, commit_index: RaftIndex
		, last_log_index: RaftIndex

		, leader_term: RaftTerm
		, leader_id: NetworkAddress
		, leader_commit_index: RaftIndex
		, leader_prev_log_index: RaftIndex
		, leader_prev_log_term: RaftTerm
		, leader_entry_count: USize

		, applied: Bool // true if these
		) =>
		"""
		Raised when this replica accepts a log entry into is log.

		Note, this does not imply that the log entry has been applied to the state machine.

		last_index: the highest index seen by the replica, but not necessarily applied or committed.
		"""
		None
	fun ref control_raised(id: NetworkAddress, control: RaftControl) =>
		"""
		Raised when this replica's internal processing and control state is changed.

		This can be a volatile reset, a persistent reset or a snapshot reset. Additionally,
		this can be a pause or resume.
		"""
		None
	fun ref state_change(id: NetworkAddress
		, mode: RaftMode								// the operational mode of this server
		, current_term: RaftTerm				// the current term in which the server is serving
		, last_applied_index: RaftIndex	// the last index that was applied to the state machine
		, commit_index: RaftIndex				// the last log index known to be committed in the cluster
		, last_log_index: RaftIndex			// the last log entry held by the server

		, update_log_index: RaftIndex		// the index of the local log that is now being applied to the state-machine
		) =>
		"""
		Raised when the replica is publishing a log command to be processed by the state machine.

		Note, `update_log_index` should always equal `(last_applied_index + 1)`.
		"""
			None

class iso NopRaftServerMonitor[T: Any val] is RaftServerMonitor[T]

interface tag RaftRaisable
	"""
	A receiver of raft server timeout notifications.
	"""
	be raise(timeout: RaftTimeout) => None

// -- the server

actor RaftServer[T: Any val, U: Any #send] is RaftEndpoint[T]

	"""
	Each raft server runs concurrently and coordinates with the other servers in the raft.
	This coordination then manages the server consensus states (leader/follower/candidate).
	Additionally, the server maintains the log in the persistent state. Finally, the server
	delegates committed log entries, containing commands of type T, to the application
	specific state machine.

	Log entries are considered commited when they are safe to be applied to the state-machine.
	That is, when a majority of the servers have appended the entry to their logs, and acknowledged
	that inclusion.

	The servers in the raft communicate via the network.

	The application state machine can then communicate directly with clients. Whereas,
	clients would communicate with one of the raft servers (and not with the application
	state machine).
	"""

	let _lower_election_timeout: U64	= 150_000_000 // 150 ms
	let _upper_election_timeout: U64	= 300_000_000 // 300 ms
	let _repeat_election_timeout: U64	= 200_000_000 // 300 ms

	/*
		§9.3 "The leader was crashed uniformly randomly within
		its heartbeat interval, which was half of the minimum
		election timeout for all tests."
	*/
	let _hearbeat_timeout: U64 = 75_000_000 // 75 ms

	let _rand: Random
	let _timers: Timers

	let _id: NetworkAddress
	let _transport: Transport[RaftSignal[T]]
	let _majority: USize
	let _peers: Array[NetworkAddress]					// other servers in the raft

	let _monitor: RaftServerMonitor[T] iso
	let _machine: StateMachine[T,U] iso					// implements the application logic

	let persistent: PersistentServerState[T]	// holds the log which would be persisted to non-volatile storage
	let volatile: VolatileServerState

	var candidate: VolatileCandidateState
	var leader: (VolatileLeaderState | None)

	var _mode: RaftMode
	var _mode_timer: Timer tag
	var _lastKnownLeader: NetworkAddress

	// FIXME need to provide a way for registering replicas with each other (fixed at first, cluster changes later)

	new create(id: NetworkAddress
		, timers: Timers
		, network: Transport[RaftSignal[T]]
		, peers: Array[NetworkAddress] val
		, machine: StateMachine[T,U] iso
		, start_command: T // used to put the zeroth entry into the log (Raft officially starts at 1)
		, monitor: RaftServerMonitor[T] iso = NopRaftServerMonitor[T]
		, initial_term: RaftTerm = 0 // really just for testing
		// TODO we should be able to pass in an iso PersistentServerState (then we won't see an implicit persistent reset)
		) =>

		// seed the random number generator
		(let sec: I64, let nsec: I64) = Time.now()
		_rand = Rand(sec.u64(), nsec.u64())

		// time keeping
		_timers = timers

		// instrumentation
		_monitor = consume monitor

		// networking
		_id = id
		_transport = network

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
		_mode_timer = Timer(object iso is TimerNotify end, 1, 1) // will expire soon
		persistent.current_term = initial_term

		// signal resume
		// TODO

		// start in follower mode
		_start_follower(initial_term)

	// -- shutdown

	be stop() =>
		_clear_timer() // FIXME this is not necessary once the control calls work
		control([as RaftControl: Paused; ResetVolatile])

	be control(ctls: Array[RaftControl] val) =>
		"""
		Perform any control operations listed.

		We provide and array of control requests so that the composition
		can be handled atomically.
		"""
		None

	// -- processing

	be apply(signal: RaftSignal[T]) => // FIXME this should be limited to RaftServerSignal[T]
		match signal
		| (let s: RaftServerSignal[T]) => absorb(s)
		else
			// TODO if not fixed at compile time then hook in a monitor call with a warning
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
		| let s: CommandEnvelope[T] => _monitor.command_req(_id); _process_client_command(consume s) // note, no matching ResponseEnvelope

		// raft coordindation singals
		| let s: VoteRequest							=> _monitor.vote_req(_id, s);			_process_vote_request(consume s)
		| let s: VoteResponse							=> _monitor.vote_res(_id, s);			_process_vote_response(consume s)
		| let s: AppendEntriesRequest[T]	=> _monitor.append_req(_id, s);		_process_append_entries_request(consume s)
		| let s: AppendEntriesResult			=> _monitor.append_res(_id, s);		_process_append_entries_result(consume s)
		| let s: InstallSnapshotRequest		=> _monitor.install_req(_id, s);	_process_install_snapshot_request(consume s)
		| let s: InstallSnapshotResponse	=> _monitor.install_res(_id, s);	_process_install_snapshot_response(consume s)
		end

	be raise(timeout: RaftTimeout) =>
		// TODO consider just ignoring timeout signals that don't match the current mode
		_monitor.timeout_raised(_id, timeout)
		match timeout
		| (let t: ElectionTimeout)	=> _start_candidate()
		| (let t: CanvasTimeout)		=> _start_election()
		| (let t: HeartbeatTimeout)	=> _emit_heartbeat()
		end

	// -- internals

	// -- -- client command

	fun ref _process_client_command(command: CommandEnvelope[T]) =>
		""" Accept a new command from a client. """
		match _mode
		| Follower	=> _accept_command_as_follower(consume command)
		| Candidate	=> _accept_command_as_candidate(consume command)
		| Leader		=> _accept_command_as_leader(consume command)
		end

	// -- -- votes

	fun ref _process_vote_request(votereq: VoteRequest) =>
		""" See Raft §5.2 """
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
		_transport.unicast(votereq.candidate_id, res)

	fun ref _process_vote_response(voteres: VoteResponse) =>
		if (_mode isnt Candidate) then return end // ignore late vote responses
		candidate.vote(voteres.vote_granted)
		// see if we got a majority
		if candidate.votes >= _majority then
			_start_leader()
		end

  // -- -- apending

	fun ref _last_log_index(): RaftIndex =>
		let last_index: RaftIndex = persistent.log.size()-1
		last_index

	fun ref _commit_index(): RaftIndex =>
			volatile.commit_index

	fun ref _last_applied_index(): RaftIndex =>
			volatile.last_applied

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
			// convert to a follower and continue to process the signal
			_start_follower(appendreq.term)
		end

		// decide if this request should be honoured (we might be ahead in a new term)
		if (appendreq.term < persistent.current_term) then
			_emit_append_res(appendreq, false)
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
			// here we are asking the leader to rewind and send us earlier entries
			_emit_append_res(appendreq, false)
			return
		end

		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it (§5.3).
		// (Note we can assume that conflicts can not happen bellow the current commit index.)
		// (Note, we've already checked that there is no "conflict" at `prev_log_index`)
		//
		//  prev    |s|i| | | | | |a            - append logs rpc with l entries
		//          ---------------
		//   …| | | | |i| | |e                  - this replicas current log entries
		//   ----------------
		//
		//   s == append_start, i == idx, e == log_end, a == append_end, l == append_len
		let log_end: RaftIndex = persistent.log.size() // TODO we might need to offset this by the snapshot start
		let append_start: RaftIndex = appendreq.prev_log_index + 1
		let append_len: USize = appendreq.entries.size()
		let append_end: RaftIndex = append_start + append_len

		var idx: RaftIndex = append_start
		try
			while (idx < append_end) and (idx < log_end) do
					let off: RaftIndex = idx - append_start
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

		// if accepted, reset timers (we might have received a heartbeat so we can chill out for now)
		if not convert_to_follower then
			// no need to do this if we already converted to a follower (since that resets the timers)
			// but now we still want to reset the timers
			_start_follower_timer()
		end

		// respond 'true' to the leader
		// (FIXME since we are processing asynchronously we need a correlation ID or more state)
		// (might be able to use the follower ID and log index values?)
		_emit_append_res(appendreq, true)

		// now update the state machine if need be
		_apply_logs_to_state_machine()

	fun ref _apply_logs_to_state_machine() =>
		"""
		Update the state machine by applying logs from after 'last_applied' up to and including
		'commit_index'. However, only the leader can reply to the client.
		"""
		// TODO _machine.accept(...)
		None

	fun ref _emit_append_res(appendreq: AppendEntriesRequest[T], success: Bool) =>
		// notify the leader
		let reply: AppendEntriesResult iso = recover iso AppendEntriesResult end
		reply.term = persistent.current_term
		reply.success = success
		let msg: AppendEntriesResult val = consume reply
		_transport.unicast(appendreq.leader_id, msg)

		// notify the monitor of our decision to, or not to, incorpate the entry
		let last_index = _last_log_index()
		let commit_index = _commit_index()
		let last_applied_index = _last_applied_index()

		_monitor.append_accepted(_id
			where
				current_term = persistent.current_term
			, last_applied_index = _last_applied_index()
			, commit_index = _commit_index()
			, last_log_index = _last_log_index()

			, leader_term = appendreq.term
			, leader_id = appendreq.leader_id
			, leader_commit_index = appendreq.leader_commit
			, leader_prev_log_index = appendreq.prev_log_index
			, leader_prev_log_term = appendreq.prev_log_term
			, leader_entry_count = appendreq.entries.size()

			, applied = msg.success
		)


	fun ref _process_append_entries_result(appendreq: AppendEntriesResult) =>
		// TODO it seems like these results can be handled asynchronously relative to the append req
		None

	// -- -- snapshots

	fun ref _process_install_snapshot_request(snapshotreq: InstallSnapshotRequest) =>
		// TODO
		None

	fun ref _process_install_snapshot_response(snapshotres: InstallSnapshotResponse) =>
		// TODO
		None

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

			_transport.unicast(p, consume append)
		end
		// note - we process the results asynchronously

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
			// hmmm, our log is probably empty (maybe we should correct our commit index?)
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
		"""
		Convert this server to being a candidate.

		Triggered by:
		  - ElectionTimeout
		"""
		// check that we are not already a candidate
		if (_mode is Candidate) then return end

		leader = None // clear any potential leader state (should have been cleared as a follower)
		_set_mode(Candidate)

		// start a new election
		_start_election()

	fun ref _start_election() =>
		"""
		Start a new election in this candidate.

		Triggered by:
		  - CanvasTimeout
			- _start_candidate after an ElectionTimeout
		"""
		// only candidates can run elections
		if (_mode isnt Candidate) then return end

		// new term, reinitialise the vote register and vote for self
		persistent.current_term = persistent.current_term + 1
		persistent.voted_for = _id
		candidate = VolatileCandidateState
		candidate.vote()
		// send vote requests to other replicas (in parallel)
		for p in _peers.values() do
			let canvas: VoteRequest iso = recover iso VoteRequest end
			canvas.term = persistent.current_term
			canvas.candidate_id = _id

			// TODO review are the log_term and log_index being set correctly?
			canvas.last_log_term = try persistent.log(volatile.commit_index.usize())?.term else RaftTerm(0) end
			canvas.last_log_index = volatile.commit_index

			_transport.unicast(p, consume canvas)
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

	fun ref _clear_timer() =>
		_timers.cancel(_mode_timer) // cancel any previous timers before recording a new one

	fun ref _set_timer(mt: Timer iso) =>
		_clear_timer()
		_mode_timer = mt
		_timers(consume mt)

	fun ref _set_mode(mode: RaftMode) =>
		_mode = mode
		_monitor.mode_changed(_id, _mode, persistent.current_term)

	fun ref _swash(lower: U64, upper: U64): U64 =>
		// randomise the timeout between [150,300) ms
		lower + _rand.int(upper - lower)

	// -- client command ingress

	fun ref _accept_command_as_follower(command: CommandEnvelope[T]) =>
		// follower  - redirect command to the leader
		//             (leader will reply, leader may provide backpressure)
		// TODO
		None

	fun ref _accept_command_as_candidate(command: CommandEnvelope[T]) =>
		// candidate - queue the command until transitioning to being a leader or follower
		//             (honour ttl and generate dropped message signals)
		//             (send backpressure if the queue gets too large)
		//             (batch queued message to the leader)
		// TODO
		None

	fun ref _accept_command_as_leader(command: CommandEnvelope[T]) =>
		// leader    - apply commands to the journal log and distribute them to followers
		//             (may generate backpressure if the nextIndex vs matchedIndex vs log
		//              starts to get too large)
		// TODO
		let c: CommandEnvelope[T] = consume command
		let cmd: T val = c.command
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
