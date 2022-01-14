/**
 * Pony Raft Library
 * Copyright (c) 2019 - Stewart Gebbie. Licensed under the MIT licence.
 * vim: set ts=2 sw=0:
 */
use "ponytest"
use "time"

actor RaftServerTests is TestList

	new create(env: Env) =>
		PonyTest(env, this)

	new make() =>
		None

	fun tag tests(test: PonyTest) =>
		test(_TestMajority)
		test(_TestRequestVote)
		test(_TestConvertToCandidate)
		test(_TestWaitForCanvas)
		test(_TestFailLowerTermAppend)
		test(_TestConvertToLeader)
		test(_TestConvertToFollower)
		test(_TestWaitForHeartbeats)
		test(_TestAppendRejectNoPrev)
		test(_TestAppendDropConflictingLogEntries)

primitive Digits
	fun val number(n: U16): String =>
		match n
		| 0 => "zero"
		| 1 => "one"
		| 2 => "two"
		| 3 => "three"
		| 4 => "four"
		| 5 => "five"
		| 6 => "six"
		| 7 => "seven"
		| 8 => "eight"
		| 9 => "nine"
		else
			"."
		end

class iso _TestMajority is UnitTest
	""" Tests majority calculation. """
	new iso create() => None
	fun name(): String => "raft:server:majority"
	fun ref apply(h: TestHelper) =>
		h.assert_eq[USize](3, _majority(5))
		h.assert_eq[USize](2, _majority(3))
		h.assert_eq[USize](9, _majority(17))
		h.assert_eq[USize](4, _majority(6))
	fun box _majority(full: USize): USize =>
		full.shr(1) + 1

class iso _TestAppendDropConflictingLogEntries is UnitTest
	""" Tests that followers drop conflicinting log entries. """

	// create a replica and drive it into the follower state
	// use a mock leader to append entries (that are not committed)
	// now use a mock leader to send conflicting entries (overlapping index but different terms)
	// check that the follower:
	//   - drops the conflicting entries (need a feedback channel for this), and
	//   - accepts the new log entries (responds true to append).

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:server:append-drop-conflict"

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)
		h.expect_action("got-follower-start")
		h.expect_action("got-append-success-one-3:7")	// 1122233 (term = 3, last = 7)
		h.expect_action("got-append-success-two-4:6")	// 112244 (term = 4, last = 6)

		let receiver_candidate_id: RaftId = 1 // actual replica server being tested
		let peer_one_id: RaftId = 2 // observer validating the replies

		// create a network
		let netmon = NopEgressMonitor[RaftId] // EnvEgressMonitor(h.env)
		let egress:RaftEgress[DummyCommand,DummyResponse] = IntraProcessRaftServerEgress[DummyCommand,DummyResponse](consume netmon)

		// set up a monitor to wait for state changes and to trigger the mock leader
		let mock_leader: _AppendAndOverwriteMockLeader = _AppendAndOverwriteMockLeader(h
													, egress, peer_one_id, receiver_candidate_id)
		let mon: RaftServerMonitor[DummyCommand] iso = FollowerAppendMonitor[DummyCommand](h, mock_leader)

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand,DummyResponse](receiver_candidate_id
			, _timers, egress
			, [as RaftId: receiver_candidate_id; peer_one_id ]
			, DummyMachine , DummyCommand.start()
			where monitor = consume mon)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock_leader)

		// register network endpoints
		egress.register_peer(receiver_candidate_id, replica)
		egress.register_peer(peer_one_id, mock_leader)

interface tag _AppendMockLeader is RaftEndpoint[DummyCommand]

	be apply(signal: RaftSignal[DummyCommand]) => None
	be lead_one() => None
	be lead_two() => None
	be lead_three() => None

actor _AppendAndOverwriteMockLeader is _AppendMockLeader

	let _h: TestHelper
	let _egress: RaftEgress[DummyCommand,DummyResponse]
	let _leader_id: RaftId
	let _follower_id: RaftId
	let _debug: Bool = false

	new create(h: TestHelper
		, egress: RaftEgress[DummyCommand,DummyResponse]
		, leader_id: RaftId, follower_id: RaftId) =>
		_h = h
		_egress = egress
		_leader_id = leader_id
		_follower_id = follower_id

	be apply(signal: RaftSignal[DummyCommand]) => None
		match consume signal
		| let s: VoteRequest =>
			if _debug then
				_h.env.out.print("got vote request in mock leader: " + _leader_id.string()
						+ " for term: " + s.term.string()
						+ " from candidate: " + s.candidate_id.string()
					)
			end
			_h.fail("mock leader should not get a vote request")
		| let s: AppendEntriesRequest[DummyCommand] =>
			_h.fail("mock leader should not get a vote append requests")
		| let s: AppendEntriesResult =>
			if _debug then
				_h.env.out.print("got append result in mock leader: " + _leader_id.string())
			end
		end

	be lead_one() =>
		// start by assuming that the follower has nothing, hence (prev_log_index, prev_log_term) = (0,0)
		let append: AppendEntriesRequest[DummyCommand] iso = recover iso AppendEntriesRequest[DummyCommand] end
		append.target_follower_id = _follower_id
		append.term = 3
		append.prev_log_index = 0
		append.prev_log_term = 0
		append.leader_commit = 0
		append.leader_id = _leader_id

		// add commands with log terms 1122233
		append.entries.clear()
		let cmd: DummyCommand = DummyCommand
		append.entries.push(Log[DummyCommand](1, cmd))
		append.entries.push(Log[DummyCommand](1, cmd))
		append.entries.push(Log[DummyCommand](2, cmd))
		append.entries.push(Log[DummyCommand](2, cmd))
		append.entries.push(Log[DummyCommand](2, cmd))
		append.entries.push(Log[DummyCommand](3, cmd))
		append.entries.push(Log[DummyCommand](3, cmd))

		// send the log
		_egress.emit(consume append)

	be lead_two() =>
		// continue with an overlapping e.g. (prev_log_index, prev_log_term) = (4,2) to change to log terms 11244
		let append: AppendEntriesRequest[DummyCommand] iso = recover iso AppendEntriesRequest[DummyCommand] end
		append.target_follower_id = _follower_id
		append.term = 4
		append.prev_log_index = 3
		append.prev_log_term = 2
		append.leader_commit = 0
		append.leader_id = _leader_id

		// add a logs to overwrite with the resultant terms 11244 (i.e. less log entries)
		append.entries.clear()
		let cmd: DummyCommand = DummyCommand
		append.entries.push(Log[DummyCommand](2, cmd))
		append.entries.push(Log[DummyCommand](4, cmd))
		append.entries.push(Log[DummyCommand](4, cmd))

		// send the log
		_egress.emit(consume append)

class iso _TestAppendRejectNoPrev is UnitTest
	""" Tests that an append is rejected if there is no match for the 'prev' log entry. """

	// create a replica and drive it into follower state
	// now mock up appends from a leader so as to add logs to the replica
	// then try to append entries that do not match on 'prev' log index and term
	// check that this last append triggers a rejection

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:server:append-no-prev"

	fun box _debug(): Bool => false

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)
		h.expect_action("got-follower-start")
		h.expect_action("got-append-success-one-1:1")
		h.expect_action("got-append-success-two-1:2")
		h.expect_action("got-append-failure-three-4:2")

		let follower_id: RaftId = 1 // actual replica server being tested
		let mock_leader_id: RaftId = 2 // observer validating the replies

		if _debug() then
			h.env.out.print("net address - follower: " + follower_id.string())
			h.env.out.print("net address - mock leader: " + mock_leader_id.string())
		end

		// create a network
		let netmon = NopEgressMonitor[RaftId] // EnvEgressMonitor(h.env)
		let egress: RaftEgress[DummyCommand,DummyResponse] = IntraProcessRaftServerEgress[DummyCommand,DummyResponse](consume netmon)

		// set up a monitor to wait for state changes and to trigger the mock leader
		let mock_leader: _AppendRejectNoPrevMockLeader = _AppendRejectNoPrevMockLeader(h
													, egress, mock_leader_id, follower_id)
		let mon: RaftServerMonitor[DummyCommand] iso = FollowerAppendMonitor[DummyCommand](h, mock_leader)

		// create a follower (NB raft servers start as followers by default)
		let replica = RaftServer[DummyCommand,DummyResponse](follower_id
			, _timers, egress
			, [as RaftId: follower_id; mock_leader_id ]
			, DummyMachine, DummyCommand.start()
			where monitor = consume mon)
		// register components that need to be shut down
		h.dispose_when_done(replica)
		h.dispose_when_done(mock_leader)

		// register network endpoints
		egress.register_peer(follower_id, replica)
		egress.register_peer(mock_leader_id, mock_leader)

actor _AppendRejectNoPrevMockLeader is _AppendMockLeader

	let _h: TestHelper
	let _debug: Bool = false
	let _egress: RaftEgress[DummyCommand,DummyResponse]
	let _leader_id: RaftId
	let _follower_id: RaftId

	new create(h: TestHelper
		, egress: RaftEgress[DummyCommand,DummyResponse]
		, leader_id: RaftId, follower_id: RaftId) =>
		_h = h
		_egress = egress
		_leader_id = leader_id
		_follower_id = follower_id

	be apply(signal: RaftSignal[DummyCommand]) => None
		match consume signal
		| let s: VoteRequest =>
			if _debug then
				_h.env.out.print("got vote request in mock leader: " + _leader_id.string()
						+ " for term: " + s.term.string()
						+ " from candidate: " + s.candidate_id.string()
					)
			end
			_h.fail("mock leader should not get a vote request")
		| let s: AppendEntriesRequest[DummyCommand] =>
			_h.fail("mock leader should not get a vote append requests")
		| let s: AppendEntriesResult =>
			if _debug then
				_h.env.out.print("got append result in mock leader: " + _leader_id.string())
			end
		end

	be lead_one() =>
		// start by assuming that the follower has nothing, hence (prev_log_index, prev_log_term) = (0,0)
		let append: AppendEntriesRequest[DummyCommand] iso = recover iso AppendEntriesRequest[DummyCommand] end
		append.target_follower_id = _follower_id
		append.term = 1
		append.prev_log_index = 0
		append.prev_log_term = 0
		append.leader_commit = 0
		append.leader_id = _leader_id

		// add a single command to the partial log
		append.entries.clear()
		let cmd: DummyCommand = DummyCommand
		let l: Log[DummyCommand] val = Log[DummyCommand](1, cmd)
		append.entries.push(consume l)

		// send the log
		_egress.emit(consume append)

	be lead_two() =>
		// continue with a valid update e.g. (prev_log_index, prev_log_term) = (1,1)
		let append: AppendEntriesRequest[DummyCommand] iso = recover iso AppendEntriesRequest[DummyCommand] end
		append.target_follower_id = _follower_id
		append.term = 1
		append.prev_log_index = 1
		append.prev_log_term = 1
		append.leader_commit = 0
		append.leader_id = _leader_id

		// add a single command to the partial log
		append.entries.clear()
		let cmd: DummyCommand = DummyCommand
		let l: Log[DummyCommand] val = Log[DummyCommand](1, cmd)
		append.entries.push(consume l)

		// send the log
		_egress.emit(consume append)

	be lead_three() =>
		// now publish an out of sequence log entry e.g. (prev_log_index, prev_log_term) = (2,3)
		// (here the index should match but the term will be out of sync and larger)
		let append: AppendEntriesRequest[DummyCommand] iso = recover iso AppendEntriesRequest[DummyCommand] end
		append.target_follower_id = _follower_id
		append.term = 4
		append.prev_log_index = 2
		append.prev_log_term = 3
		append.leader_commit = 0
		append.leader_id = _leader_id

		// add a single command to the partial log
		append.entries.clear()
		let cmd: DummyCommand = DummyCommand
		let l: Log[DummyCommand] val = Log[DummyCommand](1, cmd)
		append.entries.push(consume l)

		// send the log
		_egress.emit(consume append)

class iso FollowerAppendMonitor[T: Any val] is RaftServerMonitor[T]

	"""
	Waits for the replica to become a follower.
	Then starts the mock leader, which will append entries.
	Waits to check that the appends succeed.
	"""

	let _h: TestHelper
	let _debug: Bool = false
	let _mock_leader: _AppendMockLeader
	var _seen_follower: Bool
	var _count_append: U16

	new iso create(h: TestHelper, mock_leader: _AppendMockLeader) =>
		_h = h
		_mock_leader = mock_leader
		_seen_follower = false
		_count_append = 0

	fun ref failure(id: RaftId, term: RaftTerm, mode: RaftMode, msg: String) =>
		_h.fail("raft-"  + id.string() + ":term=" + term.string() + ";mode=" + mode.string() + ";failure" + ";msg=" + msg)

	fun ref mode_changed(id: RaftId, term: RaftTerm, mode: RaftMode) =>
		match mode
		| Follower =>
			if _debug then _h.env.out.print("got state Follower for term " + term.string()) end
			_h.complete_action("got-follower-start")
			if (not _seen_follower) then
				if _debug then _h.env.out.print("triggering mock lead one") end
				_mock_leader.lead_one()
			else
				if _debug then _h.env.out.print("already seen follower - _not_ triggering mock lead one") end
			end
			_seen_follower = true
		| Candidate =>
			_h.fail("follower should not become a candidate")
		| Leader =>
			_h.fail("follower should not become a leader")
		end

	fun ref append_processed(id: RaftId
		, term: RaftTerm
		, mode: RaftMode

		, last_applied_index: RaftIndex
		, commit_index: RaftIndex
		, last_log_index: RaftIndex

		, leader_term: RaftTerm
		, leader_id: RaftId
		, leader_commit_index: RaftIndex
		, leader_prev_log_index: RaftIndex
		, leader_prev_log_term: RaftTerm
		, leader_entry_count: USize

		, applied: Bool // true if these
		) =>

		if _debug then
			_h.env.out.print("got append accept: " + applied.string()
												+ " id: " + id.string()
												+ " current_term: " + term.string()
												+ " current_mode: " + mode.string()
												+ " last_applied_index: " + last_applied_index.string()
												+ " last_log_index: " + last_log_index.string()

												+ " leader_term:" + leader_term.string()
												+ " leader_id: " + leader_id.string()
												+ " leader_commit_index:" + leader_commit_index.string()
												+ " leader_prev_log_index:" + leader_prev_log_index.string()
												+ " leader_prev_log_term:" + leader_prev_log_term.string()
												+ " leader_entry_count:" + leader_entry_count.string()
												)
		end

		_count_append = _count_append + 1
		let pass: String = if applied then "success" else "failure" end
		let num: String val = Digits.number(_count_append)
		let token: String = "got-append-" + pass + "-" + num + "-"
													+ term.string() + ":" + last_log_index.string()

		match (_count_append, applied)
		| (1, true) =>
			if _debug then _h.env.out.print("triggering mock lead two: " + token) end
			_mock_leader.lead_two()
		| (2, true) =>
			if _debug then _h.env.out.print("triggering mock lead three: " + token) end
			_mock_leader.lead_three()
		else
			if _debug then _h.env.out.print("no action after: " + token) end
		end

		_h.complete_action(token)


class iso _TestWaitForHeartbeats is UnitTest
	""" Tests that a leader gets signal to publish heartbeats. """

	// wait for a replica to become a candidate
	// publish faux votes in favour of the candiate from a majority
	// wait for the replica to become a leader
	// wait for the leader to send out the first round of heartbeats
	// wait for a second round of heartbeats

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:server:heartbeats"

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)

		let receiver_candidate_id: RaftId = 1 // actual replica server being tested
		let peer_one_id: RaftId = 2 // observer validating the replies
		let peer_two_id: RaftId = 3 // another peer

		// create a network
		let netmon = NopEgressMonitor[RaftId] // EnvEgressMonitor(h.env)
		let egress: RaftEgress[DummyCommand,DummyResponse] = IntraProcessRaftServerEgress[DummyCommand,DummyResponse](consume netmon)
		h.expect_action("got-follower-start")
		h.expect_action("got-candidate-convert") // expecting RequestVote in the mock following this
		h.expect_action("got-vote-request")
		h.expect_action("got-leader-convert")
		h.expect_action("got-heartbeat-one")
		h.expect_action("got-heartbeat-two")

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor[DummyCommand] iso = LeaderRaftServerMonitor[DummyCommand](h)

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand,DummyResponse](receiver_candidate_id
			, _timers, egress
			, [as RaftId: receiver_candidate_id; peer_one_id; peer_two_id]
			, DummyMachine, DummyCommand.start()
			where monitor = consume mon)
		let peer_one = GrantVoteMockRaftServer(h, egress, peer_one_id)
		let peer_two = GrantVoteMockRaftServer(h, egress, peer_two_id)
		h.dispose_when_done(replica)
		h.dispose_when_done(peer_one)
		h.dispose_when_done(peer_two)

		// register network endpoints
		egress.register_peer(receiver_candidate_id, replica)
		egress.register_peer(peer_one_id, peer_one)
		egress.register_peer(peer_two_id, peer_two)

class iso _TestConvertToLeader is UnitTest
	""" Tests that a candidate will convert to a leader if it gets enough votes. """

	// wait for a replica to become a candidate
	// publish faux votes from a majority of peers
	// wait for the replica to become a leader

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:server:convert-to-leader"

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)

		let receiver_candidate_id: RaftId = 1 // actual replica server being tested
		let peer_one_id: RaftId = 2 // observer validating the replies
		let peer_two_id: RaftId = 3 // another peer

		// create a network
		let netmon = NopEgressMonitor[RaftId] // EnvEgressMonitor(h.env)
		let egress: RaftEgress[DummyCommand,DummyResponse] = IntraProcessRaftServerEgress[DummyCommand,DummyResponse](consume netmon)
		h.expect_action("got-follower-start")
		h.expect_action("got-candidate-convert") // expecting RequestVote in the mock following this
		h.expect_action("got-vote-request")
		h.expect_action("got-leader-convert")

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor[DummyCommand] iso = LeaderRaftServerMonitor[DummyCommand](h)

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand,DummyResponse](receiver_candidate_id
			, _timers, egress
			, [as RaftId: receiver_candidate_id; peer_one_id; peer_two_id]
			, DummyMachine, DummyCommand.start()
			where monitor = consume mon)
		let peer_one = GrantVoteMockRaftServer(h, egress, peer_one_id)
		let peer_two = GrantVoteMockRaftServer(h, egress, peer_two_id)
		h.dispose_when_done(replica)
		h.dispose_when_done(peer_one)
		h.dispose_when_done(peer_two)

		// register network endpoints
		egress.register_peer(receiver_candidate_id, replica)
		egress.register_peer(peer_one_id, peer_one)
		egress.register_peer(peer_two_id, peer_two)

class iso LeaderRaftServerMonitor[T: Any val] is RaftServerMonitor[T]

	let _debug: Bool = false
	let _h: TestHelper
	var _seen_follower: Bool
	var _is_candidate: Bool

	new iso create(h: TestHelper) =>
		_h = h
		_seen_follower = false
		_is_candidate = false

	fun ref failure(id: RaftId, term: RaftTerm, mode: RaftMode, msg: String) =>
		_h.fail("raft-"  + id.string() + ":term=" + term.string() + ";mode=" + mode.string() + ";failure" + ";msg=" + msg)

	fun ref timeout_raised(id: RaftId, term: RaftTerm, mode: RaftMode, timeout: RaftTimeout) => None

	fun ref mode_changed(id: RaftId, term: RaftTerm, mode: RaftMode) =>
		match mode
		| Follower =>
			if _debug then _h.env.out.print("got state Follower for term " + term.string()) end
			if _seen_follower and _is_candidate then
				_h.fail("should not convert back to a follower")
			else
				_h.complete_action("got-follower-start")
			end
			_seen_follower = true
		| Candidate =>
			if _debug then _h.env.out.print("got state Candidate for term " + term.string()) end
			_is_candidate = true
			_h.complete_action("got-candidate-convert")
		| Leader =>
			if _debug then _h.env.out.print("got state Leader for term " + term.string()) end
			_h.complete_action("got-leader-convert")
		end

actor GrantVoteMockRaftServer is RaftEndpoint[DummyCommand]

	let _h: TestHelper
	let _debug: Bool = false
	let _egress: RaftEgress[DummyCommand,DummyResponse]
	let _id: RaftId
	var _seen_append: U16

	new create(h: TestHelper , egress: RaftEgress[DummyCommand,DummyResponse] , faux_peer_id: RaftId) =>
		_h = h
		_egress = egress
		_id = faux_peer_id
		_seen_append = 0

	be apply(signal: RaftSignal[DummyCommand]) =>
		match consume signal
		| let s: VoteRequest =>
			if _debug then
				_h.env.out.print("got vote request in peer: " + _id.string()
						+ " for term: " + s.term.string()
						+ " from candidate: " + s.candidate_id.string()
					)
			end
			_h.complete_action("got-vote-request")

			// reply and grant vote
			let vote: VoteResponse iso = recover iso VoteResponse end
			vote.target_candidate_id = s.candidate_id
			vote.term = 0 // choose a low term so that the vote is accepted by the candidate
			vote.vote_granted = true
			_egress.emit(consume vote)
		| let s: AppendEntriesRequest[DummyCommand] =>
			_seen_append = _seen_append + 1
			match _seen_append
			| 1 => _h.complete_action("got-heartbeat-one")
			| 2 => _h.complete_action("got-heartbeat-two")
			| 3 => _h.complete_action("got-heartbeat-three")
			end
		end

class iso _TestConvertToFollower is UnitTest
	""" Tests that a candidate will convert to a follower if a peer is elected. """

	/*
	 * see: §5.2 "While waiting for votes, a candidate may receive an
	 * AppendEntries RPC from another server claiming to be
	 * leader. If the leader’s term (included in its RPC) is at least
	 * as large as the candidate’s current term, then the candidate
	 * recognizes the leader as legitimate and returns to follower
	 * state. If the term in the RPC is smaller than the candidate’s
	 * current term, then the candidate rejects the RPC and con-
	 * tinues in candidate state."
	 */

	// wait for a replica to become a candidate
	// publish a "heartbeat" as though another replica had become a leader (i.e. same or higher term)
	// wait for the replica to convert to being a follower

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:server:convert-to-follower"

	fun box _debug(): Bool => false

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)

		let receiver_candidate_id: RaftId = 1 // actual replica server being tested
		let listener_candidate_id: RaftId = 2 // observer validating the replies

		// create a network
		let netmon = NopEgressMonitor[RaftId] // EnvEgressMonitor(h.env)
		let egress: RaftEgress[DummyCommand,DummyResponse] = IntraProcessRaftServerEgress[DummyCommand,DummyResponse](consume netmon)
		h.expect_action("got-follower-start")
		h.expect_action("got-candidate") // expecting RequestVote in the mock
		h.expect_action("got-vote-request")
		h.expect_action("got-follower-convert")

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor[DummyCommand] iso = object iso is RaftServerMonitor[DummyCommand]
				let _h: TestHelper = h
				let _d: Bool = _debug()
				var _seen_follower: Bool = false
				var _is_candidate: Bool = false
				fun ref timeout_raised(id: RaftId, term: RaftTerm, mode: RaftMode, timeout: RaftTimeout) => None
				fun ref mode_changed(id: RaftId, term: RaftTerm, mode: RaftMode) =>
					match mode
					| Follower =>
						if _d then _h.env.out.print("got state Follower for term " + term.string()) end
						if _seen_follower and _is_candidate then
							h.complete_action("got-follower-convert")
						else
							h.complete_action("got-follower-start")
						end
						_seen_follower = true
					| Candidate =>
						if _d then _h.env.out.print("got state Candidate for term " + term.string()) end
						_is_candidate = true
						_h.complete_action("got-candidate")
					end
			end

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand,DummyResponse](receiver_candidate_id
			, _timers, egress
			, [as RaftId: receiver_candidate_id; listener_candidate_id]
			, DummyMachine, DummyCommand.start()
			where monitor = consume mon)
		let mock = HeartbeatOnVoteMockRaftServer(h, egress, listener_candidate_id)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock)

		// register network endpoints
		egress.register_peer(receiver_candidate_id, replica)
		egress.register_peer(listener_candidate_id, mock)

actor HeartbeatOnVoteMockRaftServer is RaftEndpoint[DummyCommand]

	let _h: TestHelper
	let _egress: RaftEgress[DummyCommand,DummyResponse]
	let _id: RaftId

	new create(h: TestHelper , egress: RaftEgress[DummyCommand,DummyResponse] , faux_leader_id: RaftId) =>
		_h = h
		_egress = egress
		_id = faux_leader_id

	be apply(signal: RaftSignal[DummyCommand]) =>
		match consume signal
		| let s: VoteRequest =>
			_h.complete_action("got-vote-request")
			// send an append with a lower term
			let append: AppendEntriesRequest[DummyCommand] iso = recover iso AppendEntriesRequest[DummyCommand] end
			append.target_follower_id = s.candidate_id
			append.term = s.term // set the same term seen in the vote request
			append.prev_log_index = 0
			append.prev_log_term = 0
			append.leader_commit = 0
			append.leader_id = _id

			_egress.emit(consume append)
		end

class iso _TestFailLowerTermAppend is UnitTest
	""" Tests that a replica will fail to append logs for lower terms. """

	// spin up a follower
	// send it and append entries with a lower term
	// wait for the reply and check fail

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:server:fail-lower-term-append"

	fun box _debug():Bool => false

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)

		let receiver_follower_id: RaftId = 1 // actual replica server being tested
		let listener_leader_id: RaftId = 2 // observer validating the replies

		// create a network
		let egress:RaftEgress[DummyCommand,DummyResponse] = IntraProcessRaftServerEgress[DummyCommand,DummyResponse]()
		// NB don't call h.complete(...) if using "expect actions"
		h.expect_action("got-append-false")
		h.expect_action("got-higher-term")
		let sent_term: RaftTerm = 1
		let follower_term: RaftTerm = 5

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand,DummyResponse](receiver_follower_id
			, _timers, egress
			, [as RaftId: receiver_follower_id; listener_leader_id]
			, DummyMachine , DummyCommand.start()
			where initial_term = follower_term)
		let mock = ExpectFailAppend(h, sent_term)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock)

		// register network endpoints
		egress.register_peer(receiver_follower_id, replica)
		egress.register_peer(listener_leader_id, mock)

		// send an append with a lower term
		let append: AppendEntriesRequest[DummyCommand] iso = recover iso AppendEntriesRequest[DummyCommand] end
		append.target_follower_id = receiver_follower_id
		append.term = sent_term // set a lower term (follower was forced to start with a higher term)
		append.prev_log_index = 0
		append.prev_log_term = 0
		append.leader_commit = 0
		append.leader_id = listener_leader_id

		if _debug() then h.env.out.print("sending append...") end
		egress.emit(consume append)


actor ExpectFailAppend is RaftEndpoint[DummyCommand]

	let _h: TestHelper
	let _sent_term: RaftTerm

	new create(h: TestHelper, sent_term: RaftTerm) =>
		_h = h
		_sent_term = sent_term

	be apply(signal: RaftSignal[DummyCommand]) =>
		match consume signal
		| (let s: AppendEntriesResult) =>
			if s.success == false then _h.complete_action("got-append-false") end
			if s.term > _sent_term then _h.complete_action("got-higher-term") end
		else
			_h.fail("mock got an unexpected signal")
		end

class iso _TestWaitForCanvas is UnitTest
	""" Tests that a candidate will restart a new election if an election doesn't conclude in time. """

	// simply start a replica which will default to 'follower' mode
	// wait for the follower to become a candidate based on the election timeout
	// wait for the candidate to receive a second "canvas" timeout

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:server:canvas"

	fun box _debug(): Bool => false

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)

		let receiver_candidate_id: RaftId = 1 // actual replica server being tested
		let listener_candidate_id: RaftId = 2 // observer validating the replies

		// create a network
		let egress:RaftEgress[DummyCommand,DummyResponse] = IntraProcessRaftServerEgress[DummyCommand,DummyResponse]()
		// NB don't call h.complete(...) if using "expect actions"
		h.expect_action("got-election-timeout")
		h.expect_action("got-canvas") // expecting RequestVote in the mock
		h.expect_action("got-canvas-timeout")
		h.expect_action("got-candidate-state")
		h.expect_action("got-canvas-again") // expecting RequestVote in the mock with a higher term

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor[DummyCommand] iso = object iso is RaftServerMonitor[DummyCommand]
				let _d: Bool = _debug()
				let _h: TestHelper = h
				fun box timeout_raised(id: RaftId, term: RaftTerm, mode: RaftMode, timeout: RaftTimeout) =>
					match timeout
					| (let t: ElectionTimeout) => _h.complete_action("got-election-timeout")
					| (let t: CanvasTimeout) => _h.complete_action("got-canvas-timeout")
					end
				fun box mode_changed(id: RaftId, term: RaftTerm, mode: RaftMode) =>
					match mode
					| (let m: Candidate) =>
						if _d then _h.env.out.print("got state Candidate for term " + term.string()) end
						_h.complete_action("got-candidate-state")
					end
			end

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand,DummyResponse](receiver_candidate_id
			, _timers, egress
			, [as RaftId: receiver_candidate_id; listener_candidate_id]
			, DummyMachine, DummyCommand.start()
			where monitor = consume mon)
		let mock = ExpectCanvasMockRaftServer(h)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock)

		// register network endpoints
		egress.register_peer(receiver_candidate_id, replica)
		egress.register_peer(listener_candidate_id, mock)

class iso _TestConvertToCandidate is UnitTest
	"""
	Tests that a follower will convert to being a candidate and start an election if it does not receive a heartbeat.
	"""

	// simply start a replica which will default to 'follower' mode
	// wait for the timeout via the monitor
	// wait for the state change via the monitor
	// conclude

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:server:convert-to-candidate"

	fun box _debug(): Bool => false

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)

		let receiver_candidate_id: RaftId = 1 // actual replica server being tested
		let listener_candidate_id: RaftId = 2 // observer validating the replies

		// create a network
		let egress:RaftEgress[DummyCommand,DummyResponse] = IntraProcessRaftServerEgress[DummyCommand,DummyResponse]()
		h.expect_action("got-timeout")
		h.expect_action("got-state")
		h.expect_action("got-canvas") // expecting RequestVote in the mock

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor[DummyCommand] iso = object iso is RaftServerMonitor[DummyCommand]
				let _d: Bool = _debug()
				let _h: TestHelper = h
				fun box timeout_raised(id: RaftId, term: RaftTerm, mode: RaftMode, timeout: RaftTimeout) =>
					if (timeout is ElectionTimeout) then
						_h.complete_action("got-timeout")
					end
				fun box mode_changed(id: RaftId, term: RaftTerm, mode: RaftMode) =>
					if (mode is Candidate) then
						if _d then _h.env.out.print("got state Candidate for term " + term.string()) end
						_h.complete_action("got-state")
					end
			end

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand,DummyResponse](receiver_candidate_id
			, _timers, egress
			, [as RaftId: receiver_candidate_id; listener_candidate_id]
			, DummyMachine, DummyCommand.start()
			where monitor = consume mon)
		let mock = ExpectCanvasMockRaftServer(h)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock)

		// register network endpoints
		egress.register_peer(receiver_candidate_id, replica)
		egress.register_peer(listener_candidate_id, mock)

actor ExpectCanvasMockRaftServer is RaftEndpoint[DummyCommand]

	let _h: TestHelper
	var _term_seen: (RaftTerm | None)

	new create(h: TestHelper) =>
		_h = h
		_term_seen = None

	be apply(signal: RaftSignal[DummyCommand]) =>
		match consume signal
		| let s: VoteRequest =>
			match _term_seen
			| None =>
				_term_seen = s.term // the candidate term
				_h.complete_action("got-canvas")
			| (let t: RaftTerm) =>
				if s.term > t then
					_h.complete_action("got-canvas-again")
				end
			end
		else
			_h.fail("mock got an unexpected signal")
		end

class iso _TestRequestVote is UnitTest
	""" Tests response to requesting a vote. """

	// create just one replica
	// create a network
	// register the replica on the network using its candiate ID
	// register a mock trigger replica on the network using a different candiate ID
	// send a vote request to the replica
	// wait for a reply in the mock replica
	// check the expected 'term'
	// check the expected 'vote'

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:server:vote"

	fun ref tear_down(h: TestHelper) => None

	fun ref _debug(): Bool => false

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)

		let receiver_candidate_id: RaftId = 1 // actual replica server being tested
		let listener_candidate_id: RaftId = 2 // observer validating the replies

		// create a network
		let netmon = NopEgressMonitor[RaftId] // EnvEgressMonitor(h.env)
		let egress: RaftEgress[DummyCommand,DummyResponse] = IntraProcessRaftServerEgress[DummyCommand,DummyResponse](consume netmon)

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor[DummyCommand] iso = if _debug() then
				object iso is RaftServerMonitor[DummyCommand]
					let _env: Env = h.env
					fun box vote_req(id: RaftId, signal: VoteRequest val) => _env.out.print("vote req: " + id.string() + " term:" + signal.term.string())
					fun box vote_res(id: RaftId, signal: VoteResponse val) => _env.out.print("vote res: " + id.string() + " term:" + signal.term.string())
					fun box append_req(id: RaftId, signal: AppendEntriesRequest[DummyCommand] val) => _env.out.print("append req: " + id.string())
					fun box append_res(id: RaftId, signal: AppendEntriesResult val) => _env.out.print("append res: " + id.string())
					fun box install_req(id: RaftId, signal: InstallSnapshotRequest val) => _env.out.print("install req: " + id.string())
					fun box install_res(id: RaftId, signal: InstallSnapshotResponse val) => _env.out.print("install res: " + id.string())

					fun box command_req(id: RaftId, term: RaftTerm, mode: RaftMode) => _env.out.print("command req: " + id.string())
					fun box command_res(id: RaftId, term: RaftTerm, mode: RaftMode) => _env.out.print("command res: " + id.string())

					fun box timeout_raised(id: RaftId, term: RaftTerm, mode: RaftMode, timeout: RaftTimeout) => _env.out.print("timeout raised")
					fun box mode_changed(id: RaftId, term: RaftTerm, mode: RaftMode) => _env.out.print("mode changed: " + mode.text() + " term:" + term.string())
					fun ref failure(id: RaftId, term: RaftTerm, mode: RaftMode, msg: String) => _env.out.print("failure: raft-"  + id.string() + ":term=" + term.string() + ";mode=" + mode.string() + ";failure" + ";msg=" + msg)
					fun ref warning(id: RaftId, term: RaftTerm, mode: RaftMode, msg: String) => _env.out.print("warning: raft-"  + id.string() + ":term=" + term.string() + ";mode=" + mode.string() + ";warning" + ";msg=" + msg)
				end
			else
				NopRaftServerMonitor[DummyCommand]
			end

		// register components that need to be shut down
		let replica: RaftServer[DummyCommand,DummyResponse] = RaftServer[DummyCommand,DummyResponse](1
			, _timers, egress, [as RaftId: receiver_candidate_id; listener_candidate_id;3]
			, DummyMachine, DummyCommand.start()
				where monitor = consume mon)
		let mock = ExpectVoteMockRaftServer(h)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock)

		// register network endpoints
		egress.register_peer(receiver_candidate_id, replica)
		egress.register_peer(listener_candidate_id, mock)

		// send a vote
		let canvas: VoteRequest iso = recover iso VoteRequest end
		canvas.target_peer_id = receiver_candidate_id
		canvas.term = 1
		canvas.last_log_index = 0
		canvas.last_log_term = 0
		canvas.candidate_id = listener_candidate_id

		if _debug() then h.env.out.print("sending vote...") end
		egress.emit(consume canvas)

primitive DummyCommand
	fun val start(): DummyCommand =>
		DummyCommand

primitive DummyResponse

class iso DummyMachine is StateMachine[DummyCommand, DummyResponse]
	fun ref accept(command: DummyCommand): DummyResponse => DummyResponse

class val EnvEgressMonitor is EgressMonitor[RaftId]

	let _env: Env

	new val create(env: Env) =>
		_env = env

	fun val dropped(id: RaftId) => _env.out.print("net dropped send to: " + id.string())
	fun val sent(id: RaftId) => _env.out.print("net sent to: " + id.string())

actor ExpectVoteMockRaftServer is RaftEndpoint[DummyCommand]

	let _h: TestHelper
	let _debug: Bool = false

	new create(h: TestHelper) =>
		_h = h

	be apply(signal: RaftSignal[DummyCommand]) =>
		if _debug then _h.env.out.print("mock got a signal") end
		// VoteResponse.{term: U64, vote_granted: Bool}
		match consume signal
		| let s: VoteResponse =>
			if _debug then _h.env.out.print("mock got vote response, term: " + s.term.string()) end
			_h.assert_true(s.vote_granted, "mock expected to get a vote")
		else
			_h.fail("mock got an unexpected signal")
		end
		_h.complete(true)
