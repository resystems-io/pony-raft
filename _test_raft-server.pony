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
		test(_TestArrayWithout)
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

class iso _TestArrayWithout is UnitTest
	""" Tests removal of an element from an array. """
	new iso create() => None
	fun name(): String => "raft:server:without"
	fun ref apply(h: TestHelper) ? =>
		let all: Array[NetworkAddress] val = recover val [as NetworkAddress: 1;2;3;4;5] end
		let without: Array[NetworkAddress] = ArrayWithout[NetworkAddress].without(4, all)?
		h.assert_eq[USize](4, without.size())
		h.assert_true(without.contains(1))
		h.assert_true(without.contains(2))
		h.assert_true(without.contains(3))
		h.assert_true(without.contains(5))
		h.assert_false(without.contains(4))

class iso _TestAppendDropConflictingLogEntries is UnitTest
	""" Tests that followers drop conflicinting log entries. """

	// create a replica and drive it into the follower state
	// use a mock leader to append entries (that are not committed)
	// now use a mock leader to send conflicting entries (overlapping index but different terms)
	// check that the follower:
	//   - drops the conflicting entries (need a feedback channel for this), and
	//   - accepts the new log entries (responds true to append).

	new iso create() => None
	fun name(): String => "raft:server:append-drop-conflict"
	fun ref apply(h: TestHelper) =>
		h.fail("not yet implemented")

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

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)
		h.expect_action("got-follower-start")
		h.expect_action("got-append-success-one")
		h.expect_action("got-append-success-two")
		h.expect_action("got-append-failure-three")

		let receiver_candidate_id: NetworkAddress = 1 // actual replica server being tested
		let peer_one_id: NetworkAddress = 2 // observer validating the replies

		// create a network
		let netmon = EnvNetworkMonitor(h.env)
		let net = Network[RaftSignal[DummyCommand]](netmon)

		// set up a monitor to wait for state changes and to trigger the mock leader
		let mock_leader: _AppendRejectNoPrevMockLeader = _AppendRejectNoPrevMockLeader(h
													, net, peer_one_id, receiver_candidate_id)
		let mon: RaftServerMonitor iso = FollowerAppendMonitor(h, mock_leader)

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](receiver_candidate_id, DummyMachine, _timers, net
			, [as NetworkAddress: receiver_candidate_id; peer_one_id ], DummyCommand.start()
			where monitor = consume mon)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock_leader)

		// register network endpoints
		net.register(receiver_candidate_id, replica)
		net.register(peer_one_id, mock_leader)

actor _AppendRejectNoPrevMockLeader is RaftEndpoint[DummyCommand]

	let _h: TestHelper
	let _net: Network[RaftSignal[DummyCommand]]
	let _leader_id: NetworkAddress
	let _follower_id: NetworkAddress

	new create(h: TestHelper
		, net: Network[RaftSignal[DummyCommand]]
		, leader_id: NetworkAddress, follower_id: NetworkAddress) =>
		_h = h
		_net = net
		_leader_id = leader_id
		_follower_id = follower_id

	be apply(signal: RaftSignal[DummyCommand]) => None
		match consume signal
		| let s: VoteRequest =>
			_h.env.out.print("got vote request in mock leader: " + _leader_id.string()
					+ " for term: " + s.term.string()
					+ " from candidate: " + s.candidate_id.string()
				)
			_h.fail("mock leader should not get a vote request")
		| let s: AppendEntriesRequest[DummyCommand] =>
			_h.fail("mock leader should not get a vote append requests")
		| let s: AppendEntriesResult =>
			_h.env.out.print("got append result in mock leader: " + _leader_id.string())
		end

	be lead_one() =>
		// start by assuming that the follower has nothing, hence (prev_log_index, prev_log_term) = (0,0)
		let append: AppendEntriesRequest[DummyCommand] iso = recover iso AppendEntriesRequest[DummyCommand] end
		append.term = 1
		append.prev_log_index = 0
		append.prev_log_term = 0
		append.leader_commit = 0
		append.leader_id = _leader_id

		// add a single command to the partial log
		append.entries.clear()
		let cmd: DummyCommand = DummyCommand
		let l: Log[DummyCommand] iso = recover iso Log[DummyCommand](1, cmd) end
		append.entries.push(consume l)

		// send the log
		_net.send(_follower_id, consume append)

	be lead_two() =>
		// continue with a valid update e.g. (prev_log_index, prev_log_term) = (1,1)
		let append: AppendEntriesRequest[DummyCommand] iso = recover iso AppendEntriesRequest[DummyCommand] end
		append.term = 1
		append.prev_log_index = 1
		append.prev_log_term = 1
		append.leader_commit = 0
		append.leader_id = _leader_id

		// add a single command to the partial log
		append.entries.clear()
		let cmd: DummyCommand = DummyCommand
		let l: Log[DummyCommand] iso = recover iso Log[DummyCommand](1, cmd) end
		append.entries.push(consume l)

		// send the log
		_net.send(_follower_id, consume append)

	be lead_three() =>
		// now publish an out of sequence log entry e.g. (prev_log_index, prev_log_term) = (2,3)
		// (here the index should match but the term will be out of sync and larger)
		let append: AppendEntriesRequest[DummyCommand] iso = recover iso AppendEntriesRequest[DummyCommand] end
		append.term = 4
		append.prev_log_index = 2
		append.prev_log_term = 3
		append.leader_commit = 0
		append.leader_id = _leader_id

		// add a single command to the partial log
		append.entries.clear()
		let cmd: DummyCommand = DummyCommand
		let l: Log[DummyCommand] iso = recover iso Log[DummyCommand](1, cmd) end
		append.entries.push(consume l)

		// send the log
		_net.send(_follower_id, consume append)

class iso FollowerAppendMonitor is RaftServerMonitor

	"""
	Waits for the replica to become a follower.
	Then starts the mock leader, which will append entries.
	Waits to check that the appends succeed.
	"""

	let _h: TestHelper
	let _mock_leader: _AppendRejectNoPrevMockLeader
	var _seen_follower: Bool
	var _count_append: U16

	new iso create(h: TestHelper, mock_leader: _AppendRejectNoPrevMockLeader) =>
		_h = h
		_mock_leader = mock_leader
		_seen_follower = false
		_count_append = 0

	fun ref state_changed(mode: RaftMode, term: RaftTerm) =>
		match mode
		| Follower =>
			_h.env.out.print("got state Follower for term " + term.string())
			_h.complete_action("got-follower-start")
			if (not _seen_follower) then
				_h.env.out.print("triggering mock lead one")
				_mock_leader.lead_one()
			end
			_seen_follower = true
		| Candidate =>
			_h.fail("follower should not become a candidate")
		| Leader =>
			_h.fail("follower should not become a leader")
		end

	fun ref append_accepted(leader_id: NetworkAddress, term: RaftTerm, success: Bool) =>
		_h.env.out.print("got append accept: " + success.string()
											+ " term: " + term.string() + " leader: " + leader_id.string())
		_count_append = _count_append + 1
		let pass: String = if success then "success" else "failure" end
		let num: String val = Digits.number(_count_append)
		let token: String = "got-append-" + pass + "-" + num

		match (_count_append, success)
		| (1, true) =>
			_h.env.out.print("triggering mock lead two")
			_mock_leader.lead_two()
		| (2, true) =>
			_h.env.out.print("triggering mock lead three")
			_mock_leader.lead_three()
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

		let receiver_candidate_id: NetworkAddress = 1 // actual replica server being tested
		let peer_one_id: NetworkAddress = 2 // observer validating the replies
		let peer_two_id: NetworkAddress = 3 // another peer

		// create a network
		let netmon = EnvNetworkMonitor(h.env)
		let net = Network[RaftSignal[DummyCommand]](netmon)
		h.expect_action("got-follower-start")
		h.expect_action("got-candidate-convert") // expecting RequestVote in the mock following this
		h.expect_action("got-vote-request")
		h.expect_action("got-leader-convert")
		h.expect_action("got-heartbeat-one")
		h.expect_action("got-heartbeat-two")

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor iso = LeaderRaftServerMonitor(h)

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](receiver_candidate_id, DummyMachine, _timers, net
			, [as NetworkAddress: receiver_candidate_id; peer_one_id; peer_two_id], DummyCommand.start()
			where monitor = consume mon)
		let peer_one = GrantVoteMockRaftServer(h, net, peer_one_id)
		let peer_two = GrantVoteMockRaftServer(h, net, peer_two_id)
		h.dispose_when_done(replica)
		h.dispose_when_done(peer_one)
		h.dispose_when_done(peer_two)

		// register network endpoints
		net.register(receiver_candidate_id, replica)
		net.register(peer_one_id, peer_one)
		net.register(peer_two_id, peer_two)

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

		let receiver_candidate_id: NetworkAddress = 1 // actual replica server being tested
		let peer_one_id: NetworkAddress = 2 // observer validating the replies
		let peer_two_id: NetworkAddress = 3 // another peer

		// create a network
		let netmon = EnvNetworkMonitor(h.env)
		let net = Network[RaftSignal[DummyCommand]](netmon)
		h.expect_action("got-follower-start")
		h.expect_action("got-candidate-convert") // expecting RequestVote in the mock following this
		h.expect_action("got-vote-request")
		h.expect_action("got-leader-convert")

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor iso = LeaderRaftServerMonitor(h)

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](receiver_candidate_id, DummyMachine, _timers, net
			, [as NetworkAddress: receiver_candidate_id; peer_one_id; peer_two_id], DummyCommand.start()
			where monitor = consume mon)
		let peer_one = GrantVoteMockRaftServer(h, net, peer_one_id)
		let peer_two = GrantVoteMockRaftServer(h, net, peer_two_id)
		h.dispose_when_done(replica)
		h.dispose_when_done(peer_one)
		h.dispose_when_done(peer_two)

		// register network endpoints
		net.register(receiver_candidate_id, replica)
		net.register(peer_one_id, peer_one)
		net.register(peer_two_id, peer_two)

class iso LeaderRaftServerMonitor is RaftServerMonitor

	let _h: TestHelper
	var _seen_follower: Bool
	var _is_candidate: Bool

	new iso create(h: TestHelper) =>
		_h = h
		_seen_follower = false
		_is_candidate = false

	fun ref timeout_raised(timeout: RaftTimeout) => None

	fun ref state_changed(mode: RaftMode, term: RaftTerm) =>
		match mode
		| Follower =>
			_h.env.out.print("got state Follower for term " + term.string())
			if _seen_follower and _is_candidate then
				_h.fail("should not convert back to a follower")
			else
				_h.complete_action("got-follower-start")
			end
			_seen_follower = true
		| Candidate =>
			_h.env.out.print("got state Candidate for term " + term.string())
			_is_candidate = true
			_h.complete_action("got-candidate-convert")
		| Leader =>
			_h.env.out.print("got state Leader for term " + term.string())
			_h.complete_action("got-leader-convert")
		end

actor GrantVoteMockRaftServer is RaftEndpoint[DummyCommand]

	let _h: TestHelper
	let _net: Network[RaftSignal[DummyCommand]]
	let _id: NetworkAddress
	var _seen_append: U16

	new create(h: TestHelper, net: Network[RaftSignal[DummyCommand]], faux_peer_id: NetworkAddress) =>
		_h = h
		_net = net
		_id = faux_peer_id
		_seen_append = 0

	be apply(signal: RaftSignal[DummyCommand]) =>
		match consume signal
		| let s: VoteRequest =>
			_h.env.out.print("got vote request in peer: " + _id.string()
					+ " for term: " + s.term.string()
					+ " from candidate: " + s.candidate_id.string()
				)
			_h.complete_action("got-vote-request")

			// reply and grant vote
			let vote: VoteResponse iso = recover iso VoteResponse end
			vote.term = 0 // choose a low term so that the vote is accepted by the candidate
			vote.vote_granted = true
			_net.send(s.candidate_id, consume vote)
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

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)

		let receiver_candidate_id: NetworkAddress = 1 // actual replica server being tested
		let listener_candidate_id: NetworkAddress = 2 // observer validating the replies

		// create a network
		let netmon = EnvNetworkMonitor(h.env)
		let net = Network[RaftSignal[DummyCommand]](netmon)
		h.expect_action("got-follower-start")
		h.expect_action("got-candidate") // expecting RequestVote in the mock
		h.expect_action("got-vote-request")
		h.expect_action("got-follower-convert")

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor iso = object iso is RaftServerMonitor
				let _h: TestHelper = h
				var _seen_follower: Bool = false
				var _is_candidate: Bool = false
				fun ref timeout_raised(timeout: RaftTimeout) => None
				fun ref state_changed(mode: RaftMode, term: RaftTerm) =>
					match mode
					| Follower =>
						_h.env.out.print("got state Follower for term " + term.string())
						if _seen_follower and _is_candidate then
							h.complete_action("got-follower-convert")
						else
							h.complete_action("got-follower-start")
						end
						_seen_follower = true
					| Candidate =>
						_h.env.out.print("got state Candidate for term " + term.string())
						_is_candidate = true
						_h.complete_action("got-candidate")
					end
			end

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](receiver_candidate_id, DummyMachine, _timers, net
			, [as NetworkAddress: receiver_candidate_id; listener_candidate_id], DummyCommand.start()
			where monitor = consume mon)
		let mock = HeartbeatOnVoteMockRaftServer(h, net, listener_candidate_id)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock)

		// register network endpoints
		net.register(receiver_candidate_id, replica)
		net.register(listener_candidate_id, mock)

actor HeartbeatOnVoteMockRaftServer is RaftEndpoint[DummyCommand]

	let _h: TestHelper
	let _net: Network[RaftSignal[DummyCommand]]
	let _id: NetworkAddress

	new create(h: TestHelper, net: Network[RaftSignal[DummyCommand]], faux_leader_id: NetworkAddress) =>
		_h = h
		_net = net
		_id = faux_leader_id

	be apply(signal: RaftSignal[DummyCommand]) =>
		match consume signal
		| let s: VoteRequest =>
			_h.complete_action("got-vote-request")
			// send an append with a lower term
			let append: AppendEntriesRequest[DummyCommand] iso = recover iso AppendEntriesRequest[DummyCommand] end
			append.term = s.term // set the same term seen in the vote request
			append.prev_log_index = 0
			append.prev_log_term = 0
			append.leader_commit = 0
			append.leader_id = _id

			_net.send(s.candidate_id, consume append)
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

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)

		let receiver_follower_id: NetworkAddress = 1 // actual replica server being tested
		let listener_leader_id: NetworkAddress = 2 // observer validating the replies

		// create a network
		let netmon = EnvNetworkMonitor(h.env)
		let net = Network[RaftSignal[DummyCommand]]()
		// NB don't call h.complete(...) if using "expect actions"
		h.expect_action("got-append-false")
		h.expect_action("got-higher-term")
		let sent_term: RaftTerm = 1
		let follower_term: RaftTerm = 5

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](receiver_follower_id, DummyMachine, _timers, net
			, [as NetworkAddress: receiver_follower_id; listener_leader_id], DummyCommand.start()
			where initial_term = follower_term)
		let mock = ExpectFailAppend(h, sent_term)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock)

		// register network endpoints
		net.register(receiver_follower_id, replica)
		net.register(listener_leader_id, mock)

		// send an append with a lower term
		let append: AppendEntriesRequest[DummyCommand] iso = recover iso AppendEntriesRequest[DummyCommand] end
		append.term = sent_term // set a lower term (follower was forced to start with a higher term)
		append.prev_log_index = 0
		append.prev_log_term = 0
		append.leader_commit = 0
		append.leader_id = listener_leader_id

		h.env.out.print("sending append...")
		net.send(receiver_follower_id, consume append)


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

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)

		let receiver_candidate_id: NetworkAddress = 1 // actual replica server being tested
		let listener_candidate_id: NetworkAddress = 2 // observer validating the replies

		// create a network
		let netmon = EnvNetworkMonitor(h.env)
		let net = Network[RaftSignal[DummyCommand]]()
		// NB don't call h.complete(...) if using "expect actions"
		h.expect_action("got-election-timeout")
		h.expect_action("got-canvas") // expecting RequestVote in the mock
		h.expect_action("got-canvas-timeout")
		h.expect_action("got-candidate-state")
		h.expect_action("got-canvas-again") // expecting RequestVote in the mock with a higher term

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor iso = object iso is RaftServerMonitor
				let _h: TestHelper = h
				fun box timeout_raised(timeout: RaftTimeout) =>
					match timeout
					| (let t: ElectionTimeout) => _h.complete_action("got-election-timeout")
					| (let t: CanvasTimeout) => _h.complete_action("got-canvas-timeout")
					end
				fun box state_changed(mode: RaftMode, term: RaftTerm) =>
					match mode
					| (let m: Candidate) =>
						_h.env.out.print("got state Candidate for term " + term.string())
						_h.complete_action("got-candidate-state")
					end
			end

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](receiver_candidate_id, DummyMachine, _timers, net
			, [as NetworkAddress: receiver_candidate_id; listener_candidate_id], DummyCommand.start()
			where monitor = consume mon)
		let mock = ExpectCanvasMockRaftServer(h)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock)

		// register network endpoints
		net.register(receiver_candidate_id, replica)
		net.register(listener_candidate_id, mock)

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

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)

		let receiver_candidate_id: NetworkAddress = 1 // actual replica server being tested
		let listener_candidate_id: NetworkAddress = 2 // observer validating the replies

		// create a network
		let netmon = EnvNetworkMonitor(h.env)
		let net = Network[RaftSignal[DummyCommand]]()
		h.expect_action("got-timeout")
		h.expect_action("got-state")
		h.expect_action("got-canvas") // expecting RequestVote in the mock

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor iso = object iso is RaftServerMonitor
				let _h: TestHelper = h
				fun box timeout_raised(timeout: RaftTimeout) =>
					if (timeout is ElectionTimeout) then
						_h.complete_action("got-timeout")
					end
				fun box state_changed(mode: RaftMode, term: RaftTerm) =>
					if (mode is Candidate) then
						_h.env.out.print("got state Candidate for term " + term.string())
						_h.complete_action("got-state")
					end
			end

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](receiver_candidate_id, DummyMachine, _timers, net
			, [as NetworkAddress: receiver_candidate_id; listener_candidate_id], DummyCommand.start()
			where monitor = consume mon)
		let mock = ExpectCanvasMockRaftServer(h)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock)

		// register network endpoints
		net.register(receiver_candidate_id, replica)
		net.register(listener_candidate_id, mock)

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

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)

		let receiver_candidate_id: NetworkAddress = 1 // actual replica server being tested
		let listener_candidate_id: NetworkAddress = 2 // observer validating the replies

		// create a network
		let netmon = EnvNetworkMonitor(h.env)
		let net = Network[RaftSignal[DummyCommand]](netmon)

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor iso = object iso is RaftServerMonitor
				let _env: Env = h.env
				fun box vote_req(id: NetworkAddress, signal: VoteRequest val) => _env.out.print("vote req: " + id.string() + " term:" + signal.term.string())
				fun box vote_res(id: NetworkAddress, signal: VoteResponse val) => _env.out.print("vote res: " + id.string() + " term:" + signal.term.string())
				fun box append_req(id: NetworkAddress) => _env.out.print("append req: " + id.string())
				fun box append_res(id: NetworkAddress) => _env.out.print("append res: " + id.string())
				fun box command_req(id: NetworkAddress) => _env.out.print("command req: " + id.string())
				fun box command_res(id: NetworkAddress) => _env.out.print("command res: " + id.string())
				fun box install_req(id: NetworkAddress) => _env.out.print("install req: " + id.string())
				fun box install_res(id: NetworkAddress) => _env.out.print("install res: " + id.string())
				fun box timeout_raised(timeout: RaftTimeout) => _env.out.print("timeout raised")
				fun box state_changed(mode: RaftMode, term: RaftTerm) => _env.out.print("mode changed: " + mode.text() + " term:" + term.string())
			end

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](1, DummyMachine, _timers, net, [as NetworkAddress:
				receiver_candidate_id; listener_candidate_id;3], DummyCommand.start()
				where monitor = consume mon)
		let mock = ExpectVoteMockRaftServer(h)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock)

		// register network endpoints
		net.register(receiver_candidate_id, replica)
		net.register(listener_candidate_id, mock)

		// send a vote
		let canvas: VoteRequest iso = recover iso VoteRequest end
		canvas.term = 1
		canvas.last_log_index = 0
		canvas.last_log_term = 0
		canvas.candidate_id = listener_candidate_id

		h.env.out.print("sending vote...")
		net.send(receiver_candidate_id, consume canvas)

primitive DummyCommand
	fun val start(): DummyCommand =>
		DummyCommand

class iso DummyMachine is StateMachine[DummyCommand]
	fun ref accept(command: DummyCommand) => None

class val EnvNetworkMonitor is NetworkMonitor

	let _env: Env

	new val create(env: Env) =>
		_env = env

	fun val dropped(id: NetworkAddress) => _env.out.print("net dropped: " + id.string())
	fun val sent(id: NetworkAddress) => _env.out.print("net sent: " + id.string())

actor ExpectVoteMockRaftServer is RaftEndpoint[DummyCommand]

	let _h: TestHelper

	new create(h: TestHelper) =>
		_h = h

	be apply(signal: RaftSignal[DummyCommand]) =>
		_h.env.out.print("mock got a signal")
		// VoteResponse.{term: U64, vote_granted: Bool}
		match consume signal
		| let s: VoteResponse =>
			_h.env.out.print("mock got vote response, term: " + s.term.string())
			_h.assert_true(s.vote_granted, "mock expected to get a vote")
		else
			_h.fail("mock got an unexpected signal")
		end
		_h.complete(true)
