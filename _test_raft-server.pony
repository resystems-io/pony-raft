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
		test(_TestArrayWithout)
		test(_TestRequestVote)
		test(_TestWaitForElection)
		test(_TestWaitForCanvas)
		test(_TestFailLowerTermAppend)
		test(_TestConvertToLeader)
		test(_TestConvertToFollower)
		test(_TestWaitForHeartbeat)

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

class iso _TestWaitForHeartbeat is UnitTest
	""" Tests that a leader gets signal to publish heartbeats. """
	new iso create() => None
	fun name(): String => "raft:server:heartbeat"
	fun ref apply(h: TestHelper) =>
		h.fail("not yet implemented")

class iso _TestConvertToLeader is UnitTest
	""" Tests that a candidate will convert to a leader if it gets enough votes. """
	new iso create() => None
	fun name(): String => "raft:server:convert-to-leader"
	fun ref apply(h: TestHelper) =>
		h.fail("not yet implemented")

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
		let net = Network[RaftSignal[DummyCommand]]()
		h.expect_action("got-follower-start")
		h.expect_action("got-candidate") // expecting RequestVote in the mock
		h.expect_action("got-vote-request")
		h.expect_action("got-follower-convert")

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor = object val is RaftServerMonitor
				let _h: TestHelper = h
				var _seen_follower: Bool = false
				var _is_candidate: Bool = false
				fun val timeout_raised(timeout: RaftTimeout) => None
				fun val state_changed(mode: RaftMode, term: RaftTerm) =>
					match mode
					| Follower =>
						_h.env.out.print("got state Follower for term " + term.string())
						if _seen_follower and _is_candidate then
							h.complete_action("got-follower-convert")
						else
							h.complete_action("got-follower-start")
						end
						_seen_follower = true
						_is_candidate = true
					| Candidate =>
						_h.env.out.print("got state Candidate for term " + term.string())
						_is_candidate = true
						_h.complete_action("got-candidate")
					end
			end

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](receiver_candidate_id, DummyMachine, _timers, net
			, [as NetworkAddress: receiver_candidate_id; listener_candidate_id], mon)
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

	new create(h: TestHelper, net: Network[RaftSignal[DummyCommand]], id: NetworkAddress) =>
		_h = h
		_net = net
		_id = id

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
			, [as NetworkAddress: receiver_follower_id; listener_leader_id]
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
		let mon: RaftServerMonitor = object val is RaftServerMonitor
				let _h: TestHelper = h
				fun val timeout_raised(timeout: RaftTimeout) =>
					match timeout
					| (let t: ElectionTimeout) => _h.complete_action("got-election-timeout")
					| (let t: CanvasTimeout) => _h.complete_action("got-canvas-timeout")
					end
				fun val state_changed(mode: RaftMode, term: RaftTerm) =>
					match mode
					| (let m: Candidate) =>
						_h.env.out.print("got state Candidate for term " + term.string())
						_h.complete_action("got-candidate-state")
					end
			end

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](receiver_candidate_id, DummyMachine, _timers, net
			, [as NetworkAddress: receiver_candidate_id; listener_candidate_id], mon)
		let mock = ExpectCanvasMockRaftServer(h)
		h.dispose_when_done(replica)
		h.dispose_when_done(mock)

		// register network endpoints
		net.register(receiver_candidate_id, replica)
		net.register(listener_candidate_id, mock)

class iso _TestWaitForElection is UnitTest
	""" Tests that a follower will start an election if it does not receive a heartbeat. """

	// simply start a replica which will default to 'follower' mode
	// wait for the timeout via the monitor
	// wait for the state change via the monitor
	// conclude

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:server:election"

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
		let mon: RaftServerMonitor = object val is RaftServerMonitor
				let _h: TestHelper = h
				fun val timeout_raised(timeout: RaftTimeout) =>
					if (timeout is ElectionTimeout) then
						_h.complete_action("got-timeout")
					end
				fun val state_changed(mode: RaftMode, term: RaftTerm) =>
					if (mode is Candidate) then
						_h.env.out.print("got state Candidate for term " + term.string())
						_h.complete_action("got-state")
					end
			end

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](receiver_candidate_id, DummyMachine, _timers, net
			, [as NetworkAddress: receiver_candidate_id; listener_candidate_id], mon)
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
		let mon: RaftServerMonitor = object val is RaftServerMonitor
				let _env: Env = h.env
				fun val vote_req(id: NetworkAddress, signal: VoteRequest val) => _env.out.print("vote req: " + id.string())
				fun val vote_res(id: NetworkAddress, signal: VoteResponse val) => _env.out.print("vote res: " + id.string())
				fun val append_req(id: NetworkAddress) => _env.out.print("append req: " + id.string())
				fun val append_res(id: NetworkAddress) => _env.out.print("append res: " + id.string())
				fun val command_req(id: NetworkAddress) => _env.out.print("command req: " + id.string())
				fun val command_res(id: NetworkAddress) => _env.out.print("command res: " + id.string())
				fun val install_req(id: NetworkAddress) => _env.out.print("install req: " + id.string())
				fun val install_res(id: NetworkAddress) => _env.out.print("install res: " + id.string())
			end

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](1, DummyMachine, _timers, net, [as NetworkAddress:
				receiver_candidate_id; listener_candidate_id;3], mon)
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
			_h.env.out.print("mock got vote response")
			_h.assert_true(s.vote_granted, "mock expected to get a vote")
		else
			_h.fail("mock got an unexpected signal")
		end
		_h.complete(true)
