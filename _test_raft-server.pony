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
	""" Tests that a candidate will convert to a follower a peer is elected. """
	new iso create() => None
	fun name(): String => "raft:server:convert-to-follower"
	fun ref apply(h: TestHelper) =>
		h.fail("not yet implemented")

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

		// register networkd endpoints
		net.register(receiver_candidate_id, replica)
		net.register(listener_candidate_id, mock)

		h.fail("need to actually check that a new election was started")


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
		h.expect_action("got-canvas-again") // expecting RequestVote in the mock with a higher term

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

		// register networkd endpoints
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
				None
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

		// register networkd endpoints
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
