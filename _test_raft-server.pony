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
		test(_TestRequestVote)
		test(_TestWaitForElection)
		test(_TestWaitForCanvas)
		test(_TestWaitForHeartbeat)

class iso _TestWaitForHeartbeat is UnitTest
	""" Tests that a leader gets signal to publish heartbeats. """
	new iso create() => None
	fun name(): String => "raft:server:heartbeat"
	fun ref apply(h: TestHelper) =>
		h.fail("not yet implemented")

class iso _TestWaitForCanvas is UnitTest
	""" Tests that a candidate will restart a new election if an election doesn't conclude in time. """
	new iso create() => None
	fun name(): String => "raft:server:canvas"
	fun ref apply(h: TestHelper) =>
		h.fail("not yet implemented")

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

		// create a network
		let net = Network[RaftSignal[DummyCommand]]()
		h.expect_action("got-timeout")
		h.expect_action("got-state")

		// set up a monitor that logs to _env.out
		let mon: RaftServerMonitor = object val is RaftServerMonitor
				let _h: TestHelper = h
				fun val timeout_raised(timeout: RaftTimeout) =>
					if (timeout is ElectionTimeout) then
						_h.complete_action("got-timeout")
					end
				fun val state_changed(mode: RaftMode, term: U64) =>
					if (mode is Candidate) then
						_h.complete_action("got-state")
					end
			end

		// register components that need to be shut down
		let replica = RaftServer[DummyCommand](1, DummyMachine, _timers, net, [as NetworkAddress: 1], mon)
		h.dispose_when_done(replica)

		// register networkd endpoints
		net.register(receiver_candidate_id, replica)

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
		let netmon = object val is NetworkMonitor
				let _env: Env = h.env
				fun val dropped(id: NetworkAddress) => _env.out.print("net dropped: " + id.string())
				fun val sent(id: NetworkAddress) => _env.out.print("net sent: " + id.string())
			end
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
		let replica = RaftServer[DummyCommand](1, DummyMachine, _timers, net, [as NetworkAddress: 1;2;3], mon)
		let mock = MockRaftServer(h)
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

actor MockRaftServer is RaftEndpoint[DummyCommand]

	let _h: TestHelper

	new create(h: TestHelper) =>
		_h = h

	be apply(signal: RaftSignal[DummyCommand]) =>
		_h.env.out.print("mock got a signal")
		// VoteResponse.{term: U64, vote_granted: Bool}
		match consume signal
		| let s: VoteResponse =>
			_h.env.out.print("mock got vote response")
			_h.assert_true(s.vote_granted, "mock expected to get vote")
		else
			_h.fail("mock got an unexpected signal")
		end
		_h.complete(true)
