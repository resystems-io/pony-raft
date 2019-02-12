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

class iso _TestRequestVote is UnitTest
	"""Tests response to requesting a vote. """

	// create just one replica
	// create a network
	// register the replica on the network using its candiate ID
	// register a mock trigger replica on the network using a different candiate ID
	// send a vote request to the replica
	// wait for a reply in the mock replica
	// check the expected 'term'
	// check the expected 'vote'

	let _timers: Timers
	let _net: Network[RaftSignal[DummyCommand]]
	let _tidy: Array[Stoppable]

	new iso create() =>
		_timers = Timers
		_net = Network[RaftSignal[DummyCommand]]
		_tidy = Array[Stoppable]

	fun name(): String => "raft:server:vote"

	fun tear_down(h: TestHelper) =>
		for s in _tidy.values() do
			s.stop()
		end

	fun ref apply(h: TestHelper) =>

		let receiver_candidate_id: U16 = 1 // actual replica server being tested
		let listener_candidate_id: U16 = 2 // observer validating the replies

		// TODO set up a monitor that logs to _env.out

		let replica = RaftServer[DummyCommand](1, DummyMachine, _timers, _net, [as U16: 1;2;3] )
		let mock = MockRaftServer
		_tidy.push(replica)
		_tidy.push(mock)

		let canvas: VoteRequest iso = recover iso VoteRequest end
		canvas.term = 1
		canvas.last_log_index = 0
		canvas.last_log_term = 0
		canvas.candidate_id = listener_candidate_id

		_net.send(receiver_candidate_id, consume canvas)

// VoteResponse.{term: U64, vote_granted: Bool}

		h.complete(true)

primitive DummyCommand

class iso DummyMachine is StateMachine[DummyCommand]
	fun ref accept(command: DummyCommand) => None

actor MockRaftServer is RaftEndpoint[DummyCommand]

	new create() =>
		None
