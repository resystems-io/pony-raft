/**
 * Pony Raft Library
 * Copyright (c) 2017 - Stewart Gebbie. Licensed under the MIT licence.
 * vim: set ts=2 sw=0:
 */
use "ponytest"

actor RaftTests is TestList

	new create(env: Env) =>
		PonyTest(env, this)

	new make() =>
		None

	fun tag tests(test: PonyTest) =>
		test(_TestPingPong)


class iso Ping

	let expect: U64
	let pinger: Pinger

	new iso create(_expect: U64, _pinger: Pinger) =>
		expect = _expect
		pinger = _pinger

class iso Pong

	let expect: U64
	let counter: U64

	new iso create(_expect: U64, _counter: U64) =>
		expect = _expect
		counter = _counter

type PingCommand is Ping

actor Pinger
	""" Ping client. """

	// This will send 'count' pings to the system, and check:
	//  - every ping has a response
	//  - the server count, in the response, increases by one each time
	// Ping messages will be sent in response to pong replies.
	// Ping messages will hold the Pinger actor reference for the Ponger to reply to

	let _replica: RaftServer[PingCommand]

	var _run: Bool

	new create(replica: RaftServer[PingCommand]) =>
		_replica = replica
		_run = true

	be stop() =>
		_run = false

	be go() =>
		None

	be validate(pong: Pong iso) =>
		None

class iso Ponger is StateMachine[PingCommand]
	""" Ping/Pong server state machine. """

	var _counter: U64

	new iso create() =>
		_counter = 0

	fun accept(command: PingCommand) =>
		None

class iso _TestPingPong is UnitTest
	"""Tests basic life-cycle. """

	// Create a 'Ping' client that will send a message, and then expect a counter that
	// strictly increases without gaps.
	//
	// That is, the client will send a ping and get a counter back from the "raft system"
	// even if some of the servers stop function.
	//
	// In order to simulate failures we will route the server-to-server requests over a
	// "network." The test will then be able to disable links between servers in order
	// to simulate partitions.
	//
	// Note, we need to separately simulate the case where the clients can not reach
	// a given server, and need to attempt a different server.
	//
	// Later we will need to simulate node failures by causing the server to loose
	// non-persistent state.

	new iso create() => None

	fun name(): String => "raft:pingpong"

	fun tear_down(h: TestHelper) => None

	fun ref apply(h: TestHelper) =>

		let net: Network[PingCommand] = Network[PingCommand]

		// create the individual server
		let r1: RaftServer[PingCommand] = RaftServer[PingCommand](1, Ponger, net, [as U16: 1;2;3] )
		let r2: RaftServer[PingCommand] = RaftServer[PingCommand](2, Ponger, net, [as U16: 1;2;3] )
		let r3: RaftServer[PingCommand] = RaftServer[PingCommand](3, Ponger, net, [as U16: 1;2;3] )

		// announce the severs on the network
		net.register(1, r1)
		net.register(2, r2)
		net.register(3, r3)

		// start sending ping commands
		let pinger: Pinger = Pinger(r2)

		h.assert_true(true)
