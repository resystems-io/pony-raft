/**
 * Pony Raft Library
 * Copyright (c) 2019 - Stewart Gebbie. Licensed under the MIT licence.
 * vim: set ts=2 sw=0:
 */
use "ponytest"
use "time"

actor RaftTests is TestList

	new create(env: Env) =>
		PonyTest(env, this)

	new make() =>
		None

	fun tag tests(test: PonyTest) =>
		test(_TestPingPong)

class val Ping
	""" Sent by the client to fetch a new counter from the server. """

	let expect: U64
	let pinger: PingerValidator

	new iso start() =>
		expect = 0
		pinger = NopPingerValidator

	new iso create(_expect: U64, _pinger: PingerValidator) =>
		expect = _expect
		pinger = _pinger

class val Pong
	""" Sent by the server to along with a new counter. """

	let expect: U64
	let counter: U64

	new iso create(_expect: U64, _counter: U64) =>
		expect = _expect
		counter = _counter

type PingCommand is Ping

interface tag PingerValidator
	""" Pinger interface. """
	be validate(pong: Pong val) => None

actor NopPingerValidator is PingerValidator

actor Pinger
	"""
	Ping client.

	This is a single client that then interacts with the Pong raft server.
	"""

	// This will send 'count' pings to the system, and check:
	//  - every ping has a response
	//  - the server count, in the response, increases by one each time
	// Ping messages will be sent in response to pong replies.
	// Ping messages will hold the Pinger actor reference for the Ponger to reply to

	let _replica: RaftEndpoint[PingCommand]
	let _env: Env
	let _address: NetworkAddress

	var _run: Bool
	var _expect: U64

	new create(replica: RaftEndpoint[PingCommand] tag, env: Env) =>
		_replica = replica
		_env = env
		_address = 0
		_run = true
		_expect = 0

	be stop() =>
		_run = false
		_env.out.print("Stopping pinger.")

	be go() =>
		if _run then
			_env.out.print("Sending ping: " + _expect.string())
			_replica(CommandEnvelope[PingCommand](_address, Ping(_expect, this)))
		end

	be validate(pong: Pong val) =>
		if pong.expect == pong.counter then
			// yay, got the expected count
			_env.out.print("Yay! " + pong.expect.string())
		else
			// urg, got an unexpected value
			_env.out.print("Urg! " + pong.expect.string() + " != " + pong.counter.string())
		end
		// maybe a pause should be scheduled...
		go()

class iso Ponger is StateMachine[PingCommand]
	""" Ping/Pong server state machine. """

	let _env: Env

	var _counter: U64

	new iso create(env: Env) =>
		_env = env
		_counter = 0

	fun ref accept(command: PingCommand) =>
		_env.out.print("Ponger state machine received ping: " + command.expect.string())
		_counter = _counter + 1
		let pong: Pong = Pong(command.expect, _counter)
		// send a pong back to the ping client
		// FIXME the response to the client should go via the Network and not directly to the tag
		command.pinger.validate(consume pong)

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


	let _timers: Timers
	let _net: Network[RaftSignal[PingCommand]]
	var _r1: RaftEndpoint[PingCommand]
	var _r2: RaftEndpoint[PingCommand]
	var _r3: RaftEndpoint[PingCommand]

	new iso create() =>
		_timers = Timers
		// create a network for linking the servers
		// FIXME the network need to be able to carry raft commands and not just client commands
		_net = Network[RaftSignal[PingCommand]]
		// create dummy servers
		_r1 = NopRaftEndpoint[PingCommand]
		_r2 = NopRaftEndpoint[PingCommand]
		_r3 = NopRaftEndpoint[PingCommand]

	fun name(): String => "raft:pingpong"

	fun ref set_up(h: TestHelper) =>
		// create the individual server replicas
		_r1 = RaftServer[PingCommand](1, Ponger(h.env), _timers, _net, [as U16: 1;2;3], Ping.start() )
		_r2 = RaftServer[PingCommand](2, Ponger(h.env), _timers, _net, [as U16: 1;2;3], Ping.start() )
		_r3 = RaftServer[PingCommand](3, Ponger(h.env), _timers, _net, [as U16: 1;2;3], Ping.start() )

	fun ref tear_down(h: TestHelper) =>
		// make sure to stop the servers
		_r1.stop()
		_r2.stop()
		_r3.stop()

	fun ref apply(h: TestHelper) =>

		// FIXME need to inform each replica server of the other servers in the group

		// announce the severs on the network
		_net.register(1, _r1)
		_net.register(2, _r2)
		_net.register(3, _r3)

		// start sending ping commands
		// (this client happens to be talking to raft server 2)
		// (in future we might have the client talk via the 'Raft' that selects a server)
		// FIXME need to rather send commands via a Raft than directly to the RaftServer replica
		let pinger: Pinger = Pinger(_r2, h.env)

		pinger.go()
		pinger.stop()

		// FIXME need to test that the client actuall got a pong for every ping

		h.assert_true(true)

		h.complete(true)
