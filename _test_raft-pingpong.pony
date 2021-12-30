/**
 * Pony Raft Library
 * Copyright (c) 2019 - Stewart Gebbie. Licensed under the MIT licence.
 * vim: set ts=2 sw=0:
 */
use "ponytest"
use "time"

actor RaftPingPongTests is TestList

	new create(env: Env) =>
		PonyTest(env, this)

	new make() =>
		None

	fun tag tests(test: PonyTest) =>
		test(_TestPingPong)

class val Ping
	""" Sent by the client to fetch a new counter from the server. """

	let expect: U64
	let pinger_address: NetworkAddress

	new iso start() =>
		expect = 0
		pinger_address = 0

	new iso create(_expect: U64, _pinger_address:NetworkAddress) =>
		expect = _expect
		pinger_address = _pinger_address

class val Pong
	""" Sent by the server along with a new counter. """

	let expect: U64
	let counter: U64

	new iso create(_expect: U64, _counter: U64) =>
		expect = _expect
		counter = _counter

actor Pinger is Endpoint[Pong]
	"""
	Ping client.

	This is a single client that then interacts with the Pong raft server.
	"""

	// This will send 'count' pings to the system, and check:
	//  - every ping has a response
	//  - the server count, in the response, increases by one each time
	// Ping messages will be sent in response to pong replies.
	// Ping messages will hold the Pinger actor reference for the Ponger to reply to

	let _replica: RaftEndpoint[Ping]
	let _env: Env
	let _pinger_address: NetworkAddress

	var _run: Bool
	var _expect: U64

	new create(pinger_address: NetworkAddress, replica: RaftEndpoint[Ping] tag, env: Env) =>
		_replica = replica
		_env = env
		_pinger_address = pinger_address
		_run = true
		_expect = 0

	be stop() =>
		_run = false
		_env.out.print("Stopping pinger.")

	be go() =>
		if _run then
			_env.out.print("Sending ping: " + _expect.string())
			// FIXME send to the replica via a Network
			// (not sure if this should be _raft_peer_network, _client_network or a third network?)
			_replica(CommandEnvelope[Ping](Ping(_expect, _pinger_address)))
		end

	be apply(pong: Pong val) =>
		_env.out.print("Pinger got a pong")
		_validate(pong)

	fun ref _validate(pong: Pong val) =>
		if pong.expect == pong.counter then
			// yay, got the expected count
			_env.out.print("Yay! " + pong.expect.string())
		else
			// urg, got an unexpected value
			_env.out.print("Urg! " + pong.expect.string() + " != " + pong.counter.string())
		end
		// maybe a pause should be scheduled...
		go()

class iso Ponger is StateMachine[Ping]
	"""
	Ping/Pong server state machine.

	The Pong state machine will absorb log entries once
	they are ready to be applied, thereby updating the
	server state.
	"""

	let _env: Env
	let _client_network: Transport[Pong]

	var _counter: U64

	new iso create(env: Env, client_transport: Transport[Pong]) =>
		_env = env
		_client_network = client_transport
		_counter = 0

	fun ref accept(command: Ping) =>
		_env.out.print("Ponger state machine received ping: " + command.expect.string())
		_counter = _counter + 1
		let pong: Pong = Pong(command.expect, _counter)
		// send a pong back to the ping client via the client network
		_client_network.unicast(command.pinger_address, consume pong)

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
	let _raft_peer_network: Network[RaftSignal[Ping]]
	let _client_network: Network[Pong]
	var _r1: RaftEndpoint[Ping]
	var _r2: RaftEndpoint[Ping]
	var _r3: RaftEndpoint[Ping]

	new iso create() =>
		_timers = Timers
		// create a network for linking the servers
		_raft_peer_network = IntraProcessNetwork[RaftSignal[Ping]]
		// create a network for responding back to clients
		_client_network = IntraProcessNetwork[Pong]
		// create dummy servers
		_r1 = NopRaftEndpoint[Ping]
		_r2 = NopRaftEndpoint[Ping]
		_r3 = NopRaftEndpoint[Ping]

	fun name(): String => "raft:pingpong"

	fun ref set_up(h: TestHelper) =>
		// create the individual server replicas
		_r1 = RaftServer[Ping](1, _timers, _raft_peer_network, [as U16: 1;2;3]
					, Ponger(h.env, _client_network) , Ping.start() )
		_r2 = RaftServer[Ping](2, _timers, _raft_peer_network, [as U16: 1;2;3]
					, Ponger(h.env, _client_network) , Ping.start() )
		_r3 = RaftServer[Ping](3, _timers, _raft_peer_network, [as U16: 1;2;3]
					, Ponger(h.env, _client_network) , Ping.start() )

	fun ref tear_down(h: TestHelper) =>
		// make sure to stop the servers
		_r1.stop()
		_r2.stop()
		_r3.stop()

	fun ref apply(h: TestHelper) =>

		// FIXME need to inform each replica server of the other servers in the group

		// announce the severs on the network
		_raft_peer_network.register(1, _r1)
		_raft_peer_network.register(2, _r2)
		_raft_peer_network.register(3, _r3)

		// start sending ping commands
		// (this client happens to be talking to raft server 2)
		// (in future we might have the client talk via the 'Raft' that selects a server)
		// FIXME need to rather send commands via a Raft than directly to the RaftServer replica
		let pinger_address: NetworkAddress = 5432
		let pinger: Pinger = Pinger(pinger_address, _r2, h.env)
		_client_network.register(pinger_address, pinger)

		pinger.go()

		// FIXME need to test that the client actually got a pong for every ping

		pinger.stop()

		h.assert_true(true)

		h.complete(true)
