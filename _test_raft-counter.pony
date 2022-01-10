/**
 * Pony Raft Library
 * Copyright (c) 2021 - Stewart Gebbie. Licensed under the MIT licence.
 * vim: set ts=2 sw=0:
 */
use "ponytest"
use "time"

actor RaftCounterTests is TestList

	new create(env: Env) =>
		PonyTest(env, this)

	new make() =>
		None

	fun tag tests(test: PonyTest) =>
		test(_TestSansRaft)
		test(_TestSingleSourceNoFailures)
		test(_TestMultipleSourcesNoFailures)
		test(_TestOneRaftPauseResume)
		test(_TestRaftResetVolatile)
		test(_TestRaftResetPersistent)

// -- debugging levels

trait _DebugActive
	fun level(): U16
	fun apply(d: _Debug): Bool => level() >= d.level()

primitive _DebugOff is _DebugActive
	fun level(): U16 => 0
primitive _DebugKey is _DebugActive
	fun level(): U16 => 10
primitive _DebugNoisy is _DebugActive
	fun level(): U16 => 100

type _Debug is (_DebugOff | _DebugKey | _DebugNoisy)

// -- a simple counter state machine

primitive CounterAdd
primitive CounterSub

type CounterOp is (CounterAdd | CounterSub)

class val CounterCommand
	let opcode: CounterOp
	let value: U32
	new val create(op: CounterOp, v: U32) =>
		opcode = op
		value = v

primitive CounterCommands
	fun val start(): CounterCommand =>
		CounterCommand(CounterAdd, 0)

class val CounterTotal
	let value: U32
	new val create(v: U32) =>
		value = v

class CounterMachine is StateMachine[CounterCommand,CounterTotal]
	"""
	The counter machine will add or subtrace values from
	its running total.
	"""

	var _total: U32

	new create() =>
		_total = 0

	fun ref accept(cmd: CounterCommand):CounterTotal^ =>
		match cmd.opcode
		| CounterAdd => _total = _total + cmd.value
		| CounterSub => _total = _total - cmd.value
		end
		CounterTotal(_total)

// -- counter client

actor CounterClient

	let _id: U32
	let _h: TestHelper
	let _emitter: NotificationEmitter[CounterCommand]

	var _work: U32		// number of remaining commands to issue
	var _expect: U32	// number of outstanding acks remaining
	var _last: Bool		// true if we won't be getting more work
	var _sent: U32		// number of commands sent
	var _ack: U32			// number of acknowledgements received

	var _ack_timeouts: U32	// number of missed acknowledgements

	var _started: Bool
	var _stopped: Bool

	let _debug: _Debug

	// TODO introduce correlation tokens or correlation state in order to check monotinicity

	new create(h: TestHelper, id: U32, emitter: NotificationEmitter[CounterCommand], debug: _Debug = _DebugOff) =>
		_debug = debug
		_id = id
		_h = h
		_emitter = emitter

		_work = 0
		_expect = 0
		_last = false
		_sent = 0
		_ack = 0

		_ack_timeouts = 0

		_started = false
		_stopped = false

	be work(amount: U32, last: Bool = false) =>
		if _debug(_DebugKey) then _h.env.out.print("client got work...") end
		if not _started then
			_started = true
			_h.complete_action("source-1:start")
		end
		if _last or _stopped then
			// ignore work added after the last call
			return
		end
		_work = _work + amount
		_expect = _expect + amount
		_last = last
		_drain()

	be stop() =>
		if _stopped then return end
		if _debug(_DebugKey) then _h.env.out.print("client forced stop.") end
		// force a stop
		_fin()

	be dispose() =>
		if _stopped then return end
		if _debug(_DebugKey) then _h.env.out.print("client stopped on dispose.") end
		_fin()

	be _drain() =>
		if _work <= 0 then
			return
		end
		_work = _work - 1

		// send a command
		_emitter(CounterCommand(CounterAdd, 5))
		_sent = _sent + 1
		if _debug(_DebugNoisy) then _h.env.out.print("client sending count command: " + _sent.string()) end

		// consume tail...
		_drain()

	fun ref _fin() =>
		if not _stopped then
			_stopped = true
			let t1:String val = "source-" + _id.string() + ":end:sent=" + _sent.string()
			let t2:String val = "source-" + _id.string() + ":end:ack=" + _ack.string()
			let t3:String val = "source-" + _id.string() + ":end:timeouts=" + (not (_ack_timeouts == 0)).string()
			_h.complete_action(t1)
			_h.complete_action(t2)
			_h.complete_action(t3)
			if _debug(_DebugKey) then _h.env.out.print("client fin " + t1 + " " + t2 + " " + t3) end
		end

	be apply(event: CounterTotal) =>
		if _stopped then
			if _debug(_DebugKey) then _h.env.out.print("client got late total...") end
			return
		end
		if _debug(_DebugNoisy) then _h.env.out.print("client got total...") end
		_ack = _ack + 1
		_expect = _expect - 1
		if _last and (_expect == 0) then _fin() end

// -- raft monitoring

class iso _CounterRaftMonitor is (RaftServerMonitor[CounterCommand] & RaftServerMonitorChain[CounterCommand])

	let _h: TestHelper
	let _debug: _Debug
	let _chain_link: (RaftServerMonitor[CounterCommand] | None)

	var _resumed: U32
	var _client_messages_after_resume: Bool
	var _append_heartbeat_after_resume: Bool
	var _append_content_after_resume: Bool

	new iso create(h: TestHelper, chain: (RaftServerMonitor[CounterCommand] | None) = None, debug: _Debug = _DebugOff) =>
		_h = h
		_debug = debug
		_chain_link = consume chain
		_resumed = 0
		_client_messages_after_resume = false
		_append_heartbeat_after_resume = false
		_append_content_after_resume = false

	fun ref _chain() : (RaftServerMonitor[CounterCommand] | None) => _chain_link

	fun ref mode_changed(id: NetworkAddress, term: RaftTerm, mode: RaftMode) =>
		// e.g. "raft-1:term=1;mode=leader"
		let t:String val = "raft-"  + id.string() + ":term=" + term.string() + ";mode=" + mode.string()
		if _debug(_DebugKey) then _h.env.out.print(t) end
		_h.complete_action(t)
		_chain_mode_changed(id, term, mode)

	fun ref control_raised(id: NetworkAddress, term: RaftTerm, mode: RaftMode, control: RaftControl) =>
		let tb:String val = "raft-"  + id.string() + ":control:" + control.string()
		match control
		| Resumed =>
			// detect if this raft was resumed
			_resumed = _resumed + 1
			// e.g. "raft-1:resumed:1"
			let t:String val = tb + ":" + _resumed.string()
			if _debug(_DebugKey) then _h.env.out.print(t) end
			_h.complete_action(t)
			_client_messages_after_resume = false
			_append_heartbeat_after_resume = false
			_append_content_after_resume = false
		else
			let t:String val = tb
			if _debug(_DebugKey) then _h.env.out.print(t) end
		end

	fun ref timeout_raised(id: NetworkAddress, term: RaftTerm, mode: RaftMode, timeout: RaftTimeout) =>
		let t:String val = "raft-"  + id.string() + ":term=" + term.string() + ";mode=" + mode.string()
			+ ";timeout=" + timeout.string()
		if _debug(_DebugKey) then _h.env.out.print(t) end
		_h.complete_action(t)

	fun ref command_req(id: NetworkAddress, term: RaftTerm, mode: RaftMode) =>
		// detect if this raft got messages directly from the client after the last resume
		if _client_messages_after_resume == false then
			_client_messages_after_resume = true
			// e.g "raft-1:resumed:1;client-messages-after-resume=true"
			let t:String val = "raft-"  + id.string() + ":resumed:" + _resumed.string()
				+ ";client-messages-after-resume=" + _client_messages_after_resume.string()
			if _debug(_DebugKey) then _h.env.out.print(t) end
			_h.complete_action(t)
		end

	fun ref append_req(id: NetworkAddress, signal: AppendEntriesRequest[CounterCommand] val) =>
		// detect if this raft got append messages from the leader after the last resume
		if _debug(_DebugKey) then
			_h.env.out.print("raft-" + id.string()
				+ ":appendreq"
				+ ";term=" + signal.term.string()
				+ ";leader_id=" + signal.leader_id.string()
				+ ";leader_commit=" + signal.leader_commit.string()
				+ ";prev_log_index=" + signal.prev_log_index.string()
				+ ";entries.count=" + signal.entries.size().string()
				+ ";trace_seq=" + signal.trace_seq.string()
			)
		end
		var t:(String val | None) = None
		if (signal.entries.size() == 0) then
			if not _append_heartbeat_after_resume then
				_append_heartbeat_after_resume = true
				// e.g "raft-1:resumed:1;append-messages-after-resume=true;heartbeat"
				t = "raft-"  + id.string() + ":resumed:" + _resumed.string()
					+ ";append-messages-after-resume=" + _append_heartbeat_after_resume.string() + ";heartbeat"
			end
		else
			if not _append_content_after_resume then
				_append_content_after_resume = true
				// e.g "raft-1:resumed:1;append-messages-after-resume=true;content"
				t = "raft-"  + id.string() + ":resumed:" + _resumed.string()
					+ ";append-messages-after-resume=" + _append_content_after_resume.string() + ";content"
			end
		end
		match t
		| (let ts: String val) =>
			if _debug(_DebugKey) then _h.env.out.print(ts) end
			_h.complete_action(ts)
		end

	fun ref append_res(id: NetworkAddress, signal: AppendEntriesResult) =>
		let t1:String val = "raft-"  + id.string() + ":appendres"
			+ ";.term=" + signal.term.string()
			+ ";.success=" + signal.success.string()
			+ ";.peer_id=" + signal.peer_id.string()
			+ ";.prev_log_index=" + signal.prev_log_index.string()
			+ ";.entries_count=" + signal.entries_count.string()
			+ ";.trace_seq=" + signal.trace_seq.string()
		if _debug(_DebugKey) then
			_h.env.out.print(t1)
		end
		_h.complete_action(t1)

	fun ref append_accepted(id: NetworkAddress
		, term: RaftTerm
		, mode: RaftMode
		, last_applied_index: RaftIndex
		, commit_index: RaftIndex
		, last_log_index: RaftIndex
		, leader_term: RaftTerm
		, leader_id: NetworkAddress
		, leader_commit_index: RaftIndex
		, leader_prev_log_index: RaftIndex
		, leader_prev_log_term: RaftTerm
		, leader_entry_count: USize
		, appended: Bool
		) =>
		if _debug(_DebugKey) then
			_h.env.out.print("raft-" + id.string()
				+ ":term=" + term.string()
				+ ":mode=" + mode.string()
				+ ":append-accepted"
				+ ";last_applied_index=" + last_applied_index.string()
				+ ";commit_index=" + commit_index.string()
				+ ";last_log_index=" + last_log_index.string()
				+ ";leader_term=" + leader_term.string()
				+ ";leader_id=" + leader_id.string()
				+ ";leader_commit_index=" + leader_commit_index.string()
				+ ";leader_prev_log_index=" + leader_prev_log_index.string()
				+ ";leader_prev_log_term=" + leader_prev_log_term.string()
				+ ";leader_entry_count=" + leader_entry_count.string()
				+ ";appended=" + appended.string()
			)
		end
		// e.g. "raft-5:term=1;mode=follower;append-accept=1;success=true"
		let tb:String val = "raft-"  + id.string() + ":term=" + term.string() + ";mode=" + mode.string()
			+ ";append-accept=" + last_log_index.string()
			+ ";leader=" + leader_id.string()
		let t1:String val = tb
			+ ";success=" + appended.string()
		let t2:String val = tb
			+ ";count=" + leader_entry_count.string()
		let t3:String val = tb
			+ ";prev_log_index=" + leader_prev_log_index.string()
		if _debug(_DebugKey) then
			_h.env.out.print(t1)
			_h.env.out.print(t2)
			_h.env.out.print(t3)
		end
		_h.complete_action(t1)
		_h.complete_action(t2)
		_h.complete_action(t3)

	fun ref state_change(id: NetworkAddress
		, term: RaftTerm
		, mode: RaftMode
		, last_applied_index: RaftIndex
		, commit_index: RaftIndex
		, last_log_index: RaftIndex
		, update_log_index: RaftIndex
		) =>
		let tb:String val = "raft-"  + id.string() + ":term=" + term.string() + ":mode=" + mode.string()
			+ ":state-machine-update"
			+ ";last_applied_index=" + last_applied_index.string()
			+ ";commit_index=" + commit_index.string()
		let tf:String val = tb
			+ ";last_log_index=" + last_log_index.string()
			+ ";update_log_index=" + update_log_index.string()
		if _debug(_DebugKey) then _h.env.out.print(tf) end
		_h.complete_action(tb)
		_h.complete_action(tf)

	fun ref warning(id: NetworkAddress
		, term: RaftTerm
		, mode: RaftMode
		, msg: String val) =>
		let t1:String val = "raft-"  + id.string() + ":term=" + term.string() + ";mode=" + mode.string() + ";warning"
		let t2:String val = t1 + ";msg=" + msg
		if _debug(_DebugKey) then
			_h.env.out.print(t1)
			_h.env.out.print(t2)
		end
		_h.complete_action(t1)
		_h.complete_action(t2)

// -- counter raft tests

interface iso _Runnable
	fun ref apply() => None

class iso _NopRunnable is _Runnable

class iso _TestSansRaft is UnitTest
	"""
	Creates a summation machine and runs it without raft.

	This proves that the state machine does not need to be
	specifically raft aware.
	"""

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:counter:sans-raft"
	fun label(): String => "end-to-end"

	fun ref set_up(h: TestHelper) =>
		None

	fun ref tear_down(h: TestHelper) =>
		None

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)
		h.expect_action("source-1:start")
		h.expect_action("source-1:end:sent=100")
		h.expect_action("source-1:end:ack=100")

		// allocate a state machine and transit
		let sm: CounterMachine iso^ = recover iso CounterMachine end
		let cem = object tag // router for the commands (combines egress and ingress)
				let _sm: CounterMachine iso = consume sm
				var _cl: (CounterClient | None) = None
				be apply(command: CounterCommand) =>
					"""
					Accept a command from the client and apply in the state-machine, routing the result back.
					"""
					// h.env.out.print("cem got command...")
					// apply the command to the state-machine
					let ct = _sm.accept(command)
					// route the result back to the client
					_emit(ct)
				be configure(cl: CounterClient, ready: _Runnable = _NopRunnable) =>
					"""
					Define the route to the client.
					"""
					_cl = cl
					ready()
				fun _emit(ct: CounterTotal) =>
					match _cl
					| (let c: CounterClient) => c.apply(consume ct)
					end
			end

		// allocate clients
		let source0 = CounterClient(h, 1, NopNotificationEmitter[CounterCommand])
		let source1 = CounterClient(h, 1, cem)

		// link clients to the network
		cem.configure(source1, {() =>
			// coordinate the work (after linking)
			source1.work(50)
			source1.work(50, true)
		})


class iso _TestSingleSourceNoFailures is UnitTest
	"""
	Creates a summation raft and a single client source.

	This is a sanity check, and does not test edge cases.
	"""

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:counter:single-source-no-failures"
	fun label(): String => "end-to-end"

	fun ref set_up(h: TestHelper) =>
		None

	fun ref tear_down(h: TestHelper) =>
		None

	fun ref apply(h: TestHelper) =>
		h.long_test(3_000_000_000)

		// set expectations (halting-condition)
		h.expect_action("source-1:start")
		h.expect_action("source-1:end:sent=100")
		h.expect_action("source-1:end:ack=100")
		h.expect_action("source-1:end:timeouts=false")

		// commits
		h.expect_action("raft-1:term=1:mode=leader:state-machine-update;last_applied_index=99;commit_index=100;last_log_index=100;update_log_index=100")
		h.expect_action("raft-2:term=1:mode=follower:state-machine-update;last_applied_index=99;commit_index=100;last_log_index=100;update_log_index=100")
		h.expect_action("raft-3:term=1:mode=follower:state-machine-update;last_applied_index=99;commit_index=100;last_log_index=100;update_log_index=100")
		h.expect_action("raft-4:term=1:mode=follower:state-machine-update;last_applied_index=99;commit_index=100;last_log_index=100;update_log_index=100")
		h.expect_action("raft-5:term=1:mode=follower:state-machine-update;last_applied_index=99;commit_index=100;last_log_index=100;update_log_index=100")

		// leader election
		h.expect_action("raft-1:term=1;mode=leader")
		h.expect_action("raft-2:term=1;mode=follower")
		h.expect_action("raft-3:term=1;mode=follower")
		h.expect_action("raft-4:term=1;mode=follower")
		h.expect_action("raft-5:term=1;mode=follower")

		// processing
		h.expect_action("raft-1:control:resumed:1")
		h.expect_action("raft-1:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-1:term=1;mode=leader;append-accept=1;leader=1;success=true")
		h.expect_action("raft-1:term=1;mode=leader;append-accept=100;leader=1;success=true")
		// ...
		h.expect_action("raft-2:control:resumed:1")
		h.expect_action("raft-2:resumed:1;append-messages-after-resume=true;heartbeat")
		h.expect_action("raft-2:resumed:1;append-messages-after-resume=true;content")
		h.expect_action("raft-2:term=1;mode=follower;append-accept=1;leader=1;success=true")
		h.expect_action("raft-2:term=1;mode=follower;append-accept=100;leader=1;success=true")
		h.expect_action("raft-2:term=1;mode=follower;append-accept=100;leader=1;count=0") // heatbeat after catchup
		// ...
		h.expect_action("raft-3:control:resumed:1")
		h.expect_action("raft-3:resumed:1;append-messages-after-resume=true;heartbeat")
		h.expect_action("raft-3:resumed:1;append-messages-after-resume=true;content")
		h.expect_action("raft-3:term=1;mode=follower;append-accept=1;leader=1;success=true")
		h.expect_action("raft-3:term=1;mode=follower;append-accept=100;leader=1;success=true")
		h.expect_action("raft-3:term=1;mode=follower;append-accept=100;leader=1;count=0") // heatbeat after catchup
		// ...
		h.expect_action("raft-4:control:resumed:1")
		h.expect_action("raft-4:resumed:1;append-messages-after-resume=true;heartbeat")
		h.expect_action("raft-4:resumed:1;append-messages-after-resume=true;content")
		h.expect_action("raft-4:term=1;mode=follower;append-accept=1;leader=1;success=true")
		h.expect_action("raft-4:term=1;mode=follower;append-accept=100;leader=1;success=true")
		h.expect_action("raft-4:term=1;mode=follower;append-accept=100;leader=1;count=0") // heatbeat after catchup
		// ...
		h.expect_action("raft-5:control:resumed:1")
		h.expect_action("raft-5:resumed:1;append-messages-after-resume=true;heartbeat")
		h.expect_action("raft-5:resumed:1;append-messages-after-resume=true;content")
		h.expect_action("raft-5:term=1;mode=follower;append-accept=1;leader=1;success=true")
		h.expect_action("raft-5:term=1;mode=follower;append-accept=100;leader=1;success=true")
		h.expect_action("raft-5:term=1;mode=follower;append-accept=100;leader=1;count=0") // heatbeat after catchup

		// create a local raft proxy
		// (this appears as a "direct state-machine" but actually delegates to the raft)
		let cem1 = object tag
				var _raft: (RaftEndpoint[CounterCommand] tag | None) = None
				be apply(command: CounterCommand) =>
					match _raft
					| (let r: RaftEndpoint[CounterCommand]) => r(CommandEnvelope[CounterCommand](consume command))
					end
				be configure(raft: RaftEndpoint[CounterCommand] tag, ready: _Runnable = _NopRunnable) =>
					_raft = raft
					ready()
			end

		// allocate a client
		let source1 = CounterClient(h, 1, cem1 where debug = _DebugNoisy)

		// detect when the raft gets its first leader and kick off client test work
		// (add this into the monitor chain)
		// (note, because we contrive raft-1 to be the leader, we only chain the first monitor)
		let starter = object iso is RaftServerMonitor[CounterCommand]
				fun ref mode_changed(id: NetworkAddress, term: RaftTerm, mode: RaftMode) => None
					if (id == 1) and (term == 1) and (mode is Leader) then
						if _DebugNoisy(_DebugKey) then h.env.out.print("leader detected, starting client") end
						// drive the client (start once we detect a leader)
						source1.work(50)
						source1.work(50, true)
					end
			end

		// allocate server monitors
		let rmon1: RaftServerMonitor[CounterCommand] iso^ = _CounterRaftMonitor(h where debug = _DebugNoisy, chain = consume starter)
		let rmon2: RaftServerMonitor[CounterCommand] iso^ = _CounterRaftMonitor(h where debug = _DebugNoisy)
		let rmon3: RaftServerMonitor[CounterCommand] iso^ = _CounterRaftMonitor(h where debug = _DebugNoisy)
		let rmon4: RaftServerMonitor[CounterCommand] iso^ = _CounterRaftMonitor(h where debug = _DebugNoisy)
		let rmon5: RaftServerMonitor[CounterCommand] iso^ = _CounterRaftMonitor(h where debug = _DebugNoisy)

		// allocate state machines
		let sm1: CounterMachine iso^ = recover iso CounterMachine end
		let sm2: CounterMachine iso^ = recover iso CounterMachine end
		let sm3: CounterMachine iso^ = recover iso CounterMachine end
		let sm4: CounterMachine iso^ = recover iso CounterMachine end
		let sm5: CounterMachine iso^ = recover iso CounterMachine end

		// allocate a raft peer network
		let netmon = EnvNetworkMonitor(h.env)
		let net: Network[RaftSignal[CounterCommand]] =
			IntraProcessNetwork[RaftSignal[CounterCommand]](netmon)
		let peers: Array[NetworkAddress] val = [as NetworkAddress: 1;2;3;4;5]

		// allocate raft servers
		let initial_delay: U64 = 400_000_000 // 0.4 seconds for raft servers other than raft-1
		let raft1: RaftServer[CounterCommand,CounterTotal] =
			RaftServer[CounterCommand,CounterTotal](1, _timers, net, peers,
					consume sm1, CounterCommands.start()
					where monitor = consume rmon1, initial_processing = Paused, resume_delay = 0) // we give raft1 a head start
		let raft2: RaftServer[CounterCommand,CounterTotal] =
			RaftServer[CounterCommand,CounterTotal](2, _timers, net, peers,
					consume sm2, CounterCommands.start()
					where monitor = consume rmon2, initial_processing = Paused, resume_delay = initial_delay)
		let raft3: RaftServer[CounterCommand,CounterTotal] =
			RaftServer[CounterCommand,CounterTotal](3, _timers, net, peers,
					consume sm3, CounterCommands.start()
					where monitor = consume rmon3, initial_processing = Paused, resume_delay = initial_delay)
		let raft4: RaftServer[CounterCommand,CounterTotal] =
			RaftServer[CounterCommand,CounterTotal](4, _timers, net, peers,
					consume sm4, CounterCommands.start()
					where monitor = consume rmon4, initial_processing = Paused, resume_delay = initial_delay)
		let raft5: RaftServer[CounterCommand,CounterTotal] =
			RaftServer[CounterCommand,CounterTotal](5, _timers, net, peers,
					consume sm5, CounterCommands.start()
					where monitor = consume rmon5, initial_processing = Paused, resume_delay = initial_delay)

		// register replicas in thier network
		// (TODO move to egress routing)
		net.register(1, raft1)
		net.register(2, raft2)
		net.register(3, raft3)
		net.register(4, raft4)
		net.register(5, raft5)

		// configure the client proxy
		// (this must happen-before the client is starts;
		//  and therefore before a leader is elected;
		//  and therefore before the cluster starts)
		cem1.configure(raft1, {() =>
			// rafts start paused (this alleviates races when configuring new intraprocess clusters)
			// resume the rafts
			raft1.ctrl(Resumed)
			raft2.ctrl(Resumed)
			raft3.ctrl(Resumed)
			raft4.ctrl(Resumed)
			raft5.ctrl(Resumed)
		})

		// source1.stop() // FIXME remove: note, we should rely on autostop...

		// dispose components when the test completes
		// (otherwise the test might detect the failure, but pony won't stop)
		h.dispose_when_done(raft1)
		h.dispose_when_done(raft2)
		h.dispose_when_done(raft3)
		h.dispose_when_done(raft4)
		h.dispose_when_done(raft5)
		h.dispose_when_done(source1)

class iso _TestMultipleSourcesNoFailures is UnitTest
	"""
	Creates a summation raft and a many client sources.

	All clients should see the resultants increasing monotonically.
	"""

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:counter:multiple-sources-no-failures"
	fun label(): String => "end-to-end"

	fun ref set_up(h: TestHelper) =>
		None

	fun ref tear_down(h: TestHelper) =>
		None

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)
		h.expect_action("source-1:start")
		h.expect_action("source-1:end:sent=100:ack=100")
		h.expect_action("source-2:start")
		h.expect_action("source-2:end:sent=100:ack=100")
		h.expect_action("source-3:start")
		h.expect_action("source-3:end:sent=100:ack=100")
		h.expect_action("source-4:start")
		h.expect_action("source-4:end:sent=100:ack=100")
		h.expect_action("source-5:start")
		h.expect_action("source-5:end:sent=100:ack=100")
		h.expect_action("raft-1:resumed:1")
		h.expect_action("raft-1:resumed:1;client-messages-after-resume=true")

		// TODO allocate server monitors
		// TODO allocate clients and servers
		// TODO control server failure
		h.fail_action("not-yet-implemented")

class iso _TestOneRaftPauseResume is UnitTest
	"""
	Creates a summation raft and many client sources where one
	raft server pauses and resumes, without affecting the cluster.

	All clients should see the resultants increasing monotonically.
	The test must observe the pause/resume.
	"""

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:counter:minority-pause-resume"
	fun label(): String => "end-to-end"

	fun ref set_up(h: TestHelper) =>
		None

	fun ref tear_down(h: TestHelper) =>
		None

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)
		// all client should see all messages being processed
		h.expect_action("source-1:start")
		h.expect_action("source-1:end:sent=100")
		h.expect_action("source-1:end:ack=100")
		h.expect_action("source-1:end:timeouts=false")
		h.expect_action("source-2:start")
		h.expect_action("source-2:end:sent=100")
		h.expect_action("source-2:end:ack=100")
		h.expect_action("source-2:end:timeouts=false")
		h.expect_action("source-3:start")
		h.expect_action("source-3:end:sent=100")
		h.expect_action("source-3:end:ack=100")
		h.expect_action("source-3:end:timeouts=false")
		h.expect_action("source-4:start")
		h.expect_action("source-4:end:sent=100")
		h.expect_action("source-4:end:ack=100")
		h.expect_action("source-4:end:timeouts=false")
		h.expect_action("source-5:start")
		h.expect_action("source-5:end:sent=100")
		h.expect_action("source-5:end:ack=100")
		h.expect_action("source-5:end:timeouts=false")

		// pause and resume a (non-leader) server
		h.expect_action("raft-1:resumed:1")
		h.expect_action("raft-1:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-1:paused:1")
		h.expect_action("raft-1:resumed:2")
		// in order to check that we paused and resumed during the processing,
		// we explicity monitor for the receipt of client state-machine commands
		// after a resume cycle change.
		h.expect_action("raft-1:resumed:2;client-messages-after-resume=true")
		h.expect_action("raft-1:paused:2")
		// other servers
		h.expect_action("raft-2:resumed:1")
		h.expect_action("raft-2:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-2:paused:1")
		h.expect_action("raft-3:resumed:1")
		h.expect_action("raft-3:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-3:paused:1")
		h.expect_action("raft-4:resumed:1")
		h.expect_action("raft-4:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-4:paused:1")
		h.expect_action("raft-5:resumed:1")
		h.expect_action("raft-5:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-5:paused:1")

		// TODO allocate server monitors
		// TODO allocate clients and servers
		// TODO control server failure
		h.fail_action("not-yet-implemented")
		// TODO register dispose

class iso _TestMajorityRaftPauseResume is UnitTest
	"""
	Creates a summation raft and many client sources where
	the majority of the raft servers are paused, and then resume.

	Clients should observe timeouts because the raft can not
	reply (given that commands won't see a majority).
	"""

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:counter:majority-pause-resume"
	fun label(): String => "end-to-end"

	fun ref set_up(h: TestHelper) =>
		None

	fun ref tear_down(h: TestHelper) =>
		None

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)
		// all client should see all messages being processed
		h.expect_action("source-1:start")
		h.expect_action("source-1:end:sent=100")
		h.expect_action("source-1:end:timeouts=true")
		h.expect_action("source-2:start")
		h.expect_action("source-2:end:sent=100")
		h.expect_action("source-2:end:timeouts=true")
		h.expect_action("source-3:start")
		h.expect_action("source-3:end:sent=100")
		h.expect_action("source-3:end:timeouts=true")
		h.expect_action("source-4:start")
		h.expect_action("source-4:end:sent=100")
		h.expect_action("source-4:end:timeouts=true")
		h.expect_action("source-5:start")
		h.expect_action("source-5:end:sent=100")
		h.expect_action("source-5:end:timeouts=true")

		// pause and resume in a majority
		h.expect_action("raft-1:resumed:1")
		h.expect_action("raft-1:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-1:paused:1")
		h.expect_action("raft-1:resumed:2")
		h.expect_action("raft-1:resumed:2;client-messages-after-resume=true")
		h.expect_action("raft-1:paused:2")
		// ...
		h.expect_action("raft-2:resumed:1")
		h.expect_action("raft-2:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-2:paused:1")
		h.expect_action("raft-2:resumed:2")
		h.expect_action("raft-2:resumed:2;client-messages-after-resume=true")
		h.expect_action("raft-2:paused:2")
		// ...
		h.expect_action("raft-3:resumed:1")
		h.expect_action("raft-3:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-3:paused:1")
		h.expect_action("raft-3:resumed:2")
		h.expect_action("raft-3:resumed:2;client-messages-after-resume=true")
		h.expect_action("raft-3:paused:2")

		// other servers
		h.expect_action("raft-4:resumed:1")
		h.expect_action("raft-4:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-4:resumed:1;stuck=true")
		h.expect_action("raft-4:paused:1")
		// ...
		h.expect_action("raft-5:resumed:1")
		h.expect_action("raft-5:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-4:resumed:1;stuck=true")
		h.expect_action("raft-5:paused:1")

		// TODO allocate server monitors
		// TODO allocate clients and servers
		// TODO control server failure
		h.fail_action("not-yet-implemented")
		// TODO register dispose

class iso _TestRaftResetVolatile is UnitTest
	"""
	Creates a summation raft and many client sources where
	at least one of the raft servers expiriences a volatile reset.

	Clients should be unaffected. The reset server should recover,
	once it starts following and its state machine catches up.
	"""

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:counter:reset-volatile"
	fun label(): String => "end-to-end"

	fun ref set_up(h: TestHelper) =>
		None

	fun ref tear_down(h: TestHelper) =>
		None

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)
		// all client should see all messages being processed
		h.expect_action("source-1:start")
		h.expect_action("source-1:end:sent=100")
		h.expect_action("source-1:end:ack=100")
		h.expect_action("source-2:start")
		h.expect_action("source-2:end:sent=100")
		h.expect_action("source-2:end:ack=100")
		h.expect_action("source-3:start")
		h.expect_action("source-3:end:sent=100")
		h.expect_action("source-3:end:ack=100")
		h.expect_action("source-4:start")
		h.expect_action("source-4:end:sent=100")
		h.expect_action("source-4:end:ack=100")
		h.expect_action("source-5:start")
		h.expect_action("source-5:end:sent=100")
		h.expect_action("source-5:end:ack=100")

		// reset one server
		h.expect_action("raft-1:resumed:1")
		h.expect_action("raft-1:reset=volatile:1")
		h.expect_action("raft-1:reset=volatile:1;state-machine-applied=0")
		h.expect_action("raft-1:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-1:resumed:1;reset=volatile")
		h.expect_action("raft-1:reset=volatile:2")
		h.expect_action("raft-1:reset=volatile:2;state-machine-applied=0") // reapply the state to the machine
		h.expect_action("raft-1:reset=volatile:2;state-machine-applied=1")
		h.expect_action("raft-1:paused:1")

		// other servers
		h.expect_action("raft-2:resumed:1")
		h.expect_action("raft-1:reset=volatile:1")
		h.expect_action("raft-2:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-2:paused:1")
		// ...
		h.expect_action("raft-3:resumed:1")
		h.expect_action("raft-3:reset=volatile:1")
		h.expect_action("raft-3:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-3:paused:1")
		// ...
		h.expect_action("raft-4:resumed:1")
		h.expect_action("raft-4:reset=volatile:1")
		h.expect_action("raft-4:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-4:paused:1")
		// ...
		h.expect_action("raft-5:resumed:1")
		h.expect_action("raft-5:reset=volatile:1")
		h.expect_action("raft-5:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-5:paused:1")

		// TODO allocate server monitors
		// TODO allocate clients and servers
		// TODO control server failure
		h.fail_action("not-yet-implemented")
		// TODO register dispose

class iso _TestRaftResetPersistent is UnitTest
	"""
	Creates a summation raft and many client sources where
	at least one of the raft servers experiences a persistent reset.

	Clients should be unaffected. The reset server should recover,
	once it starts following and its log catches up.
	"""

	let _timers: Timers

	new iso create() =>
		_timers = Timers

	fun name(): String => "raft:counter:reset-persistent"
	fun label(): String => "end-to-end"

	fun ref set_up(h: TestHelper) =>
		None

	fun ref tear_down(h: TestHelper) =>
		None

	fun ref apply(h: TestHelper) =>
		h.long_test(1_000_000_000)
		// all client should see all messages being processed
		h.expect_action("source-1:start")
		h.expect_action("source-1:end:sent=100")
		h.expect_action("source-1:end:ack=100")
		h.expect_action("source-2:start")
		h.expect_action("source-2:end:sent=100")
		h.expect_action("source-2:end:ack=100")
		h.expect_action("source-3:start")
		h.expect_action("source-3:end:sent=100")
		h.expect_action("source-3:end:ack=100")
		h.expect_action("source-4:start")
		h.expect_action("source-4:end:sent=100")
		h.expect_action("source-4:end:ack=100")
		h.expect_action("source-5:start")
		h.expect_action("source-5:end:sent=100")
		h.expect_action("source-5:end:ack=100")

		// reset one server
		h.expect_action("raft-1:resumed:1")
		h.expect_action("raft-1:reset=volatile:1")
		h.expect_action("raft-1:reset=volatile:1;state-machine-applied=0")
		h.expect_action("raft-1:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-1:resumed:1;reset=persistent")
		h.expect_action("raft-1:reset=volatile:2") // note, a persistent reset implies a volatile reset too
		h.expect_action("raft-1:reset=volatile:2;state-machine-applied=0") // reapply the state to the machine
		h.expect_action("raft-1:reset=volatile:2;state-machine-applied=1")
		h.expect_action("raft-1:paused:1")

		// other servers
		h.expect_action("raft-2:resumed:1")
		h.expect_action("raft-1:reset=volatile:1")
		h.expect_action("raft-2:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-2:paused:1")
		// ...
		h.expect_action("raft-3:resumed:1")
		h.expect_action("raft-3:reset=volatile:1")
		h.expect_action("raft-3:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-3:paused:1")
		// ...
		h.expect_action("raft-4:resumed:1")
		h.expect_action("raft-4:reset=volatile:1")
		h.expect_action("raft-4:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-4:paused:1")
		// ...
		h.expect_action("raft-5:resumed:1")
		h.expect_action("raft-5:reset=volatile:1")
		h.expect_action("raft-5:resumed:1;client-messages-after-resume=true")
		h.expect_action("raft-5:paused:1")

		// TODO allocate server monitors
		// TODO allocate clients and servers
		// TODO control server failure
		h.fail_action("not-yet-implemented")
		// TODO register dispose
