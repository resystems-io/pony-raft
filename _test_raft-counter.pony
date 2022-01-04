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
		test(_TestSingleSourceNoFailures)
		test(_TestMultipleSourcesNoFailures)
		test(_TestOneRaftPauseResume)
		test(_TestRaftResetVolatile)
		test(_TestRaftResetPersistent)

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
		h.long_test(1_000_000_000)
		h.expect_action("source-1:start")
		h.expect_action("source-1:end:sent=100")
		h.expect_action("source-1:end:ack=100")
		h.expect_action("source-1:end:timeouts=false")
		h.expect_action("raft-1:resumed:1")
		h.expect_action("raft-1:resumed:1;client-messages-after-resume=true")

		// TODO allocate server monitors
		// TODO allocate client and servers
		h.fail_action("not-yet-implemented")

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

	fun name(): String => "raft:counter:reset-persisten"
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
