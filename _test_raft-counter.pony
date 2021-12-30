/**
 * Pony Raft Library
 * Copyright (c) 2020 - Stewart Gebbie. Licensed under the MIT licence.
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
//		test(_TestTwoRaftPauseResume)
//		test(_TestOneRaftReset)
//		test(_TestTwoRaftReset)

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
		h.expect_action("source-1:end")
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
		h.expect_action("source-1:end")
		h.expect_action("source-2:start")
		h.expect_action("source-2:end")
		h.expect_action("source-3:start")
		h.expect_action("source-3:end")
		h.expect_action("source-4:start")
		h.expect_action("source-4:end")
		h.expect_action("source-5:start")
		h.expect_action("source-5:end")
		h.fail_action("not-yet-implemented")

class iso _TestOneRaftPauseResume is UnitTest
	"""
	Creates a summation raft and a many client sources where one
	raft server pauses and resumes.

	All clients should see the resultants increasing monotonically.
	The test must observe the pause/resume.
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
		h.expect_action("source-1:end")
		h.expect_action("source-2:start")
		h.expect_action("source-2:end")
		h.expect_action("source-3:start")
		h.expect_action("source-3:end")
		h.expect_action("source-4:start")
		h.expect_action("source-4:end")
		h.expect_action("source-5:start")
		h.expect_action("source-5:end")
		h.expect_action("raft-1:resumed:0")
		h.expect_action("raft-1:paused:1")
		h.expect_action("raft-1:resumed:1")
		h.fail_action("not-yet-implemented")
		// TODO need to set up actions to ensure that pause/resume has client work before and after
		//      maybe signal the clients when the anomoly takes place, and include that in the actions
		// register dispose

//		test(_TestMultipleSourcesOneRaftPauseResume)
//		test(_TestMultipleSourcesTwoRaftPauseResume)
//		test(_TestMultipleSourcesOneRaftReset)
//		test(_TestMultipleSourcesTwoRaftReset)
