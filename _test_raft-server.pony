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
	"""Tests basic life-cycle. """

	new iso create() =>
	  None

	fun name(): String => "raft:server:vote"

	fun tear_down(h: TestHelper) =>
		None

	fun ref apply(h: TestHelper) =>

		h.complete(true)
