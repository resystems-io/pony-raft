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
		test(_TestStuff)

class iso _TestStuff is UnitTest
	"""Tests stuff. """

	new iso create() => None

	fun name(): String => "raft:stuff"

	fun tear_down(h: TestHelper) => None

	fun ref apply(h: TestHelper) =>
		h.assert_true(true)
