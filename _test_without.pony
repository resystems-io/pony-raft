/**
 * Pony Raft Library
 * Copyright (c) 2019 - Stewart Gebbie. Licensed under the MIT licence.
 * vim: set ts=2 sw=0:
 */
use "ponytest"
use "time"

actor WithoutTests is TestList

	new create(env: Env) =>
		PonyTest(env, this)

	new make() =>
		None

	fun tag tests(test: PonyTest) =>
		test(_TestArrayWithout)

class iso _TestArrayWithout is UnitTest
	""" Tests removal of an element from an array. """
	new iso create() => None
	fun name(): String => "raft:without:without"
	fun ref apply(h: TestHelper) ? =>
		let all: Array[U32] val = recover val [as U32: 1;2;3;4;5] end
		let without: Array[U32] = ArrayWithout[U32].without(4, all)?
		h.assert_eq[USize](4, without.size())
		h.assert_true(without.contains(1))
		h.assert_true(without.contains(2))
		h.assert_true(without.contains(3))
		h.assert_true(without.contains(5))
		h.assert_false(without.contains(4))
