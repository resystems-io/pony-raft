/**
 * Pony Raft Library
 * Copyright (c) 2018 - Stewart Gebbie. Licensed under the MIT licence.
 * vim: set ts=2 sw=0:
 */
use "ponytest"
use "itertools"

actor Main is TestList
	new create(env: Env) =>
		try
			if (env.args.size() == 2) and (env.args(1)? == "demo") then
				let demo = RaftDemo(env)
			else
				PonyTest(env, this)
			end
		else
			env.err.print("Bad command line arguments")
			env.exitcode(1)
		end

	new make() =>
		None

	fun tag tests(test: PonyTest) =>
		WithoutTests.make().tests(test)
		RaftPingPongTests.make().tests(test)
		RaftServerTests.make().tests(test)

// -- demo

actor RaftDemo

	new create(env: Env) => None
