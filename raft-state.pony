// -- state

class val Log[T: Any #send]
	"""
	Log entries, holding the command along with the term when the entry was
	received by the leader.
	"""

	let term: RaftTerm
	let command: T

	new val create(t: RaftTerm, cmd: T) =>
		term = t
		command = consume cmd

class ref PersistentServerState[T: Any #send]
	"""
	Update on stable storage before responding to consensus module requests."
	"""

	// The latest term that the server has seen
	// (initialised to 0 on first boot, increases monotonically)
	var current_term: RaftTerm

	// The candidate ID that receieved a vote in the current term
	// (or none if no vote has been cast)
	var voted_for: (NetworkAddress | None)

	// The log entries. Each entry contains a command for the the
	// state machine, and the term when the entry was received by
	// the leader. (the first index is 1).
	embed log: Array[Log[T] val]

	new create() =>
		current_term = 0
		voted_for = None
		log = Array[Log[T] val](0)

class VolatileServerState
	"""
	Recorded in memory in all servers.
	"""

	// The index of the highest log entry known to be committed
	// (initialised to 0, increases monotonically)
	var commit_index: RaftIndex

	// The index of the highest log applied to the state machine
	// (initialised to 0, increases monotonically)
	var last_applied: RaftIndex

	new create() =>
		commit_index = 0
		last_applied = 0

class ref VolatileCandidateState
	"""
	Record the number of votes received during an election round.
	"""

	var votes: USize // hmmm, don't really need that size

	new create() =>
		votes = 0

	fun ref vote(granted: Bool = true) =>
		if granted then
			votes = votes + 1
		end

class ref VolatileLeaderState
	"""
	Recorded in memory in the leader, and reinitialised after an election.
	"""

	// For each server, this is the index of the next log entry to be sent
	// to that server (initialised to leader last log index + 1).
	// (each time the append fails for a given peer this is decremented
	//  until common ground is found between the leader and peer).
	var next_index: Array[RaftIndex]

	// For each server, this is the index of the highest log entry known to
	// replicated on that server (initialised to 0, increases monotonically).
	// (each time a peer acknowledges an append match index for that peer
	//  is updated so that the leader can check for a majority of log updates
	//  across its followers and update its commit index. The commit index
	//  then drives applying updates to the state machine in all replicas —
	//  only the leader responds to the client.)
	var match_index: Array[RaftIndex]

	new create() =>
		next_index = Array[RaftIndex](0)
		match_index = Array[RaftIndex](0)
