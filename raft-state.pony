// -- state

class Log[T]
	"""
	Log entries, holding the command along with the term when the entry was
	received by the leader.
	"""

	var term: RaftTerm
	var command: T

	new create(t: T) =>
		term = 0
		command = consume t

class PersistentServerState[T]
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
	embed log: Array[Log[T]]

	new create() =>
		current_term = 0
		voted_for = None
		log = Array[Log[T]](0)

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

class VolatileCandidateState
	"""
	Record the number of votes received during an election round.
	"""

	var votes: U16

	new create() =>
		votes = 0

	fun ref vote() =>
		votes = votes + 1

class VolatileLeaderState
	"""
	Recorded in memory in the leader, and reinitialised after an election.
	"""

	// For each server, this is the index of the next log entry to be sent
	// to that server (initialised to leader last log index + 1)
	var next_index: Array[RaftIndex]

	// For each server, this is the index of the highest log entry known to
	// replicated on that server (initialised to 0, increases monotonically)
	var match_index: Array[RaftIndex]

	new create() =>
		next_index = Array[RaftIndex](0)
		match_index = Array[RaftIndex](0)
