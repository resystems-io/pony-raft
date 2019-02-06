// -- state

class Log[T]
	"""
	Log entries, holding the command along with the term when the entry was received by the leader.
	"""

	var term: U64
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
	var current_term: U64

	// The candidate ID that receieved a vote in the current term
	// (or none if no vote has been cast)
	var voted_for: (U16 | None)

	// The log entries. EAch entry contains a command for the the
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
	var commit_index: U64

	// The index of the highest log applied to the state machine
	// (initialised to 0, increases monotonically)
	var last_applied: U64

	new create() =>
		commit_index = 0
		last_applied = 0

class VolatileLeaderState
	"""
	Recorded in memory in the leader, and reinitialised after an election.
	"""

	// For each server, this is the index of the next log entry to be sent
	// to that server (initialised to leader last log index + 1)
	var next_index: Array[U64]

	// For each server, this is the index of the highest log entry known to
	// replicated on that server (initialised to 0, increases monotonically)
	var match_index: Array[U64]

	new create() =>
		next_index = Array[U64](0)
		match_index = Array[U64](0)
