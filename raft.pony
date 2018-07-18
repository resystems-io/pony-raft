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


// -- votes

class VoteRequest
	"""
	Invoked by candidates to gather votes.

	# Receiver Implementation:

	1. Reply false if term < current_term
	2. If voted_for is null or candidate_id, and candidate’s log is at least as up-to-date
	   as receiver’s log, then grant vote.
	"""

	// Candidate's term
	var term: U64

	// Index of the candidates last log entry
	var last_log_index: U64

	// Term of the candidates last log entry
	var last_log_term: U64

	// Candidate requesting the vote
	var candidate_id: U16

	new create() =>
		term = 0
		last_log_index = 0
		last_log_term = 0
		candidate_id = 0

class VoteResponse

	// The current_term, for the candidate to update itself
	var term: U64

	// A true vote means that the candidate received a vote
	var vote_granted: Bool

	new create() =>
		term = 0
		vote_granted = false


// -- append

class AppendEntriesRequest[T]
	"""
	Invoked by the leader to replicate log entries and also used as a heartbeat.

	# Receiver Implementation:

	1. Reply false if term < current_term
	2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prev_log_term
	3. If an existing entry conflicts with a new one (same index but different terms),
	   delete the existing entry and all that follow it
	4. Append any new entries not already in the log
	5. If leader_commit > commit_index, set commit_index = min(leader_commit, index of last new entry)
	"""

	// Leader's term
	var term: U64

	// Index of the log entry immediately preceding the new ones.
	var prev_log_index: U64

	// Term of the `prev_log_index` entry.
	var prev_log_term: U64

	// Leader's commit index.
	var leader_commit: U64

	// Leader ID used so that the follower can redirect cilents.
	var leader_id: U16

	// Log entries
	embed entries: Array[T]

	new create() =>
		term = 0
		prev_log_index = 0
		prev_log_term = 0
		leader_commit = 0
		leader_id = 0
		entries = Array[T](0)

class AppendEntriesResult

	// Current term, for the leader to update itself
	var term: U64

	// True if the follower contained the entry matching prev_log_index and prev_log_term
	var success: Bool

	new create() =>
		term = 0
		success = false
