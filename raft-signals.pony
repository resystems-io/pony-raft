type RaftSignalStandard is (
	  VoteRequest val
	| VoteResponse val
	| AppendEntriesResult val
	| InstallSnapshotRequest val
	| InstallSnapshotResponse val
)

type RaftServerSignal[T: Any val] is (
	  RaftSignalStandard
	| AppendEntriesRequest[T] val
	| CommandEnvelope[T] val
)

type RaftClientSignal[T: Any val] is (
	ResponseEnvelope[T] val
)

type RaftSignal[T: Any val] is (
	  RaftServerSignal[T]
	| RaftClientSignal[T] val
)

// -- commands

class val CommandEnvelope[T: Any #send]
	"""
	An envelope to transport commands from the client raft a replica.
	"""

	let command: T	// message to be processed by the state machine
	let source: NetworkAddress // ID of the sending client raft, to receive a response on the network
	// TODO consider carrying the TTL

	new val create(value: T) =>
		this.command = consume value
		this.source = 0

class val ResponseEnvelope[T: Any #send]
	"""
	An envelope to transport responses from the state machine back to a client raft.
	"""

	let response: T
	// TODO consider carrying the backpressure or dropped status (if known)
	//      When dropped, the 'response' would be the original command

	new val create(value: T) =>
		this.response = consume value

// -- votes

class val VoteRequest
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
	var candidate_id: NetworkAddress

	new create() =>
		term = 0
		last_log_index = 0
		last_log_term = 0
		candidate_id = 0

class val VoteResponse

	// The current_term, for the candidate to update itself
	var term: U64

	// A true vote means that the candidate received a vote
	var vote_granted: Bool

	new create() =>
		term = 0
		vote_granted = false


// -- append

class val AppendEntriesRequest[T: Any val]
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
	var leader_id: NetworkAddress

	// Log entries
	embed entries: Array[T]

	new create() =>
		term = 0
		prev_log_index = 0
		prev_log_term = 0
		leader_commit = 0
		leader_id = 0
		entries = Array[T](0)

class val AppendEntriesResult

	// Current term, for the leader to update itself
	var term: U64

	// True if the follower contained the entry matching prev_log_index and prev_log_term
	var success: Bool

	new create() =>
		term = 0
		success = false

// -- snapshot

class val InstallSnapshotRequest
	"""
	Invoked by the leader to send chunks of a snapshot to a follower.

	Leaders always send chunks in order.

	# Receiver implementation:
	1. Reply immediately if term < currentTerm
	2. Create new snapshot file if first chunk (offset is 0)
	3. Write data into snapshot file at given offset
	4. Reply and wait for more data chunks if done is false
	5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	6. If existing log entry has same index and term as snapshot’s last included entry,
	   retain log entries following it and reply
	7. Discard the entire log
	8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	"""

	// Leader's term
	var term: U64

	// Leader ID so that the follower can redirect clients
	var leader_id: NetworkAddress

	// The last included index (the snapshot replaces all entries up through to and including this index)
	var last_included_index: U64

	// The term of the last included index
	var last_included_term: U64

	// Flag set to true if this is the last chunk
	var done: Bool

	// Byte offset where the chunk is positioned in the snapshot file
	var offset: U64

	// Raw byte data of the snapshot chunk, starting at the given offset
	embed data: Array[U8]

	new create() =>
		term = 0
		leader_id = 0
		last_included_index = 0
		last_included_term = 0
		done = false
		offset = 0
		data = Array[U8](0)

class val InstallSnapshotResponse

	// Current term, for the leader to update itself
	var term: U64

	new create() =>
		term = 0
