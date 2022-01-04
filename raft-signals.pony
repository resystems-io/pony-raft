/*
 * Raft Signals
 *
 * All data types for messages that need to pass in between raft servers,
 * or between a client and a raft server.
 */

type RaftSignalStandard is (
	  VoteRequest val
	| VoteResponse val
	| AppendEntriesResult val
	| InstallSnapshotRequest val
	| InstallSnapshotResponse val
)

type RaftServerSignal[T: Any val] is (
	  RaftSignalStandard val
	| AppendEntriesRequest[T] val
	| CommandEnvelope[T] val
)

type RaftClientSignal[T: Any val] is (
	ResponseEnvelope[T] val
)

type RaftSignal[T: Any val] is (
	  RaftServerSignal[T] val
	| RaftClientSignal[T] val
)

// -- common types used in signals

type RaftTerm is U64
type RaftIndex is USize

// -- common accessors

interface val HasTerm
	fun val signal_term(): RaftTerm => 0

// -- commands

class val CommandEnvelope[T: Any #send]
	"""
	An envelope to transport commands from the client raft a replica.

	Note, any addressing of the client is client specific and is not
	included in this envelope, but should be part of T.
	"""

	let command: T	// message to be processed by the state machine

	new val create(value: T) =>
		this.command = consume value

class val RaftRedirect[T: Any #send]
	"""
	A signal to inform the client that the message should be sent to
	a different server.
	"""
	let leader_id: NetworkAddress
	let command: T // the message that was sent, and should be redirected

	new val create(id: NetworkAddress, value: T) =>
		this.leader_id = id
		this.command = consume value

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
	var term: RaftTerm

	// Index of the candidates last log entry
	var last_log_index: RaftIndex

	// Term of the candidates last log entry
	var last_log_term: RaftTerm

	// Candidate requesting the vote
	var candidate_id: NetworkAddress

	new create() =>
		term = 0
		last_log_index = 0
		last_log_term = 0
		candidate_id = 0

	fun val signal_term(): RaftTerm =>
		term

class val VoteResponse

	// The current_term, for the candidate to update itself
	var term: RaftTerm

	// A true vote means that the candidate received a vote
	var vote_granted: Bool

	new create() =>
		term = 0
		vote_granted = false

	fun val signal_term(): RaftTerm =>
		term

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
	var term: RaftTerm

	// Index of the log entry immediately preceding the new ones.
	// (i.e. this is the offset from which to and index when accessing interpret entries[])
	var prev_log_index: RaftIndex

	// Term of the `prev_log_index` entry.
	var prev_log_term: RaftTerm

	// Leader's commit index.
	var leader_commit: RaftIndex

	// Leader ID used so that the follower can redirect cilents.
	var leader_id: NetworkAddress

	// Log entries
	// (Note, iso field as per: Viewpoint adaptation)
	// (https://tutorial.ponylang.io/reference-capabilities/combining-capabilities.html)
	embed entries: Array[Log[T] val] iso

	new create() =>
		term = 0
		prev_log_index = 0
		prev_log_term = 0
		leader_commit = 0
		leader_id = 0
		entries = recover iso Array[Log[T] val](0) end

	fun val signal_term(): RaftTerm =>
		term

class val AppendEntriesResult

	// Current term, for the leader to update itself
	var term: RaftTerm

	// True if the follower contained the entry matching prev_log_index and prev_log_term
	var success: Bool

	new create() =>
		term = 0
		success = false

	fun val signal_term(): RaftTerm =>
		term

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
	var term: RaftTerm

	// Leader ID so that the follower can redirect clients
	var leader_id: NetworkAddress

	// The last included index (the snapshot replaces all entries up through to and including this index)
	var last_included_index: RaftIndex

	// The term of the last included index
	var last_included_term: RaftTerm

	// Flag set to true if this is the last chunk
	// (Note, while the leader may send snapshot chunks in order, they may arrive out of order
	//  and therefore care should be take to ensure that all chunks in range 0 -> last chunk
	//  are applied. Once could consider replacing this with a full-snapshot checksum.)
	var done: Bool

	// Byte offset where the chunk is positioned in the snapshot file
	var offset: USize

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

	fun val signal_term(): RaftTerm =>
		term

class val InstallSnapshotResponse

	// Current term, for the leader to update itself
	var term: RaftTerm

	new create() =>
		term = 0

	fun val signal_term(): RaftTerm =>
		term
