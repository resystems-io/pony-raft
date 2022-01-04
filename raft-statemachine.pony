// -- simple wiring

interface tag NotificationEmitter[T: Any #send]
	"""
	Used by components to emit messages.

	This might be used by a state-machine when it responds to commands,
	or this might be used by clients when the generate commands.
	"""
	be apply(event: T) => None

actor NopNotificationEmitter[T: Any #send] is NotificationEmitter[T]

// -- parameterise the server

primitive NopResponse

interface SimpleStateMachine[T: Any #send, U: Any #send = NopResponse]
	"""
	A state machine manages the internal state transitions that are specific to the
	application logic. A raft essentially drives the state machine once the event
	messages are committed to the journal.

	Note, the state machine is implemented as a class (not an actor). That is, it will
	run within the concurrency context of the raft server.

	State machines must be deterministic. Therefore, given the same sequence of commands,
	they must generate the exact same sequence of responses. However, a given command
	may generate zero or more responses.

	When a raft is _not_ a leader, it will simply squash any output from the
	state-machine.
	"""

	fun ref accept(command: T): U

interface SnapshotSupport
	"""
	Note, the raft server does not have a record of the state-machine's state. Therefore,
	on restart, the state-machine is bootstrapped by replaying all of the commands in
	the log.

	However, in order to optimise the bootstrap, raft supports snapshotting. However,
	this depends on cooperation with the state-machine.
	"""

	fun ref has_snapshot_support(): Bool => false

	fun ref snapshot() /* FIXME */ ? =>
		"""
		Generate a snapshot from this state-machine.
		"""
		// we should probably return some sort of sequence of (offset, Array[U8])
		error

	fun ref bootstrap(/* data: FIXME */) ? =>
		// we should probably consume some sort of seqeuence of (offset, Array[U8])
		error

type StateMachine[T: Any #send, U: Any #send] is (SnapshotSupport & SimpleStateMachine[T,U])
