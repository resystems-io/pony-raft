// -- parameterise the server

interface SimpleStateMachine[T: Any #send]
	"""
	A state machine manages the internal state transitions that are specific to the
	application logic. A raft essentially drives the state machine once the event
	messages are committed to the journal.
	"""

	fun ref accept(command: T) => None

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

type StateMachine[T: Any #send] is (SnapshotSupport & SimpleStateMachine[T])
