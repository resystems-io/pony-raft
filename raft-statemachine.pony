// -- parameterise the server

interface StateMachine[T: Any #send]
	"""
	A state machine manages the internal state transitions that are specific to the
	application logic. A raft essentially drives the state machine once the event
	messages are committed to the journal.
	"""
	fun ref accept(command: T) => None
