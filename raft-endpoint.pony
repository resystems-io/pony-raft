use "collections"
use "time"
use "random"


// TODO consider being more explicit by defining Endpoint relative to Raft and RaftServer

interface tag Stoppable is DisposableActor
	be stop() => None
	be dispose() => stop()

interface tag Endpoint[T: Any #send] is Stoppable
	"""
	Logically an endpoint absorbs a message.

	The implementation might then delegate the message to some other element.
	"""
	be apply(msg: T) => None
	be apply_copy(msg: val->T) => None

actor NopEndpoint[T: Any #send] is Endpoint[T]
	"""
	A dead-end endpoint that doesn't send a message.
	"""
	be apply(msg: T) => None
	be stop() => None

actor SpanningEndpoint[A: Any val, B: Any val] is Endpoint[(A|B)]
	"""
	An endpoint that can process two types and dispatch accordingly.

	Precedence is given to the type 'A' over type 'B' in the case that both match.
	"""

	let _enda: Endpoint[A]
	let _endb: Endpoint[B]

	new create(enda: Endpoint[A], endb: Endpoint[B]) =>
		_enda = consume enda
		_endb = consume endb

	be apply(msg: (A|B)) =>
		// Give explicit precedence to type 'A' in the case that both A and B match
		match msg
		| let m: A => _enda.apply(m)
		else
			match msg
			| let m: B => _endb.apply(m)
			else
				None // not actualy reachable
			end
		end
