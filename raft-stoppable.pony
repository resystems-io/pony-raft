interface tag Stoppable is DisposableActor
	be stop() => None
	be dispose() => stop()
