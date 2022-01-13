interface tag Stoppable is DisposableActor
	be stop(ready: {():None}iso={()=>None} ) => ready()
	be dispose() => stop()
