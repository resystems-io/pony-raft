interface Textable
	"""
	Use textable when the string is a constant.

	This avoids unnecessary allocation and copying of strings.
	"""
	fun text():String val

trait TextStringable is (Stringable & Textable)
	""" Convert a 'textable' to a 'stringable'. """
	fun string():String iso^ => text().string()
