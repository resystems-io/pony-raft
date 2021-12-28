use "collections"

primitive ArrayWithout[T: Equatable[T val] val]

	fun val without(t: T, v: Array[T] val): Array[T] ? =>
		let w: Array[T] iso = recover iso Array[T](v.size() - 1) end
		var i: USize = 0
		while (i < v.size()) do
			let v': T = v(i)?
			if v' != t then
				w.push(v')
			end
			i = i+1
		end
		consume w
