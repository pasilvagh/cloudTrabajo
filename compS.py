class compS:
	def __init__(self, _s, _s12):
		self._s = _s
		self._s12 = _s12

	def leq2(self, a1, a2, b1, b2):
		return((a1 < b1) or (a1 == b1) and (a2 <= b2))

	def leq3(self, a1, a2, a3, b1, b2, b3):
		return((a1 < b1) or (a1 == b1) and (self.leq2(a2, a3, b2, b3)))

	def comp(self, i, j):
		if ((i%3 == 1) or (j%3 == 1)):
			return self.leq2(self._s[i],self._s12[i+1], self._s[j],self._s12[j+1])
		else:
			return self.leq3(self._s[i],self._s[i+1],self._s12[i+2], self._s[j],self._s[j+1],self._s12[j+2]);
