class ArrRef:
	def __init__(self, A):
		self.A = A
		self.N = len(self.A)

	def get(self,i):
		if(i < self.N):
			return self.A[i]

	def getF(self):
		if(len(self.A) == 2):
			return self.A[0]

	def getS(self):
		if(len(self.A) == 2):
			return self.A[1]

	def eBitsExec(self,w):
		return w.mask&(w.f(self)>>w.offset)

class eBits(ArrRef):
	def __init__(self,bits,offset,f):
		self.mask = (1 << bits) - 1
		self.offset = offset
		self.f = f
