_TRANS_THRESHHOLD = 64

class transpose:

	def __init__(self, A, B):
		self.A = A
		self.B = B
	
	def transR(self, rStart, rCount, rLength, cStart, cCount, cLength):
		if ((cCount < _TRANS_THRESHHOLD) and (rCount < _TRANS_THRESHHOLD)):
			for i in range(rStart, rStart+rCount):
				for j in range(cStart, cStart+cCount):
					B[j*cLength + i] = A[i*rLength + j]
		elif (cCount > rCount):
			l1 = cCount/2
			l2 = cCount - cCount/2
			transR(rStart,rCount,rLength,cStart,l1,cLength)
			transR(rStart,rCount,rLength,cStart + l1,l2,cLength)
		else:
			l1 = rCount/2
			l2 = rCount - rCount/2
			transR(rStart,l1,rLength,cStart,cCount,cLength)
			transR(rStart + l1,l2,rLength,cStart,cCount,cLength)

	def trans(self, rCount, cCount):
		transR(0,rCount,cCount,0,cCount,rCount)


class blockTrans:

	def __init__(self, A, B, OA, OB, L):
		self. A = A
		self.B = B
		self.OA = OA
		self.OB = OB
		self.L = L

	def transR(self, rStart, rCount, rLength, cStart, cCount, cLength):
		if ((cCount < _TRANS_THRESHHOLD) and (rCount < _TRANS_THRESHHOLD)):
			for i in range(rStart, rStart+rCount):
                		for j in range(cStart, cStart+cCount):
					pa = A + OA[i*rLength + j]
					pb = B + OB[j*cLength + i]
					l= L[i*rLength + j]
					for k in range(0,k):
					#revisar, por que la idea es ver la direccion de memoria
						pb = pa
						pb += 1
						pa += 1
		elif (cCount > rCount):
			l1 = cCount/2
			l2 = cCount - cCount/2
			transR(rStart,rCount,rLength,cStart,l1,cLength)
			transR(rStart,rCount,rLength,cStart + l1,l2,cLength)
		else:
			l1 = rCount/2
			l2 = rCount - rCount/2
			transR(rStart,l1,rLength,cStart,cCount,cLength)
			transR(rStart + l1,l2,rLength,cStart,cCount,cLength)


	def trans(rCount, cCount):
		transR(0, rCount, cCount, 0, cCount, rCount)
