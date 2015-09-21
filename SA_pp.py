import pp, os, sys, time, dill, operator
import readFromFile
from SA_LCP import SA_LCP
import Utils
from ArrRef import ArrRef
from ArrRef import eBits
#from transpose import transpose
#from transpose import blockTrans
from compS import compS

dill.settings['recurse'] = True

_BSIZE = 2048
_SCAN_LOG_BSIZE = 10
_SCAN_BSIZE = (1 << _SCAN_LOG_BSIZE)
_INT_MAX = sys.maxint
MAX_RADIX = 8 
BUCKETS = 256
_TRANS_THRESHHOLD = 64
_F_BSIZE = (2*_SCAN_BSIZE)
_MERGE_BSIZE = 8192

def mod3iss1(i):
	return (i%3 == 1)


def radixBlock(A, B, Tmp, counts, offsets, Boffset, n, m, extract):
	for i in range(0,m):
		counts[i] = 0
	for j in range(0,n):
		k = Tmp[j] = ArrRef(A[j]).eBitsExec(extract)
		counts[k] += 1
	s = Boffset
	for i in range(0,m):
		s += counts[i]
		offsets[i] = s
	for j in range(n-1,-1,-1):
		offsets[Tmp[j]] = offsets[Tmp[j]] - 1
		x = offsets[Tmp[j]]
		B[x] = A[j]

def radixStepSerial(A, B, Tmp, buckets, n, m, extract):
	radixBlock(A, B, Tmp, buckets, buckets, 0, n, m, extract)
	for i in range(0,n):
		A[i] = B[i]
	return

###################

def parallel_rBlock(A, B, Tmp, m, extract, cnts, nn, oB, i):
	od = i*nn
	nni = min(max(n-od,0), nn)
	radixBlock(A+od, B, Tmp+od, cnts + m*i, oB + m*i, od, nni, m, extract)

##################


#Se pudo lograr solo hacerlo hasta un tamano menor
def radixStep(A, B, Tmp, BK, numBK, n, m, top, extract, js):
	expand = 32
	blocks = min(numBK/3,(1+n/(BUCKETS*expand)))
	if (blocks < 2):
		radixStepSerial(A, B, Tmp, BK[0], n, m, extract)
		return
'''
	nn = (n + blocks - 1)/ blocks
	cnts = BK
	oA = (BK + blocks)
	oB = (BK + 2*blocks)

	jobs = [(i, js.submit(pBlocked_for, (_ss, i, _bsize, _ee, body, f, g,), (reduceSerial,))) for i in range(0,blocks)]
        for i, job in jobs:
                sums[i] = job()
        return sums

'''	


def radixLoopBottomUp(A, B, Tmp, BK, numBK, n, bits, top, f, js):
	rounds = 1 + (bits - 1) / MAX_RADIX
	rbits = 1+(bits-1)/rounds
	bitOffset = 0
	while(bitOffset < bits):
		if (bitOffset+rbits > bits):
			rbits = bits-bitOffset
		radixStep(A, B, Tmp, BK, numBK, n, 1 << rbits, top, eBits(rbits,bitOffset,f), js)
		bitOffset += rbits

	
###################

def parallel_rLoopTopD(n, offsets, y, i, A, B, Tmp, BK, bits, f, js):
	segOffset = offsets[i]
	segNextOffset = n if (i == BUCKETS-1) else offsets[i+1]
	segLen = segNextOffset - segOffset
	blocksOffset = (math.floor(segOffset * y)) + i + 1
	blocksNextOffset = (math.floor(segNextOffset * y)) + i + 2
	blockLen = blocksNextOffset - blocksOffset
	radixLoopTopDown(A + segOffset, B + segOffset, Tmp + segOffset, BK + blocksOffset, blockLen, segLen, bits - MAX_RADIX, f, js)

#################


def radixLoopTopDown(A, B, Tmp, BK, numBK, n, bits, f, js):
	if (n == 0):
		return
	if (bits <= MAX_RADIX):
		radixStep(A, B, Tmp, BK, numBK, n, 1 << bits, True, eBits(bits,0,f), js)
	elif(numBK >= BUCKETS+1):
		radixStep(A, B, Tmp, BK, numBK, n, BUCKETS, True, eBits(MAX_RADIX,bits-MAX_RADIX,f), js)
		offsets = BK[0]
		remain = numBK - BUCKETS - 1
		y = remain / n
		jobs = [(i, js.submit(parallel_rLoopTopD, (n, offsets, y, i, A, B, Tmp, BK, bits, f, js), (radixStep,))) for i in range(0,BUCKETS)]
		for i, job in jobs:
			job()
	else:
		radixLoopBottomUp(A, B, Tmp, BK, numBK, n, bits, False, f, js)


def iSort(A, bucketOffsets, n, m, bottomUp, f, js):
	bits = Utils.log2Up(m)
	B = [0]*n
	Tmp = [0]*n
	numBK = 1 + n/(BUCKETS*8)
	BK = [[0]*BUCKETS]*numBK

	if (bottomUp):
		radixLoopBottomUp(A, B, Tmp, BK, numBK, n, bits, True, f, js)
	else:
		radixLoopTopDown(A, B, Tmp, BK, numBK, n, bits, f, js)
	#caso bucket sea no vacio
	if (bucketOffsets != None):
		#paralelizar
		for i in range(0,m):
			bucketOffsets[i] = n
		for i in range(0, n-1):
			v = f(ArrRef(A[i]))
			vn = f(ArrRef(A[i+1]))
			if (v != vn):
				bucketOffsets[vn] = i + 1
		bucketOffsets[f(ArrRef(A[0]))] = 0
		scanIBack(bucketOffsets,bucketOffsets, m, min, n);
	del B
	del Tmp
	del BK


#def iSort(A, bucketOffsets, n, m, f, js):
#	iSort(A, bucketOffsets, n, m, False, f, js)

def iSortInic(A, n, m, f, js):
	iSort(A, None, n, m, False, f, js)

def iSortBottomUp(A, n, m, f, js):
	iSort(A, None, n, m, True, f, js)


def radixSortPair(A, n, m, js):
	iSortInic(A, n, m, ArrRef.getF, js)

def reduceSerial(s, e, f, g):
	r = g(s)
	for j in range(s+1,e):
		r = f(r,g(j))
	return r

#########
def pBlocked_for(_ss, _i, _bsize, _ee, body, f, g):
        _s = _ss + _i * (_bsize)
        _e = min(_s + (_bsize), _ee)
        return body(_s, _e, f, g)

########

def nblocks(n, bsize):
        return (1 + ((n) - 1)/(bsize))

def blocked_for(_s, _e, _bsize, body, f, g, js):
        _ss = _s
        _ee = _e
        _n = _ee - _ss
        _l = nblocks(_n, _bsize)
        sums = [0]*_l
        #paralelizar
	jobs = [(i, js.submit(pBlocked_for, (_ss, i, _bsize, _ee, body, f, g,), (reduceSerial,))) for i in range(0,_l)]
	for i, job in jobs:
                sums[i] = job()
        return sums

def reduce(s, e, f, g, js):
        l = nblocks(e-s, _SCAN_BSIZE)
        if (l <= 1):
                return reduceSerial(s, e, f, g)
        Sums = blocked_for(s, e, _SCAN_BSIZE, reduceSerial, f, g, js)
        r = reduce(0, l, f, ArrRef(Sums).get, js)
	del Sums
	return r

def reduceInit(A, n, f, js):
        return reduce(0, n, f, ArrRef(A).get, js)

############

def fillSS(A,i):
	return (ord(A[i]) + 1)

############

def fillCBig(s, j, bits):
	return [(s[j] << 2*bits) + (s[j+1] << bits) + s[j+2], j]

############

def fillC(s, j):
        return [s[j + 2], j]

############

def fillCFirst(s, i):
	return s[i]

##################
# i = i, j = i - 1
def fillName12(s, i, j):
	if ((s[i] != s[j]) or (s[i + 1] != s[j + 1]) or (s[i+2] != s[j + 2])):
		return 1
	else:
		return 0


##################

def scanSerial(Out, s, e, f, g, zero, inclusive, back, js):
	r = zero
	if (inclusive):
		if (back):
			for i in range(e-1,s-1, -1):
				Our[i] = r = f(r,g(i))
		else:
			for i in range(s, e):
				Out[i] = r = f(r,g(i))
	else:
		if (back):
			for i in range(e-1, s-1, -1):
				t = g(i)
				Out[i] = r
				r = f(r,t)
		else:
			for i in range(s, e):
				t = g(i)
                                Out[i] = r
                                r = f(r,t)
	return r

def scan(Out, s, e, f, g, zero, inclusive, back, js):
	n = e - s
	l = nblocks(n, _SCAN_BSIZE)
	if (l <= 2):
		return scanSerial(Out, s, e, f, g, zero, inclusive, back, js)
	#para blockes mayores a 2
#	Sums = [0]*l
#	Sums = blocked_for(s, e, _SCAN_BSIZE, reduceSerial, f, g, js)
#	total = scan(Sums, 0, l, f, ArrRef(Sums).get, zero, False, back, js)



def scanI(In, Out, n, f, zero, js):
	return scan(Out, 0, n, f, ArrRef(In).get, zero, True, False, js)



##############################

def fillS12(name12, i):
	return name12[i]


######################

def fillSA12(i, SA12, n1):
	l = SA12[i]
	if ((l < n1)):
		return 3 * l + 1
	else:
		return 3 * (l - n1) + 2

#####################

def fillRank(i):
	return i + 2


###################

def fillFl(p, In, i):
	return p(In[i])


####################


def packSerial(Out, Fl, s, e, f, js):
	k = 0
	for i in range(s, e):
		if (Fl[i]):
			Out[k] = f(i)
			k = k + 1
	return k



def pack(Out, Fl, s, e, f, js):
	l = nblocks(e - s, _F_BSIZE)
	if ( l <= 1):
		return packSerial(Out, Fl, s, e, f, js)
	


def packInic(In, Out, Fl, n, js):
	return pack(Out, Fl, 0, n, ArrRef(In).get, js)


def filterI(In, Out, n, p, js):
	Fl = [False]*n
	jobs = [(i, js.submit(fillFl,(p, In, i),)) for i in range(0,n)]
	for i, job in jobs:
		Fl[i] = bool(job())
	m = packInic(In, Out, Fl, n, js)
	del Fl
	return m

########################

def fillD(i, s, s0):
	return [s[s0[i] - 1], s0[i] - 1]

######################

def fillSA0(D, i):
	return D[i][1]


#######################


def binSearch(S, n, v, f):
	T = S #apunta al comienzo de S
	pT = 0
	while (n > 0):
		mid = n/2
		if (f(v, T[mid])):
			n = mid
		else:
			n = (n - mid) - 1
			pT = pT + mid + 1
	return 	pT

#merge(SA0+o, n0-o, SA12+1-o, n12+o-1, SA, comp);
#merge(SA0, n0-o, SA12, n12+o-1, SA, comp.comp,o, js)

def merge(S1, l1, S2, l2, R, f, o, js):
	lr = l1 + l2
	if ( lr > _MERGE_BSIZE):
		if (l2 > l1):
			merge(S2, l2, S1, l1, R, f, js)
		else:
			m1 = l1/2
			m2 = binSearch(S2, l2, S1[m1], f) 
			#paralelizar despues!
			merge(S1, m1, S2, m2, R, f, js)
			merge(S1, l1 - m1, S2, l2 - m2, R, f)
	else:
		#Son punteros, en python seian los indices en las listas
		pR = 0 #inicio de R
		pS1 = 0 + o # inicio de S1
		pS2 = 0 + 1 - o #inicio de S2
		eS1 = pS1 + l1 # corrido en l1
		eS2 = pS2 + l2 #corrido en l2
		while (True):
			if (pS1 == eS1):
				R[pR:pR] = S2[pS2:eS2]
				break
			if (pS2 == eS2):
				R[pR:pR] = S1[pS1:eS1]
				break
			if f(S2[pS2],S1[pS1]):

				R[pR] = S2[pS2]
				pS2 = pS2 + 1
				pR = pR + 1
			else:

				R[pR] = S1[pS1]
				pS1 = pS1 + 1
				pR = pR + 1



def suffixArrayRec(s, n, K, js):
	n = n + 1
	n0 = (n + 2)/3
	n1 = (n + 1)/3
	n12 = n - n0
	C = [0]*n12
	bits = Utils.log2Up(K)
	if (bits < 11):
		jobs = [(i, js.submit(fillCBig,(s, 1 + (i + i + i)/2,bits),)) for i in range(0,n12)]
		for i, job in jobs:
			C[i] = job()

		#iniciar radixSort
		radixSortPair(C, n12, 1 << 3*bits, js)
	else:
		print("con bits mayor a 11")
		jobs = [(i, js.submit(fillC,(s,1 + (i + i + i)/2))) for i in range(0,n12)]
		for i, job in jobs:
			C[i] = job()

		radixSortPair(C, n12, K, js)
		jobs = [(i, js.submit(fillCFirst,(s, C[i][1] + 1),)) for i in range(0,n12)]
		for i, job in jobs:
			C[i][0] = job()
		radixSortPair(C, n12, K, js)
		jobs = [(i, js.submit(fillCFirst,(s, C[i][1]),)) for i in range(0,n12)]
		for i, job in jobs:
			C[i][0] = job()
		radixSortPair(C, n12, K, js)


	sorted12 = [0]*n12
	for i in range(0,n12):
		sorted12[i] = C[i][1]
	del C

	name12 = [0]*n12
	jobs = [(i, js.submit(fillName12,(s, sorted12[i], sorted12[i-1]),)) for i in range(1,n12)]
	for i, job in jobs:
		name12[i] = job()

	name12[0] = 1
###listo hacia arriba
	scanI(name12, name12, n12, operator.__add__, 0, js)
	names = name12[n12-1]

	LCP12 = None
	SA12_LCP = None
	SA12 = None

	if (names < n12):
		s12 = [0]*(n12 + 3)
		s12[n12] = s12[n12 + 1] = s12[n12 + 2] = 0
		jobs = [(i, js.submit(fillS12,(name12, i),)) for i in range(0,n12)]
		for i, job in jobs:
			if (sorted12[i] % 3 == 1):
				s12[sorted12[i] / 3] = job()
			else:
				s12[sorted12[i] / 3 + n1] = job()
		del name12
		del sorted12


		SA12_LCP = suffixArrayRec(s12, n12, names+1, js)
		SA12 = SA12_LCP
		del s12

		jobs = [(i, js.submit(fillSA12,(i, SA12, n1),)) for i in range(0,n12)]
		for i, job in jobs:
			SA12[i] = job()

	else:
		del name12
		SA12 = sorted12

	rank = [0]*(n + 2)
	rank[n] = 1
	rank[n + 1] = 0
	jobs = [(i, js.submit(fillRank,(i,),)) for i in range(0,n12)]
	for i, job in jobs:
		rank[SA12[i]] = job()

	s0 = [0]*n0
	x = filterI(SA12, s0, n12, mod3iss1, js)
	D = [0]*n0
	D[0] = [s[n - 1], n - 1]
	jobs = [(i, js.submit(fillD,(i, s, s0),)) for i in range(0,x)]
	for i, job in jobs:
		D[i + n0 - x] = job()
	radixSortPair(D, n0, K, js)

	SA0 = s0

	jobs = [(i, js.submit(fillSA0,(D, i),)) for i in range(0,n0)]
	for i, job in jobs:
		SA0[i] = job()
	del D
	
	comp = compS(s, rank)
	SA = [0]*n
	o = 1 if (n%3 == 1) else 0
	SA12.extend([-1])
	SA0.extend([-1])
	merge(SA0, n0-o, SA12, n12+o-1, SA, comp.comp,o, js)
	SA12 = SA12[:-1]
	SA0 = SA0[:-1]

	del SA0
	del SA12
	del rank
	print("SA antes de return: ", SA)
	return SA

	


def suffixArray(sa_lcp, js):
	n = sa_lcp.N
	sa_lcp.SS = [0]*(n + 3)
	sa_lcp.SS[n] = sa_lcp.SS[n+1] = sa_lcp.SS[n+2] = 0
	jobs = [(inp, js.submit(fillSS,(sa_lcp.S,inp), )) for inp in range(0,n)]
	for i, job in jobs:
		sa_lcp.SS[i] = job()
	#Reduce para obtener k
	k = 1 + reduceInit(sa_lcp.SS, sa_lcp.N, max, js)
	SA_LCP = suffixArrayRec(sa_lcp.SS, sa_lcp.N, k, js)

fileName = ""

if len(sys.argv) < 2:
	sys.exit('Usage: %s filename' % sys.argv[0])

if not os.path.exists(sys.argv[1]):
	sys.exit('ERROR: file %s was not found!' % sys.argv[1])
else:
	fileName = str(sys.argv[1])

	ppservers = ()
	job_server = pp.Server(1, ppservers=ppservers)
	print (job_server.get_ncpus(), " workers\n")

	(S,n) = readFromFile.read(fileName)
	sa_lcp = SA_LCP(S,n)
	time1 = time.clock()
	SA = suffixArray(sa_lcp, job_server)
	print("time: ",time.clock()-time1)
