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
		print("C antes: ", C)
		radixSortPair(C, n12, 1 << 3*bits, js)
	else:
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
	print(C)
	for i in range(0,n12):
		sorted12[i] = C[i][1]
	print("sorted12: ", sorted12)
	del C

	name12 = [0]*n12
	jobs = [(i, js.submit(fillName12,(s, sorted12[i], sorted12[i-1]),)) for i in range(1,n12)]
	for i, job in jobs:
		name12[i] = job()

	name12[0] = 1
	print("name12: ",name12)
##Aca!!!!
	scanI(name12, name12, n12, operator.__add__, 0)
	names = name12[n12-1]

	SA12_LCP
	SA12
	LCP12 = None
	
	


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

#def suffixArray():

fileName = "mississippi"

ppservers = ()
job_server = pp.Server(1, ppservers=ppservers)
print (job_server.get_ncpus(), " workers\n")

(S,n) = readFromFile.read(fileName)
sa_lcp = SA_LCP(S,n)
time1 = time.clock()
SA = suffixArray(sa_lcp, job_server)
print("time: ",time.clock()-time1)
