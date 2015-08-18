import pp, os, sys, time, dill
import readFromFile
from SA_LCP import SA_LCP
import Utils
from ArrRef import ArrRef
from ArrRef import eBits

dill.settings['recurse'] = True

_BSIZE = 2048
_SCAN_LOG_BSIZE = 10
_SCAN_BSIZE = (1 << _SCAN_LOG_BSIZE)

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
        print("Entroooo")
        #paralelizar
	jobs = [(i, js.submit(pBlocked_for, (_ss, i, _bsize, _ee, body, f, g,), (reduceSerial,))) for i in range(0,_l)]
	for i, job in jobs:
                sums[i] = job()
#	for i in range(0,_l):
#		sums[i] = pBlocked_for(_ss, i, _bsize, _ee, body, f, g)
	print("sums: ", sums)
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
def suffixArray(sa_lcp, js):
	sa_lcp.SS = [0]*sa_lcp.N
	jobs = [(inp, js.submit(fillSS,(sa_lcp.S,inp), )) for inp in range(0,n)]
	for i, job in jobs:
		sa_lcp.SS[i] = job()
	#Reduce para obtener k
	k = 1 + reduceInit(sa_lcp.SS, sa_lcp.N, max, js)
#	print("k: ", str(k))

#def suffixArray():

fileName = "mississippi2"

ppservers = ()
job_server = pp.Server(ppservers=ppservers)
print (job_server.get_ncpus(), " workers\n")

(S,n) = readFromFile.read(fileName)
sa_lcp = SA_LCP(S,n)
time1 = time.clock()
SA = suffixArray(sa_lcp, job_server)
print("time: ",time.clock()-time1)
