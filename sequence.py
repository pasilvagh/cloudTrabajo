import Utils

_BSIZE = 2048
_SCAN_LOG_BSIZE = 10
_SCAN_BSIZE = (1 << _SCAN_LOG_BSIZE)


def nblocks(n, bsize):
	return (1 + ((n) - 1)/(bsize))

def pBlocked_for(_ss, _i, _bsize, _ee, body, f, g):
	_s = _ss + _i * (_bsize)
	_e = min(_s + (_bsize), _ee)
	return body(_s, _e, f, g)

def blocked_for(_s, _e, _bsize, body, f, g, js):
	_ss = _s
	_ee = _e
	_n = _ee - _ss
	_l = nblocks(_n, _bsize)
	sums = [0]*l
	#paralelizar
#jobs = [(i, js.submit(pBlocked_for(_ss, i, _bsize, _ee, body, f, g,)) for i in range(0,_l)]
#	for i, job in jobs:
#		sums[i] = job()
	for i in range(0,_l):
		sums[i] = pBlocked_for(_ss, i, _bsize, _ee, body, f, g)
	return sums

def reduceSerial(s, e, f, g):
	r = g(s)
	for j in range(s+1,e):
		r = f(r,g(j))
	return r

def reduce(s, e, f, g, js):
	l = nblocks(e-s, _SCAN_BSIZE)
	if (l <= 1)
		return reduceSerial(s, e, f, g)
	Sums = blocked_for(s, e, _SCAN_BSIZE, reduceSerial, f, g, js)
	r = reduce(0, l, f, ArrRef(Sums))
	del Sums
	return r

def reduce(A, n, f, js):
	return reduce(0, n, f, ArrRef(A)
