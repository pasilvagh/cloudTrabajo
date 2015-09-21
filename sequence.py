def pBlocked_for(_ss, _i, _bsize, _ee, body, f, g):
	_s = _ss + _i * (_bsize)
	_e = min(_s + (_bsize), _ee)
	return body(_s, _e, f, g)

