import io, os, sys

def read(filename):
	with open(filename, 'rb') as f:
		read_data = f.read()
	f.closed
	bString = read_data
	n = len(bString)
	return (bString, n)
