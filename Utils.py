import os, math

def log2Up(i):
	a=0
	b = i-1
	while (b > 0):
		b = b >> 1
		a +=1
	return a
