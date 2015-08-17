import pp, os, sys
import readFromFile
from SA_LCP import SA_LCP


def fillSS(A,i):
	return (ord(A[i]) + 1)


def suffixArray(sa_lcp, js):
	sa_lcp.SS = [0]*sa_lcp.N
	jobs = [(inp, js.submit(fillSS,(sa_lcp.S,inp), )) for inp in range(0,n)]
	for i, job in jobs:
		sa_lcp.SS[i] = job()
	print(sa_lcp.SS)

#def suffixArray():

fileName = "mississippi"

ppservers = ()
job_server = pp.Server(ppservers=ppservers)
print (job_server.get_ncpus(), " workers\n")

(S,n) = readFromFile.read(fileName)
sa_lcp = SA_LCP(S,n)
SA = suffixArray(sa_lcp, job_server)
