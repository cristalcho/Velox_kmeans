from veloxmr_lib import *

def map(line):
	d = { }
	for i in line.split():
		if i in d:
			d[i].append('1')
		else:
			d[i] = ['1']
	return d

def reduce(a,b):
	return str(int(a)+int(b))

mapreduce(sys.argv[1], map, reduce, "wc-output")
cat("wc-output")
mapreduce("wc-output", map, reduce, "total-output")
