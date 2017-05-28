from veloxmr_lib import *

def map(line, output, options):
	for i in line.split():
		if i in output:
			output[i].append('1')
		else:
			output[i] = ['1']

def reduce(key, values, output):
	total_size = 0
	for value in values:
		total_size += int(value)
	
	output[key] = str(total_size);

mapreduce(sys.argv[1], map, reduce, "wc-output")
cat("wc-output")
#mapreduce("wc-output", map, reduce, "total-output")
