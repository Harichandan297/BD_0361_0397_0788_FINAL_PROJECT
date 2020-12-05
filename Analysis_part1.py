
#finding the mean median for job
def mean_median(filename):
	f = open(filename, 'r')
	d = {}
	for i in f.readlines():
		id, t = i.split('\t')
		if id in d:
			d[id] += float(t)
		else:
			d[id] = -float(t)

	val = list(d.values())
	mean = sum(val) / len(val)
	val.sort()
	mid = len(val)//2
	if (len(val) % 2 == 0):
		median = (val[mid] + val[mid-1])/2
	else:
		median = val[mid-1]

	print('Mean for '+filename+' is ', abs(mean))
	print('Median for '+filename+' is ',abs(median), "\n")
	f.close()

mean_median("job_log_RANDOM.txt")
mean_median("task_log_RANDOM.txt")