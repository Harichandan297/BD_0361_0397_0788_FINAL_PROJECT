import sys
import numpy as np
import matplotlib.pyplot as plt

#Line Graph for Schedulers.
algo = sys.argv[1]
def line_graph(algo):
	slots1 = []
	sec1  = []
	slots2 = []
	sec2 = []
	slots3 = []
	sec3 = []
	filename = "worker_" + algo + ".txt"
	f = open(filename, "r")

	for line in f.readlines():
		wid, slots, t = line.split('\t')
		if wid == '1':
			slots1.append(int(slots))
			sec1.append(float(t))
		if wid == '2':
			slots2.append(int(slots))
			sec2.append(float(t))
		if wid == '3':
			slots3.append(int(slots))
			sec3.append(float(t))

	plt.plot(sec1, slots1, color = 'green', label = 'worker1')
	plt.plot(sec2, slots2, color = 'red', label = 'worker2')
	plt.plot(sec3, slots3, color = 'blue', label = 'worker3')

	plt.xlabel('Time', fontweight = 'bold')
	plt.ylabel('Slots', fontweight = 'bold')
	plt.yticks(rotation = 90)

	plt.title('Worker occupancy - ' + algo)
	plt.legend()
	plt.show()

line_graph(algo)