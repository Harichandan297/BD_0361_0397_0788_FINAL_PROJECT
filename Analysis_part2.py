import numpy as np
import matplotlib.pyplot as plt
 
#Analysis files

analyse_random = open("Analysis_RANDOM.txt", 'r')
analyse_rr = open("Analysis_RR.txt", "r")
analyse_ll = open("Analysis_LL.txt", "r")

random = []
rr = []
ll = []
for i in analyse_random.readlines():
	random.append(float(i))
for i in analyse_rr:
	rr.append(float(i))
for i in analyse_ll:
	ll.append(float(i))

# Width of bar
barWidth = 0.1
 
# Height of bar
bars1 = [random[0], rr[0], ll[0]]
bars2 = [random[1], rr[1], ll[1]]
bars3 = [random[2], rr[2], ll[2]]
bars4 = [random[3], rr[3], ll[3]]

r1 = np.arange(len(bars1))
r2 = [x + barWidth for x in r1]
r3 = [x + barWidth for x in r2]
r4 = [x + barWidth for x in r3]

plt.bar(r1, bars1, color='green', width=barWidth, edgecolor='white', label='job mean')
plt.bar(r2, bars2, color='blue', width=barWidth, edgecolor='white', label='job median')
plt.bar(r3, bars3, color='yellow', width=barWidth, edgecolor='white', label='task mean')
plt.bar(r4, bars4, color='red', width=barWidth, edgecolor='white', label='task median')
 
plt.xlabel('group', fontweight='bold')
plt.xticks([r + barWidth for r in range(len(bars1))], ['RANDOM', 'RR', 'LL'])
 
plt.legend()
plt.show()

