import json
from socket import *
import time
import sys
import random
import numpy as np
import threading

#config_file_path = sys.args[0]
#scheduling_algo = sys.args[1]

master_addr = 'localhost'
master_recv_port = 5000
master_worker_ack_port = 5001

#Taking the tasks and implementation time from job request message.JSON
json_load = json.load(open('Copy of config.json'))['workers']
worker_data= {}
for i in json_load:
	for j in i:
		if j in worker_data:
			worker_data[j].append(i[j])
		else:
			worker_data[j] = [i[j]]
#print(worker_data)

mapper_tasks = {}
reduce_tasks = {}
job_data = {}
def rcv_requests(master_addr, master_recv_port):
	master_request_sckt = socket(AF_INET, SOCK_STREAM)
	master_request_sckt.connect((master_addr, master_recv_port))
	master_request_sckt.recv(task_data)
	#print(task_data.decode())
	with json.load(task_data)['job_id'] as job:
		if job[0] in job_data['job_id']: 
			job_data['job_id'].append(job[0])
		else:
			job_data['job_id'] = [job[0]]

	#Storing all the mapper tasks in an array
	for i in json.load(task_data)["map_tasks"]:
		for j in i:
			if j in mapper_tasks:
				mapper_tasks[j].append(i[j])
			else:
				mapper_tasks[j] = [i[j]]
	job_data['mapper_data'] = mapper_tasks
	
	#Storing all the reducer tasks in an array.
	for i in json.load(task_data)["reduce_tasks"]:
		for j in i:
			if j in mapper_tasks:
				mapper_tasks[j].append(i[j])
			else:
				mapper_tasks[j] = [i[j]]
	job_data['reducer_data'] = reduce_tasks

	
def send_to_workers(master_addr, master_worker_ack_port):	
	sckt = socket(AF_INET, SOCK_STREAM)
	sckt.bind((master_addr,scheduler_algorithm(scheduling_algo)))



def scheduler_algorithm(scheduling_algo):
	if scheduling_algo == 'RAND':
		random_scheduler(worker_data, job_data)
	elif scheduling_algo == 'RR':
		round_robin(worker_data, job_data)
	elif scheduling_algo == 'LL':
		least_loaded(worker_data, job_data)

def random_scheduler(worker_data, job_data):
	worker_port_no = random.randint(0,2)
	if(worker_data['slots'][worker_port_no] > 0)
		worker_data['slots'][worker_port_no] -= 1  
		return worker_data['port'][worker_port_no]
	else:
		random_scheduler(worker_data, mapper_tasks, reduce_tasks)

def round_robin(worker_data, job_data):
	i = 0
	while i < 3:
		if worker_data['slots'][i] > 0:
			return worker_data['port'][i]
		else :
			i++
def least_loaded(worker_data, job_data):
	least = min(worker_data['slots'])
	least_port =  min(worker_data['slots']).index(least)
	return worker_data['ports'][least_port]
