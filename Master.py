import json
from socket import *
import time
import sys
import random
import threading

#The system arguments.
config_file_path = sys.argv[1]
scheduling_algo = sys.argv[2]

#The initial address and ports for communication.
master_address = 'localhost'
worker_address = 'localhost'
request_address = 'localhost'
master_job_port = 5000
master_worker_comm_port = 5001

#Dictionaries to store the worker data and the job data.
worker_data= {}
job_data = {}

json_load = json.load(open(config_file_path))
for i in json_load['workers']:
	for j in i:
		if j in worker_data:
			worker_data[j].append(i[j])
		else:
			worker_data[j] = [i[j]]

#Lock for acknowledgemet and slot updates.
worker_ack_lock = threading.Lock()
slot_update_lock = threading.Lock()

#Array for storing received task ids acks for finished tasks.
ack_task_ids = []

#Function for receiving acknowledgements of finished tasks fron workers.
def rcv_ack_workers():
	master_worker_comm_socket = socket(AF_INET, SOCK_STREAM)
	master_worker_comm_socket.bind((worker_address, master_worker_comm_port))
	master_worker_comm_socket.listen(1)
	print("Receiving acks on port ", master_worker_comm_port)
	while True:
		ack_conn, worker_addr = master_worker_comm_socket.accept()
		worker_ack_lock.acquire()
		message = ack_conn.recv(1024)
		task_id = message.decode()
		ack_task_ids.append(task_id)
		filename = "task_log_" + scheduling_algo + ".txt"
		task_file = open(filename,"a")
		task_file.write(task_id)
		task_file.write('\t')
		task_file.write(str(time.time()))
		task_file.write("\n")
		task_file.close()
		print("task ack received for task ", task_id)
		print("task ", task_id, "ended at time ", time.time())
		worker_ack_lock.release()

#Function for updating free slots after the alloted task is finished.
def update_slots(task_id, worker_port_no):
	while True:	
		worker_ack_lock.acquire()
		if task_id in ack_task_ids:
			ack_task_ids.remove(task_id)
			slot_update_lock.acquire()
			worker_data['slots'][worker_port_no] += 1
			slot_update_lock.release()
			worker_ack_lock.release()
			break
		worker_ack_lock.release()

#Function for sending tasks to workers based on ports.
def send_task_workers(worker_data_port, task_id, task_dur):
	task_info = {}
	while True:	
		task_info[task_id] = task_dur
		print("Sending", task_id, "task", "to worker port ",worker_data_port)	
		sckt = socket(AF_INET, SOCK_STREAM)	
		sckt.connect((worker_address, worker_data_port))
		message = str(task_info)
		sckt.send(message.encode())
		sckt.close()
		break
		#rcv_requests_thread.join()

#Random scheduler.
def random_sch(task_id, task_dur):
	#print("in random sch")
	worker_data_port = 0
	while True:
		slot_update_lock.acquire()
		worker_port_no = random.randint(0,2)
		if worker_data['slots'][worker_port_no] > 0 :
			worker_data['slots'][worker_port_no] -= 1
			filename = "worker_RANDOM.txt"
			worker_log = open(filename, "a")
			worker_log.write(str(worker_data['worker_id'][worker_port_no]) + "\t"+ str(worker_data['slots'][worker_port_no])+'\t'+ str(time.time())+"\n")
			worker_log.close()	
			worker_data_port = worker_data['port'][worker_port_no]
			slot_update_lock.release()
			break
		slot_update_lock.release()
	send_worker_thread = threading.Thread(target = send_task_workers, args = (worker_data_port, task_id, task_dur))
	send_worker_thread.start()
	send_worker_thread.join()
	update_slots(task_id, worker_port_no)

#Round robin scheduler.
def round_robin(task_id, task_dur):
	#print('implementing round robin algo')
	worker_data_port = 0
	worker_port_no = 0
	while True:
		slot_update_lock.acquire()
		if worker_data['slots'][worker_port_no] > 0 :
			worker_data['slots'][worker_port_no] -= 1
			filename = "worker_RR.txt"
			worker_log = open(filename, "a")
			worker_log.write(str(worker_data['worker_id'][worker_port_no]) + "\t"+ str(worker_data['slots'][worker_port_no])+'\t'+ str(time.time())+"\n")
			worker_log.close()
			worker_data_port = worker_data['port'][worker_port_no] 
			worker_port_no = (worker_port_no + 1) % 3
			slot_update_lock.release()
			break
		else:
			worker_port_no = (worker_port_no + 1) % 3
			slot_update_lock.release()

	send_worker_thread = threading.Thread(target = send_task_workers, args = (worker_data_port, task_id, task_dur))
	send_worker_thread.start()
	send_worker_thread.join()
	update_slots(task_id, worker_port_no)

#Least loaded slots based scheduler.
def least_loaded(task_id, task_dur):
	#print('implementing ll algo')
	worker_port_no = worker_data['slots'].index(max(worker_data['slots']))
	worker_data_port = worker_data['port'][worker_port_no]
	filename = "worker_LL.txt"
	while True:
		slot_update_lock.acquire()
		if worker_data['slots'][worker_port_no] > 0:
			worker_data['slots'][worker_port_no] -= 1
			worker_log = open(filename, "a")
			worker_log.write(str(worker_data['worker_id'][worker_port_no]) + "\t"+ str(worker_data['slots'][worker_port_no])+'\t'+ str(time.time())+"\n")
			worker_log.close()	
			slot_update_lock.release()
			break
		else:
			slot_update_lock.release()
	send_worker_thread = threading.Thread(target = send_task_workers, args = (worker_data_port, task_id, task_dur))
	send_worker_thread.start()
	update_slots(task_id, worker_port_no)

#Function to select scheduler based on the system argument.
def scheduler_selection(job_data, sch_algo):
	#print("in sch_sel")
	for task in job_data['map_tasks']:
		filename = "task_log_" + sch_algo + ".txt"
		task_file = open(filename, "a")
		task_file.write(task['task_id'])
		task_file.write('\t')
		task_file.write(str(time.time()))
		task_file.write("\n")
		print("task ", task['task_id']," started")
		if sch_algo == "RANDOM":		
			rand_thread = threading.Thread(target = random_sch, args = (task['task_id'], task['duration']))
			rand_thread.start()
			rand_thread.join()
		elif sch_algo =="RR":
			round_r_thread = threading.Thread(target = round_robin, args = (task['task_id'], task['duration']))
			round_r_thread.start()
			round_r_thread.join()
		elif sch_algo =="LL":
			ll_thread = threading.Thread(target = least_loaded, args = (task['task_id'], task['duration']))
			ll_thread.start()
			ll_thread.join()

	for task in job_data['reduce_tasks']:
		filename = "task_log_" + sch_algo + ".txt"
		task_file = open(filename, "a")
		task_file.write(task['task_id'])
		task_file.write('\t')
		task_file.write(str(time.time()))
		task_file.write("\n")
		print("task ", task['task_id']," started")
		if sch_algo =="RANDOM":
			rand_thread = threading.Thread(target = random_sch, args = (task['task_id'], task['duration']))
			rand_thread.start()
			rand_thread.join()
		elif sch_algo =="RR":
			round_r_thread = threading.Thread(target = round_robin, args = (task['task_id'], task['duration']))
			round_r_thread.start()
			round_r_thread.join()
		elif sch_algo =="LL":
			ll_thread = threading.Thread(target = least_loaded, args = (task['task_id'], task['duration']))
			ll_thread.start()
			ll_thread.join()
	filename = "job_log_" + sch_algo + ".txt"
	job_file = open(filename, "a")
	job_file.write(job_data['job_id'])
	job_file.write('\t')
	job_file.write(str(time.time()))
	job_file.write("\n")
	job_file.close()
	print("Job", job_data['job_id'], "ended ")
	print("Job end time :",time.time())


#Function to receive requests from the random job creator.
def rcv_requests():
	master_request_sckt = socket(AF_INET, SOCK_STREAM)
	master_request_sckt.bind((request_address, master_job_port))
	master_request_sckt.listen(1)
	print("Listening on port :", master_job_port)
	while True:
		request_conn, request_addr = master_request_sckt.accept()
		print('receiving from', request_addr)	
		data = request_conn.recv(1024)
		data = data.decode()
		job_data = eval(data)
		request_conn.close()
		job_thread = threading.Thread(target = scheduler_selection, args = (job_data, scheduling_algo))
		job_thread.start()
		filename = "job_log_" + scheduling_algo + ".txt"
		job_file = open(filename, "a")
		job_file.write(job_data['job_id'])
		job_file.write('\t')
		job_file.write(str(time.time()))
		job_file.write("\n")
		job_file.close()
		print("Job id :", job_data['job_id'])
		print("Job start time :", time.time())

# Thread for receiving requests.
rcv_requests_thread = threading.Thread(target = rcv_requests)
rcv_requests_thread.start()

#Thread for getting acks from workers.
ack_thread = threading.Thread(target = rcv_ack_workers)
ack_thread.start()

# Closing the files.
# job_file.close()
# task_file.close()