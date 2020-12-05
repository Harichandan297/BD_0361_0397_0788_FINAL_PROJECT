import json
from socket import *
import time
import sys
import random
import threading

worker_port = sys.argv[1]
worker_port = int(worker_port)
master_worker_update_port = 5001
worker_id = sys.argv[2]
worker_id = int(worker_id)
worker_addr = 'localhost'
master_addr = 'localhost' 

def worker_job(worker_conn):
	message= worker_conn.recv(1024)
	data = message.decode()
	task = eval(data)
	for task_id, dur in task.items():
		print("Task", task_id, "received by worker", worker_id)
		time.sleep(dur)		
		send_ack_master(task_id)
		worker_conn.close()

def worker():
	sckt = socket(AF_INET, SOCK_STREAM)
	sckt.bind((master_addr, worker_port))
	sckt.listen(1)
	print("Listening on port ", worker_port)
	while 1:
		worker_conn, worker_addr = sckt.accept()
		worker_thread = threading.Thread(target = worker_job, args = (worker_conn, ))
		worker_thread.start()

def send_ack_master(compl_task_id):
	worker_update_sckt = socket(AF_INET, SOCK_STREAM) 
	worker_update_sckt.connect((master_addr, master_worker_update_port))
	message = compl_task_id.encode()
	worker_update_sckt.send(message)
	print("ack sent for task :", compl_task_id)

task_thread1= threading.Thread(target = worker)
task_thread1.start()
