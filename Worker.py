import json
from socket import *
import time
import sys
import random
import numpy as np

worker_port = sys.args[0]
master_worker_update_port = 5001
worker_id = sys.arg[1]
master_ip_addr = 'localhost'

sckt = socket(AF_INET, SOCK_STREAM)

sckt.bind((master_ip_addr, worker_port))
