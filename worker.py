import socket
from multiprocessing import Process
from ConvertFile import ConvertFile, nx
from matplotlib import pyplot as plt

NUM_PARTITIONS_START = 0
NUM_PARTITIONS_END = 4
PARTITION_LIST = [i for i in range(NUM_PARTITIONS_START, NUM_PARTITIONS_END)]
NODE_FEATURES = "./data/node_features.txt"

def load_node_data(worker_id):
    node_data = {}
    with open(NODE_FEATURES, 'r') as file:
        lines = file.readlines()
    for line in lines:
        parts = line.strip().split()
        if int(parts[0]) % 4 == worker_id and len(parts) == 2:
            node_data[parts[0]] = parts[1]
    return node_data

def worker(worker_id, port, node_data):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', port))
        s.listen()
        print(f"Worker {worker_id} listening on port {port}")
        while True:
            conn, addr = s.accept()
            with conn:
                nid = conn.recv(1024).decode()
                feature = node_data.get(nid, "Node not found")
                conn.sendall(str(feature).encode())

def start_worker(worker_id, port):
    worker(worker_id, port, load_node_data(worker_id))

if __name__ == "__main__":
    workersDict = {}
    graphDict = {}

    for i in PARTITION_LIST:
        w = Process(target=start_worker, args=(i, 10000+i))
        g = ConvertFile.toGraph(f"./data/partition_{i}.txt", " ")
        workersDict[i] = w
        graphDict[i] = g
        w.start()
    # for w in workers:
    #     w.join()
