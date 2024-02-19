import socket
from multiprocessing import Process
from ConvertFile import ConvertFile, nx
import json

NUM_PARTITIONS_START = 0
NUM_PARTITIONS_END = 4
PARTITION_LIST = [i for i in range(NUM_PARTITIONS_START, NUM_PARTITIONS_END)]
NODE_FEATURES = "./data/node_features.txt"

class Worker:
    worker_id = None
    port = None
    node_data = {}
    graph = {}

    def __init__(self, wid, port):
        self.worker_id = wid
        self.port = port

    def load_node_data(self):
        with open(NODE_FEATURES, 'r') as file:
            lines = file.readlines()
        for line in lines:
            parts = line.strip().split()
            if int(parts[0]) % 4 == self.worker_id and len(parts) == 2:
                self.node_data[parts[0]] = parts[1]

    def load_graph_dict(self):
        self.graph = ConvertFile.toGraph(f"./data/partition_{self.worker_id}.txt", " ")

    def khop_neighborhood(self, nid, k, deltas):
        if k == 0 or not deltas:
            return self.node_data(nid)

        current_layer_nodes = {nid}
        features_sum = 0
        
        for deltas_index in range(0, k):
            total_nodes_needed = deltas[deltas_index]
            next_layer_nodes = set()
            
            for node in current_layer_nodes:
                neighbors = list(self.graph.neighbors(node))
                for neighbor in neighbors:
                    next_layer_nodes.add(neighbor)
                    if len(next_layer_nodes) == total_nodes_needed:
                        break  
                if len(next_layer_nodes) == total_nodes_needed:
                    break
            
            if len(next_layer_nodes) < total_nodes_needed:
                print("not enough")

            for node in current_layer_nodes:
                features_sum += int(self.node_data[node])
            # print(current_layer_nodes)
        
            current_layer_nodes = next_layer_nodes.copy()
        
        for node in current_layer_nodes:
            features_sum += int(self.node_data[node])
        # print(current_layer_nodes)

        return features_sum
        

    def start_worker(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', self.port))
            s.listen()
            print(f"Worker {self.worker_id} listening on port {self.port}")
            while True:
                conn, addr = s.accept()
                with conn:
                    data = conn.recv(1024).decode()
                    request_data = json.loads(data)
                    nid = request_data['nid']
                    k = request_data['k']
                    deltas = request_data['deltas']
                    result = self.khop_neighborhood(nid, k, deltas)
                    conn.sendall(str(result).encode())
    

def start_worker(wid, port):
    worker = Worker(wid, port)
    worker.load_node_data()
    worker.load_graph_dict()
    worker.start_worker()


if __name__ == "__main__":
    # workersDict = {}
    # graphDict = {}

    for i in PARTITION_LIST:
        w = Process(target=start_worker, args=(i, 10000+i))
        # g = ConvertFile.toGraph(f"./data/partition_{i}.txt", " ")
        # workersDict[i] = w
        # graphDict[i] = g
        w.start()
        # w.join()
