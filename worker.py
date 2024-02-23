import random
import socket
from multiprocessing import Process
import threading
from ConvertFile import ConvertFile, nx
import json
from MySocket import MySocket
import sys

NUM_PARTITIONS = 4
# NUM_PARTITIONS_START = 0
# NUM_PARTITIONS_END = 4
# PARTITION_LIST = [i for i in range(NUM_PARTITIONS_START, NUM_PARTITIONS_END)]
NODE_FEATURES = "./data/node_features.txt"

class NodeForOtherWorker(Exception):
    def __init__(self):
        pass
class Worker:
    worker_id = None
    s = None
    node_data = {}
    graph = {}
    # acc = 0

    def __init__(self, wid, p):
        self.worker_id = int(wid)
        self.s = MySocket(myNode=wid, port=p, NUM_PARTITIONS=NUM_PARTITIONS)

    def load_node_data(self):
        with open(NODE_FEATURES, 'r') as file:
            lines = file.readlines()
        for line in lines:
            parts = line.strip().split()
            if int(parts[0]) % 4 == self.worker_id and len(parts) == 2:
                self.node_data[parts[0]] = [int(parts[1])]

    def load_graph_dict(self):
        self.graph = ConvertFile.toGraph(f"./data/partition_{self.worker_id}.txt", " ")
        # self.graph = ConvertFile.toGraph(f"./data/test_{self.worker_id}.txt", " ")
        
    def node_feature(self, nid):
        return self.node_data.get(nid, [0])[0]
        
    def feature_and_neighborhood(self, nid, delta):
        node_neighbors_list = list(self.graph.neighbors(nid))
        random_neighbors = random.sample(node_neighbors_list, delta if len(node_neighbors_list) > delta else len(node_neighbors_list))
        
        return self.node_feature(nid), random_neighbors
    
    def khop_neighborhood(self, nid, k, deltas):
        sums = self.node_feature(nid)
        node_neighbors_set = set(self.graph.neighbors(nid))
        
        for j in range(k): # [2,3,2]
            random_neighbors = random.sample(list(node_neighbors_set), deltas[j] if len(node_neighbors_set) > deltas[j] else len(node_neighbors_set))
            
            for node in random_neighbors:
                if j < k - 1:
                    request_data = {
                        'feature_and_neighborhood' : {
                            'nid' : node,
                            'delta' : deltas[j + 1]
                        }
                    }
                else:
                    request_data = {
                        'node_feature' : node
                    }
                request_json = json.dumps(request_data)
                self.s.ask(threading.current_thread().name + node, node, request_json)
                
            okDict = {node:False for node in random_neighbors}
            node_neighbors_set = set()
            
            while not all(value for value in okDict.values()):
                for node in random_neighbors:
                    if threading.current_thread().name + node in self.s.ask_reply_dict:
                        request_data = json.loads(self.s.ask_reply_dict[threading.current_thread().name + node])
                        
                        if j < k - 1:
                            node_neighbors_set.update(request_data['neighborhood'])
                        sums += request_data['node_feature']
                        
                        okDict[node] = True
                        random_neighbors.remove(node)
                        break
        
        return sums
    
    # def aggregate_neighborhood(self, k, deltas, epochs):


    def handle_msg(self, client_socket, message):
        request_data = json.loads(message)
        
        try:
            if 'node_feature' in request_data:
                nid = request_data['node_feature']
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                request_data = {
                    'node_feature' : self.node_feature(nid), # feature
                }
                
            elif 'khop_neighborhood' in request_data:
                nid = request_data['khop_neighborhood']['nid']
                k = request_data['khop_neighborhood']['k']
                deltas = request_data['khop_neighborhood']['deltas']
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                request_data = {
                    'node_feature' : self.khop_neighborhood(nid, k, deltas), # feature
                }
                
            elif 'feature_and_neighborhood' in request_data:
                nid = request_data['feature_and_neighborhood']['nid']
                delta = request_data['feature_and_neighborhood']['delta']
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                feature, neighborhoodSet = self.feature_and_neighborhood(nid, delta)
                request_data = {
                    'node_feature' : feature, # feature
                    'neighborhood' : neighborhoodSet # [nid, nid, nid...]
                }
            
            elif 'neighborhood_aggregation' in request_data:
                nid = request_data['neighborhood_aggregation']['nid']
                k = request_data['neighborhood_aggregation']['k']
                deltas = request_data['neighborhood_aggregation']['deltas']
                epochs = request_data['neighborhood_aggregation']['epochs']

                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                # send back to client
            
            request_json = json.dumps(request_data)
        except NodeForOtherWorker:
            self.s.ask(threading.current_thread().name + nid, node=nid, msg=message)
        
            while True:
                if threading.current_thread().name + nid in self.s.ask_reply_dict:
                    request_json = self.s.ask_reply_dict[threading.current_thread().name + nid]
                    break
        
        self.s.message_send_queue.put((client_socket, request_json))


def start_worker(wid, port):
    worker = Worker(wid, port)
    worker.load_node_data()
    worker.load_graph_dict()
    while True:
        if not worker.s.message_get_queue.empty():
            client_socket, message = worker.s.message_get_queue.get()
            handle_thread = threading.Thread(target=worker.handle_msg, args=(client_socket, message))
            handle_thread.start()

if __name__ == "__main__":
        w = Process(target=start_worker, args=(sys.argv[1], 12345+int(sys.argv[1])))
        w.start()
