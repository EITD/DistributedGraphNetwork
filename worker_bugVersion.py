import random
import socket
from multiprocessing import Process
import threading
from ConvertFile import ConvertFile, nx
import json
from MySocket import MySocket
import sys

# NUM_PARTITIONS_START = 0
# NUM_PARTITIONS_END = 4
# PARTITION_LIST = [i for i in range(NUM_PARTITIONS_START, NUM_PARTITIONS_END)]
NODE_FEATURES = "./data/node_features.txt"

class Worker:
    worker_id = None
    s = None
    node_data = {}
    graph = {}
    # acc = 0

    def __init__(self, wid, p):
        self.worker_id = int(wid)
        self.s = MySocket(myNode=wid, port=p, NUM_PARTITIONS=4)

    def load_node_data(self):
        with open(NODE_FEATURES, 'r') as file:
            lines = file.readlines()
        for line in lines:
            parts = line.strip().split()
            if int(parts[0]) % 4 == self.worker_id and len(parts) == 2:
                self.node_data[parts[0]] = parts[1]

    def load_graph_dict(self):
        self.graph = ConvertFile.toGraph(f"./data/partition_{self.worker_id}.txt", " ")
        # self.graph = ConvertFile.toGraph(f"./data/test_{self.worker_id}.txt", " ")
        
    def node_feature(self, nid, event, data_container):
        data_container.append(self.node_data[nid])
        # data_container.append(nid)
        event.set()
    
    def khop_neighborhood(self, nid, k, deltas, event, data_container):
        kNew = k - 1
        
        node_neighbors_list = list(self.graph.neighbors(nid))
        
        random_neighbors = random.sample(node_neighbors_list, deltas[0] if len(node_neighbors_list) > deltas[0] else len(node_neighbors_list))
        
        for i in range(len(random_neighbors)):
            request_data = {
                'khop_neighborhood': {
                    'nid': random_neighbors[i],
                    'k': kNew,
                    'deltas': deltas[1:]
                }
            }
            request_json = json.dumps(request_data)
            self.s.ask(threading.current_thread().name + random_neighbors[i], random_neighbors[i], request_json)
        
        node_features_list = []
        khop_neighborhood_list = []
        
        okDict = {i:False for i in random_neighbors}
        
        while not all(value for value in okDict.values()):
            for i in range(len(random_neighbors)):
                if threading.current_thread().name + random_neighbors[i] in self.s.ask_reply_dict:
                    request_data = json.loads(self.s.ask_reply_dict[threading.current_thread().name + random_neighbors[i]])
                    
                    node_features_list.append(request_data['node_feature'][0]) # [int] k = 2
                    khop_neighborhood_list.append(request_data['khop_neighborhood']) # [[int,int...], [int,int...]]
                    okDict[random_neighbors[i]] = True
                    random_neighbors.remove(random_neighbors[i])
                    break
        
        data_container.append(node_features_list)
        
        for i in range(len(deltas[1:])):
            temp = []
            for lst in khop_neighborhood_list:
                temp += lst[i]
            random.shuffle(temp)
            temp = temp[:deltas[i + 1]]
            data_container.append(temp.copy())
        print(data_container)
        event.set()

    def handle_msg(self, client_socket, message):
        request_data = json.loads(message)
        
        node_feature_data_container = []
        khop_neighborhood_data_container = []
        
        if 'node_feature' in request_data:
            nid = request_data['node_feature']
            k = 0
        elif 'khop_neighborhood' in request_data:
            nid = request_data['khop_neighborhood']['nid']
            k = request_data['khop_neighborhood']['k']
            deltas = request_data['khop_neighborhood']['deltas']
        
        if nid not in self.node_data:
            self.s.ask(threading.current_thread().name + nid, node=nid, msg=message)
            
            while True:
                if threading.current_thread().name + nid in self.s.ask_reply_dict:
                    request_json = self.s.ask_reply_dict[threading.current_thread().name + nid]
                    break
            
        else:
            node_feature_event = threading.Event()
            khop_neighborhood_event = threading.Event()
                
            node_feature_thread = threading.Thread(target=self.node_feature, args=(nid, node_feature_event, node_feature_data_container))
            node_feature_thread.start()
            if k > 0:
                khop_neighborhood_thread = threading.Thread(target=self.khop_neighborhood, args=(nid, k, deltas, khop_neighborhood_event, khop_neighborhood_data_container))
                khop_neighborhood_thread.start()
            
            node_feature_event.wait()
            if k > 0:
                khop_neighborhood_event.wait()

            request_data = {
                    'node_feature' : node_feature_data_container, #[int]
                    'khop_neighborhood': khop_neighborhood_data_container  # [[int,int...]]
                }
        
            request_json = json.dumps(request_data)
        
        self.s.message_send_queue.put((client_socket, request_json))

def start_worker(wid, port):
    worker = Worker(wid, port)
    worker.load_node_data()
    worker.load_graph_dict()
    while True:
        # print("???")
        # print(list(worker.s.message_get_queue))
        if not worker.s.message_get_queue.empty():
            client_socket, message = worker.s.message_get_queue.get()
            handle_thread = threading.Thread(target=worker.handle_msg, args=(client_socket, message))
            handle_thread.start()

if __name__ == "__main__":
        w = Process(target=start_worker, args=(sys.argv[1], 12345+int(sys.argv[1])))
        w.start()
