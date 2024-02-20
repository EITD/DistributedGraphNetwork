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

    def handle_msg(self, client_socket, message):
        request_data = json.loads(message)
        if nid in self.node_data:
            if 'node_feature' in request_data:
                nid = request_data['node_feature']
                self.s.message_send_queue.put((client_socket, self.node_data[nid]))
            if 'khop_neighborhood' in request_data:
                nid = request_data['khop_neighborhood']['nid']
                k = request_data['khop_neighborhood']['k']
                deltas = request_data['khop_neighborhood']['deltas']
                sums = self.khop_neighborhood(nid, k, deltas)
                self.s.message_send_queue.put((client_socket, self.node_data[nid] + sums))
            if 'khop_ask_phase' in request_data:
                nid = request_data['khop_ask_phase']['nid']
                k = request_data['khop_ask_phase']['k']
                deltas = request_data['khop_ask_phase']['deltas']
                if 
        else:
            self.s.ask(threading.current_thread().name + nid, node=nid, msg=message)
            # self.acc+=1

    
    def khop_neighborhood(self, nid, k, deltas):
        kNew = k - 1

        newDeltas = [i // deltas[0] for i in deltas[1:]]
        newDeltasList = [newDeltas.copy() for _ in range(deltas[0])]
        
        for i in range(len(newDeltas)):
            s = sum(newDeltasList[j][i] for j in range(len(newDeltasList)))
            
            if s == deltas[i + 1]:
                continue
            
            remaining = deltas[i + 1] - s
            for _ in range(remaining):
                idx = random.randint(0, len(newDeltasList) - 1)
                newDeltasList[idx][i] += 1
        
        # print(newDeltasList)
        
        sums = 0
        
        random_neighbors = random.sample(list(self.graph.neighbors(nid)), len(newDeltasList))
        
        for i in range(len(random_neighbors)):
            request_data = {
                'khop_ask_phase': {
                    'nid': random_neighbors[i],
                    'k': kNew,
                    'deltas': newDeltasList[i]
                }
            }
            request_json = json.dumps(request_data)
            self.s.ask(threading.current_thread().name + random_neighbors[i], random_neighbors[i], request_json)
            
            start_to_sum = False
            okDict = {i: False for i in random_neighbors}
            
            while not start_to_sum:
                for i in range(len(random_neighbors)):
                    if threading.current_thread().name + random_neighbors[i] in self.s.ask_reply_dict:
                        
        #     if len(deltas) > 1:

        #         print("send to others")
        #     # features_sum += khop_neighborhood(neighbor, k - 1, deltas[1:])
        #     # return -1 continue 
        #     count += 1
        #     if count == deltas[0]:
        #         break
        
        # if count < deltas[0]:
        #     return -1

        return sums

    # def start_worker(self):
    #     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    #         s.bind(('localhost', self.port))
    #         s.listen()
    #         print(f"Worker {self.worker_id} listening on port {self.port}")
    #         while True:
    #             conn, addr = s.accept()
    #             with conn:
    #                 data = conn.recv(1024).decode()
    #                 request_data = json.loads(data)
    #                 nid = request_data['nid']
    #                 k = request_data['k']
    #                 deltas = request_data['deltas']
    #                 result = self.khop_neighborhood(nid, k, deltas)
    #                 conn.sendall(str(result).encode())
    

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
