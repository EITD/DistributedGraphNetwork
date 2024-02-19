import socket
from multiprocessing import Process
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

    def start_listen(self):
        client_socket, message = self.s.message_get_queue.get()
        request_data = json.loads(message)
        if 'node_feature' in request_data:
            nid = request_data['node_feature']
            if nid in self.node_data:
                self.s.message_send_queue.put((client_socket, self.node_data[nid]))
            else:
                self.s.ask(0, node=int(nid), msg=message)

    
    # def khop_neighborhood(self, nid, k, deltas, num):
    #     if nid in self.node_data:
    #         features_sum = self.node_data[nid]
    #         if k == 0 or not deltas:
    #             return features_sum
            
    #         count = 0
    #         for neighbor in list(self.graph.neighbors(nid)):
    #             if len(deltas) > 1:

    #             print("send to others")
    #             # features_sum += khop_neighborhood(neighbor, k - 1, deltas[1:])
    #             # return -1 continue 
    #             count += 1
    #             if count == deltas[0]:
    #                 break
            
    #         if count < deltas[0]:
    #             return -1

    #         return features_sum
        
    #     else:
    #         print("send to others")
        

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
    while(True):
        worker.start_listen()

if __name__ == "__main__":
        w = Process(target=start_worker, args=(sys.argv[1], 12345+int(sys.argv[1])))
        w.start()
