import json
import socket
import struct
import threading
import concurrent.futures

from ConvertFile import ConvertFile
from worker_asy import Vertex, ask

NUM_PARTITIONS = 4
NODE_FEATURES = "./data/node_features.txt"
host = 'localhost'
NODE_DEFAULT_FEATURE = 0
serverDict = [host, host, host, host]


class Worker:
    worker_id = None
    # vertexDict = {}
    initial_vertex = []
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    executor = concurrent.futures.ThreadPoolExecutor()
    
    def __init__(self, wid):
        self.worker_id = int(wid)
        
        graph = ConvertFile.toGraph(f"./data/neighbor.txt", " ")
        
        with open(NODE_FEATURES, 'r') as file:
            lines = file.readlines()
        for line in lines:
            parts = line.strip().split()[:2]
            if int(parts[0]) % NUM_PARTITIONS == self.worker_id:
                out_edges = graph.successors(parts[0])
                in_edges = graph.predecessors(parts[0])
                # self.vertexDict[12345 + int(parts[0])] = Vertex(parts[0], int(parts[1]), in_edges, out_edges)
                if list(out_edges) == 0:
                    self.initial_vertex.append(parts[0])
                Vertex(parts[0], int(parts[1]), in_edges, out_edges)
        
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        self.server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.server_socket.bind((host, 10000 + self.worker_id))
        self.server_socket.listen(5000)
        
        self.executor.submit(self.handle_client)

    def handle_client(self):
        while True:
            client_socket, _ = self.server_socket.accept()
            self.executor(self.handle_client_connection, client_socket)
    
    def handle_client_connection(self, client_socket):
        data = client_socket.recv(20480)
        # print('get msg:', data)
        request_data = json.loads(data)
        final_epoch = request_data['neighborhood_aggregation_async']['epochs']
        k = request_data['neighborhood_aggregation_async']['k']
        deltas = request_data['neighborhood_aggregation_async']['deltas']

        for e in range(final_epoch):
            self.send_snapshot_to_initial_vertex(k, deltas)

        client_socket.shutdown(socket.SHUT_RDWR)
        client_socket.close()
    
    def send_snapshot_to_initial_vertex(self, k, deltas):
        initial_vertex_ask_list = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for vertex in self.initial_vertex:
                future = executor.submit(ask, vertex, f"snapshot {k} {deltas}")
                initial_vertex_ask_list.append(future)
        concurrent.futures.wait(initial_vertex_ask_list)