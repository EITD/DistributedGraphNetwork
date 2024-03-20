import queue
import random
import socket
import struct
from time import sleep
import traceback
from ConvertFile import ConvertFile
import json
import sys
import concurrent.futures
import platform
import copy

system = platform.system()

NUM_PARTITIONS = 4
K = 2
# DELTAS = [20, 400, 160000]
DELTAS = [5000, 5000**2]
NODE_FEATURES = "./data_small/node_feature_small.txt"
# NODE_FEATURES = "./data/node_features.txt"
host = 'localhost'
NODE_DEFAULT_FEATURE = 0
serverDict = [host, host, host, host]

# class Marker:
#     def __init__(self):
#         pass

class Worker:
    worker_id = None
    vertexDict = {}
    vertex_number = 0
    initial_vertex = []
    target_epoch = ""
    
    def __init__(self, wid):
        self.worker_id = int(wid)
        
        graph = ConvertFile.toGraph(f"./data_small/neighbor_small.txt", " ")
        # graph = ConvertFile.toGraph(f"./data/neighbor.txt", " ")
        
        with open(NODE_FEATURES, 'r') as file:
            lines = file.readlines()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1500)
        for line in lines:
            parts = line.strip().split()[:2]
            if int(parts[0]) % NUM_PARTITIONS == self.worker_id:
                self.vertex_number += 1
                out_edges = graph.successors(parts[0])
                in_edges = graph.predecessors(parts[0])
                # self.vertexDict[12345 + int(parts[0])] = Vertex(parts[0], int(parts[1]), in_edges, out_edges)
                executor.submit(Vertex, parts[0], int(parts[1]), list(in_edges), list(out_edges))
        
        sources = [n for n, d in graph.out_degree() if d == 0]
        for vertex in sources:
            if int(vertex) % NUM_PARTITIONS == self.worker_id:
                self.vertex_number += 1
                self.initial_vertex.append(vertex)
                in_edges = graph.predecessors(vertex)
                executor.submit(Vertex, vertex, 0, list(in_edges), [])
        
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        server_socket.bind((host, 10000 + self.worker_id))
        server_socket.listen(1500)
        
        # executor = concurrent.futures.ThreadPoolExecutor()
        # self.handle_client(server_socket)
        print('worker', self.worker_id , 'ready!')
        with concurrent.futures.ThreadPoolExecutor() as e:
            while True:
                client_socket, _ = server_socket.accept()
                e.submit(self.handle_client_connection, client_socket)

    # def handle_client(self, server_socket):
    #     print("handle_client")
    #     with concurrent.futures.ThreadPoolExecutor() as executor:
    #         while True:
    #             client_socket, _ = server_socket.accept()       
    #             executor.submit(self.handle_client_connection, client_socket)
    
    def handle_client_connection(self, client_socket):
        data = client_socket.recv(102400)
        message = data.decode()
        print('worker', self.worker_id, ':', 'get msg:', data)
        if "epoch_" in message:
            self.target_epoch = message.split("_")[1]
            for e in range(int(self.target_epoch) + 1):
                self.send_snapshot_to_initial_vertex(e)
            while True:
                if len(self.vertexDict.keys()) == self.vertex_number:
                    key = f"vertex_{self.worker_id}"
                    send_data = json.dumps({key: self.vertexDict})
                    client_socket.send(send_data.encode())
                    break

        elif "record_" in message:
            parts = message.split("_")
            nid = parts[1]
            feature = parts[2]
            epoch = parts[3]
            if epoch == self.target_epoch:
                self.vertexDict.update({nid: feature})
        if system == 'Darwin':
            client_socket.shutdown(socket.SHUT_WR)
        client_socket.close()
    
    def send_snapshot_to_initial_vertex(self, epoch):
        initial_vertex_notify_list = []
        # print(self.initial_vertex)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for vertex in self.initial_vertex:
                future = executor.submit(notify, vertex, f"snapshot_{epoch}")
                initial_vertex_notify_list.append(future)
        concurrent.futures.wait(initial_vertex_notify_list)

class Vertex:
    def __init__(self, node, feature, in_edges, out_edges):
        self.id = node
        self.port = 12345 + int(node)
        self.sp = [feature] 
        # self.sp_snapshot = None # sp*

        # in_edges is Op in chandy lamport algorithm.
        self.in_edges_list = in_edges
        # out_edges is Ip in chandy lamport algorithm.
        self.out_edges_list = out_edges
        # self.Enabled = copy.deepcopy(self.out_edges_list) # [all out_edges]
        self.Enabled = self.out_edges_list.copy() # [all out_edges]

        # self.epoch_dict = {}
        # n_f.append(inbox.pop) until '__MARKER__0'
        self.neighbor_features = [[] for i in range(K)] # [['v0f23', ...], ['v0v10f13', ...], ['v0v10v20f33', ...], ...] len(n_f)==k (features we use to khop epoch 1)
        self.inbox = {out:[] for out in self.out_edges_list} # ['__MARKER__e0v0', ..., 'v0v10v20fxxx', 'v0v10fxxxx', 'v0fxxxxx', '__MARKER__e0v8', ..., '__MARKER__e1v0', ..., ...]
        # self.Mp = [] # [m, m, m, ...] this is part of inbox
        self.message_queue = queue.Queue()
        
        # print(self.id)
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        server_socket.bind((host, self.port))
        server_socket.listen(5000)

        # print("start: ", self.id)

        exec = concurrent.futures.ThreadPoolExecutor()
        # fList = []
        for c in self.out_edges_list:
            exec.submit(self.handle_msg, c, self.inbox[c])
            # fList.append(future)
        # print(self.id)
        exec.submit(self.toInbox)
        while True:
            client_socket, _ = server_socket.accept()
            try:
                data = client_socket.recv(102400)
                self.message_queue.put(data)
                # print('vertex', self.id, ':', 'get msg:', data)
                # self.toInbox(data.decode())
                # client_socket.send(rep.encode())
                # print('vertex', self.id, ':', 'reply msg:', rep)
            finally:
                # if system == 'Darwin':
                #     client_socket.shutdown(socket.SHUT_WR)
                client_socket.close()
        # concurrent.futures.wait(fList)
    
    def toInbox(self):
        # print(message)
        while True:
            # print(self.id, self.inbox)
            message = self.message_queue.get().decode()
            print('vertex', self.id, ':', 'get msg:', message)
            if "snapshot_" in message:
                parts = message.split("_")
                epoch = parts[1]
                
                self.record(epoch, self.get(self.epoch()))

                # pass feature and then marker
                # for e in range(epoch):
                initial_vertex_feature_list = []
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for out in self.in_edges_list:
                        future = executor.submit(notify, out, f"v{self.id}f{self.get(self.epoch())}")
                        initial_vertex_feature_list.append(future)
                concurrent.futures.wait(initial_vertex_feature_list)

                print(self.id, "send all features")

                initial_vertex_marker_list = []
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for out in self.in_edges_list:
                        future = executor.submit(notify, out, f"marker_{epoch}_{self.id}")
                        initial_vertex_marker_list.append(future)
                concurrent.futures.wait(initial_vertex_marker_list)

                print(self.id, "send all markers")
                continue

                # return 'ok'
            elif "marker_" in message:
                _, _, c = message.split("_")
            else:
                c = message.split('f')[0].split('v')[1]
            self.inbox[c].append(message)
            
            # print(self.id, self.message_queue.empty)
            # return 'ok'
    
    def epoch(self):
        return len(self.sp) - 1
    
    def get(self, epoch):
        try:
            return self.sp[epoch]
        except IndexError:
            return None
    
    def khop_neighborhood(self):
        try:
            sums = self.get(self.epoch())
            
            node_neighbors_set = set(self.out_edges_list)
            # node_neighbors_set = set(['v' + i for i in out_edges_list])
            
            for j in range(K): 
                random_neighbors = random.sample(list(node_neighbors_set), DELTAS[j] if len(node_neighbors_set) > DELTAS[j] else len(node_neighbors_set))
                node_neighbors_set = set()
                temp_set = set()

                # print(random_neighbors)
                # for feature in self.neighbor_features[j]:
                #         if feature[0:feature.rfind('f')] in random_neighbors:
                #                 sums += int(feature[feature.rfind('f') + 1:])
                
                # if j < k - 1:
                #         for feature in self.neighbor_features[j+1]:
                #                 if feature[0:feature.rfind('v')] in random_neighbors:
                #                         node_neighbors_set.add(feature[0:feature.rfind('f')])
                for vertex in random_neighbors:
                    for feature in self.neighbor_features[j]:
                        if feature.startswith("v" + vertex):
                            start_index = feature.find("f")
                            sub_text = feature[start_index + 1:] 
                            sums += int(sub_text)
                    if j < K - 1:
                        for v in self.neighbor_features[j + 1]:
                            if v.startswith("v" + vertex):
                                # start_index = len("v" + vertex)
                                end_index = v.find("f")
                                sub_text = v[1:end_index]
                                temp = v[v.rfind("v") + 1 : end_index]
                                if temp not in temp_set:
                                    node_neighbors_set.add(sub_text) # [2v8 7v8]
                                    temp_set.add(temp)
                
                # featrueList = [self.epoch_dict.get(vertex, None) for vertex in random_neighbors]
                
                # while None in featrueList:
                #     sleep(3)
                #     featrueList = [self.epoch_dict.get(vertex, None) for vertex in random_neighbors]
                
                # for featrue in featrueList:
                #     sums += featrue
                
                # for ask_future in neighborhood_ask_list:
                #     msg = ask_future.result()
                #     data = json.loads(msg)
                #     if j < k - 1:
                #         node_neighbors_set.update(data['out_edges'])
        except Exception as e:
            with open('khop_neighborhood', 'a') as f:
                f.write(str(e) + '\n' + str(traceback.format_exc()) + '\n\n\n\n\n')
        return sums
    
    # def startRecording(self):
        # self.sp_snapshot = self.get(self.epoch())
        # for out in self.out_edges_list:
        #     send marker
        # pass

    def handle_msg(self, c, messageList):
        while True:
            try:
                # print(self.id, messageList)
                message = messageList[0]
            except IndexError:
                sleep(3)
                continue
            
            if "marker_" in message:
                parts = message.split("_")
                epoch = parts[1]

                if c in self.Enabled:
                    self.record(epoch, self.get(self.epoch()))
                    self.Enabled.remove(c)

                    if len(self.Enabled) == 0:
                        # send self feature
                        vertex_feature_list = []
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            for out in self.in_edges_list:
                                future = executor.submit(notify, out, f"v{self.id}f{self.get(self.epoch())}")
                                vertex_feature_list.append(future)
                        concurrent.futures.wait(vertex_feature_list)

                        print(self.id, "send all features")

                        self.sp.append(self.khop_neighborhood())
                        self.neighbor_features = [[] for i in range(K)]

                        vertex_marker_list = []
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            for out in self.in_edges_list:
                                future = executor.submit(notify, out, f"marker_{epoch}_{self.id}")
                                vertex_marker_list.append(future)
                        concurrent.futures.wait(vertex_marker_list)

                        print(self.id, "send all markers")

                        # self.Enabled = copy.deepcopy(self.out_edges_list)
                        self.Enabled = self.out_edges_list.copy()
                        # print(self.Enabled)
                # messages are before marker, marker can't be in Disabled
                else:
                    continue

            else:
                if c in self.Enabled:
                    index = message.count('v')
                    if index <= K:
                        self.neighbor_features[index - 1].append(message)
                    if index < K:
                        send_feature = f"v{self.id}" + message
                        neighbor_feature_list = []
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            for out in self.in_edges_list:
                                future = executor.submit(notify, out, send_feature)
                                neighbor_feature_list.append(future)
                        concurrent.futures.wait(neighbor_feature_list)

                else:
                    continue
            
            messageList.pop(0)

            # return 'ok'

                # if cqp not in self.Recorded:
                #     pass
                
                # elif cqp in self.Recorded:
                #     pass
    def record(self, epoch, sp_snaposhot):
        message = f"record_{self.id}_{sp_snaposhot}_{epoch}"
        notify(str(int(self.id) % NUM_PARTITIONS), message, True)


def notify(node, msg, worker=False):
    print('notify:', msg)
    while True:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            r = random.randint(0,2)
            if r == 1:
                raise ConnectionRefusedError()
            elif r == 2:
                raise OSError()
            
            if worker:
                client_socket.connect((serverDict[int(node) % NUM_PARTITIONS], 10000 + int(node)))
            else:
                client_socket.connect((serverDict[int(node) % NUM_PARTITIONS], 12345 + int(node)))
            
            # print("connect: ", node)
            client_socket.send(msg.encode())
            
            # data = client_socket.recv(102400).decode()
            
            # print('get reply:', data)

            client_socket.shutdown(socket.SHUT_WR)
            client_socket.close()
            break
        except ConnectionRefusedError:
            print('notify connection error')
            client_socket.close()
            # sleep(1)
            continue
        except OSError:
            print('notify os error')
            client_socket.close()
            # sleep(1)
            continue
        # except Exception as e:
        #     with open('ask', 'a') as f:
        #         f.write(str(msg) + '\n' + str(e) + '\n' + str(traceback.format_exc()) + '\n\n\n\n\n')
        finally:
            client_socket.close()

if __name__ == "__main__":
    worker = Worker(sys.argv[1])
