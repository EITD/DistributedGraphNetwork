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

system = platform.system()

NUM_PARTITIONS = 4
NODE_FEATURES = "./data/node_features.txt"
host = 'localhost'
NODE_DEFAULT_FEATURE = 0
serverDict = [host, host, host, host]

class NodeForOtherWorker(Exception):
    def __init__(self):
        pass

# class Marker:
#     def __init__(self):
#         pass

class Worker:
    worker_id = None
    # vertexDict = {}
    
    def __init__(self, wid):
        self.worker_id = int(wid)
        # TODO: 加一个socket用来收feature
        
        graph = ConvertFile.toGraph(f"./data/neighbor.txt", " ")
        initial_vertex = []
        
        with open(NODE_FEATURES, 'r') as file:
            lines = file.readlines()
        for line in lines:
            parts = line.strip().split()[:2]
            if int(parts[0]) % NUM_PARTITIONS == self.worker_id:
                out_edges = graph.successors(parts[0])
                in_edges = graph.predecessors(parts[0])
                # self.vertexDict[12345 + int(parts[0])] = Vertex(parts[0], int(parts[1]), in_edges, out_edges)
                if list(out_edges) == 0:
                    initial_vertex.append(parts[0])
                Vertex(parts[0], int(parts[1]), in_edges, out_edges)
        
        initial_vertex_ask_list = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for vertex in initial_vertex:
                future = executor.submit(ask, vertex, "snapshot")
                initial_vertex_ask_list.append(future)
        concurrent.futures.wait(initial_vertex_ask_list)

class Vertex:
    def __init__(self, node, feature, in_edges, out_edges):
        self.port = 12345 + int(node)
        self.sp = [feature] # [feature epoch 0, feature epoch 1, ...]
        # self.feature = feature
        # in_edges is Op in chandy lamport algorithm.
        self.in_edges_list = list(in_edges)
        # out_edges is Ip in chandy lamport algorithm.
        self.out_edges_list = list(out_edges)
        # self.epoch_dict = {}
        # n_f.append(inbox.pop) until '__MARKER__0'
        self.neighbor_features = [] # [['v0f23', ...], ['v0v10f13', ...], ['v0v10v20f33', ...], ...] len(n_f)==k (features we use to khop epoch 1)
        self.inbox = [] # [k, deltas, '__MARKER__e0v0', ..., 'v0v10v20fxxx', 'v0v10fxxxx', 'v0fxxxxx', '__MARKER__e0v8', ..., '__MARKER__e1v0', ..., ...]
        self.Mp = [] # [m, m, m, ...] this is part of inbox
        self.Recorded = [] # [all out_edges]

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        server_socket.bind((host, self.port))
        server_socket.listen(5000)
        with concurrent.futures.ProcessPoolExecutor() as executor:
            while True:
                client_socket, _ = server_socket.accept()
                executor.submit(self.handle_client, client_socket)
    
    def epoch(self):
        return len(self.sp) - 1
    
    def get(self, epoch):
        try:
            return self.sp[epoch]
        except IndexError:
            return None
        
    # def feature_and_neighborhood(self, nid, delta, epoch):
    #     node_neighbors_list = list()
    #     if nid in self.node_data.keys():
    #         node_neighbors_list = list(self.graph.neighbors(nid))
    #     random_neighbors = random.sample(node_neighbors_list, delta if len(node_neighbors_list) > delta else len(node_neighbors_list))
        
    #     return self.node_feature(nid, epoch), random_neighbors
    
    def khop_neighborhood(self, k, deltas):
        # self.neighbor_features = [[] for i in range(k)]
        try:
            sums = self.get(self.epoch())
            
            node_neighbors_set = set(self.out_edges_list)
            # node_neighbors_set = set(['v' + i for i in out_edges_list])
            # prefix = ""
            
            for j in range(k): # [2,3,2]
                random_neighbors = random.sample(list(node_neighbors_set), deltas[j] if len(node_neighbors_set) > deltas[j] else len(node_neighbors_set))
                node_neighbors_set = set()

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
                    if j < k - 1:
                        for v in self.neighbor_features[j + 1]:
                            if v.startswith("v" + vertex):
                                # start_index = len("v" + vertex)
                                end_index = v.find("f")
                                sub_text = v[1:end_index] 
                                node_neighbors_set.add(sub_text)
                
                # featrueList = [self.epoch_dict.get(vertex, None) for vertex in random_neighbors]
                
                # while None in featrueList:
                #     sleep(3)
                #     featrueList = [self.epoch_dict.get(vertex, None) for vertex in random_neighbors]
                
                # neighborhood_ask_list = []
                # with concurrent.futures.ThreadPoolExecutor() as executor:
                #     for node in random_neighbors:
                #         if j < k - 1:
                #             request_data = {
                #                 'out_edges' : {
                #                     'delta' : deltas[j + 1]
                #                 }
                #             }
                #             future = executor.submit(ask, node, json.dumps(request_data))
                #         neighborhood_ask_list.append(future)
                # concurrent.futures.wait(neighborhood_ask_list)
                
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
    
    # def aggregate_neighborhood_async(self, target_epoch, k, deltas):
    #     minEpoch = min(value for key, value in self.epoch.items() if (int(key) % NUM_PARTITIONS) == self.worker_id)
    #     filter_nodes_1 = self.filter_nodes(minEpoch + 1)
    #     filter_nodes_2 = self.filter_nodes(target_epoch)
    #     filter_nodes = filter_nodes_1.copy()
    #     filter_nodes.extend(node for node in filter_nodes_2 if node not in filter_nodes_1)
    #     with concurrent.futures.ThreadPoolExecutor() as executor:
    #         futures = []
    #         for node in filter_nodes:
    #             future = executor.submit(self.update_node_epoch_async, node, target_epoch, k, deltas, executor)
    #             futures.append(future)
    #         concurrent.futures.wait(futures)

    # def filter_nodes(self, target_epoch):
    #     return [node for node in list(self.node_data.keys())
    #             if self.epoch[node] < target_epoch and (int(node) % NUM_PARTITIONS == self.worker_id)]
    
    # def update_node_epoch_async(self, node, target_epoch, k, deltas, executor):
    #     new_feature = self.khop_neighborhood(node, k, deltas)
        
    #     if new_feature is not None:
    #         history = self.node_data.get(node, {})
    #         my_epoch = sorted(list(history.keys()), reverse=True)[0]
    #         history[my_epoch + 1] = new_feature
            
    #         self.epoch[node] += 1
            
    #         request_data = {
    #             'update_node_epoch': {
    #                 'nid': node,
    #                 'epoch': self.epoch[node]
    #             }
    #         }
    #         request_json = json.dumps(request_data)

    #         with concurrent.futures.ThreadPoolExecutor() as broadcast:
    #             for server in range(4):
    #                 if server != self.worker_id:
    #                     broadcast.submit(tell, server, request_json)
            
    #         if self.epoch[node] < target_epoch:
    #             future = executor.submit(self.update_node_epoch_async, node, target_epoch, k, deltas, executor)
    #             concurrent.futures.wait(future)
    
    def handle_client(self, client_socket):
        try:
            data = client_socket.recv(102400)
            print('get msg:', data)
            
            # if b'__MARKER__' not in data:
            self.handle_msg(data.decode())
            # print('send out:', message)
            # client_socket.send(message.encode())
            # else:
            #     self.handle_msg(data.replace(b'__MARKER__', b'', 1).decode())
        finally:
            if system == 'Darwin':
                client_socket.shutdown(socket.SHUT_WR)
            client_socket.close()

    def handle_msg(self, message):
        request_data = json.loads(message)
        
        try:
            if 'node_feature' in request_data:
                nid = request_data['node_feature']
                epoch = int(request_data.get('epoch', self.epoch.get(nid, 0)))
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                request_data = {
                    'node_feature' : self.node_feature(nid, epoch) # feature
                }
                
            elif 'khop_neighborhood' in request_data:
                nid = request_data['khop_neighborhood']['nid']
                k = request_data['khop_neighborhood']['k']
                deltas = request_data['khop_neighborhood']['deltas']
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                sums = self.khop_neighborhood(nid, k, deltas)
                
                request_data = {
                    'node_feature' : sums if sums is not None else 'Not available.' # feature
                }
                
            elif 'out_edges' in request_data:
                delta = request_data['out_edges']['delta']
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                sned = random.sample(self.out_edges_list, delta if len(self.out_edges_list) > delta else len(self.out_edges_list))
                
                request_data = {
                    'out_edges' : sned # [nid, nid, nid...]
                }
                
            elif 'neighborhood_aggregation_async' in request_data:
                final_epoch = request_data['neighborhood_aggregation_async']['epochs']
                k = request_data['neighborhood_aggregation_async']['k']
                deltas = request_data['neighborhood_aggregation_async']['deltas']

                request_data = {
                    'graph_weight_async': {
                        'target_epoch': final_epoch,
                        'k': k,
                        'deltas': deltas
                    }
                }
                request_json = json.dumps(request_data)

                epoch_dict = {}
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = []
                    for server in range(4):
                        future = executor.submit(ask, server, request_json)
                        futures.append(future)
                for future in futures:
                    try:
                        response = future.result()
                        request_data = json.loads(response)
                        epoch_dict.update(request_data['graph_weight_async'])
                    except Exception as exc:
                        print(f"neighborhood_aggregation generated an exception: {exc}")

                request_data = {
                    'epoch_dict' : epoch_dict
                }
            
            elif 'graph_weight_async' in request_data:
                target_epoch = request_data['graph_weight_async']['target_epoch']
                k = request_data['graph_weight_async']['k']
                deltas = request_data['graph_weight_async']['deltas']

                while target_epoch > min(value for key, value in self.epoch.items() if (int(key) % NUM_PARTITIONS) == self.worker_id):
                    # print('do one more time')
                    if self.updateFlag:
                        self.aggregate_neighborhood_async(target_epoch, k, deltas)
                        self.updateFlag = False
                    else:
                        sleep(0.5)
                request_data = {
                    'graph_weight_async' : {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
                }

            request_json = json.dumps(request_data)
        except NodeForOtherWorker:
            return ask(nid, message)
        
        return request_json
        

def ask(node, msg):
    print('ask:', msg)
    while True:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            client_socket.connect((serverDict[int(node) % NUM_PARTITIONS], 12345 + int(node)))
            client_socket.send(msg.encode())
            
            # data = client_socket.recv(102400).decode()
            
            # print('get reply:', data)

            client_socket.shutdown(socket.SHUT_WR)
            client_socket.close()
            return data
        except ConnectionRefusedError:
            print('ask connection error')
            client_socket.close()
            continue
        except OSError:
            print('ask os error')
            client_socket.close()
            sleep(1)
            continue
        # except Exception as e:
        #     with open('ask', 'a') as f:
        #         f.write(str(msg) + '\n' + str(e) + '\n' + str(traceback.format_exc()) + '\n\n\n\n\n')
        finally:
            client_socket.close()
    

# def tell(server, msg):
#     print('tell:', msg)
#     while True:
#         client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#         client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
#         client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
#         try:
#             client_socket.connect(serverDict[int(server) % NUM_PARTITIONS])
#             client_socket.send(b'__TELL__'+msg.encode())
            
#             client_socket.shutdown(socket.SHUT_WR)
#             client_socket.close()
#             break
#         except ConnectionRefusedError:
#             print('tell connection error')
#             client_socket.close()
#             continue
#         except OSError as e:
#             print('tell os error')
#             client_socket.close()
#             sleep(1)
#             continue
#         # except Exception as e:
#         #     with open('tell', 'a') as f:
#         #         f.write(str(msg) + '\n' + str(e) + '\n' + str(traceback.format_exc()) + '\n\n\n\n\n')
#         finally:
#             client_socket.close()

if __name__ == "__main__":
    worker = Worker(sys.argv[1])
