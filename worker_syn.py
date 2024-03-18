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

try:
    profile
except NameError:
    def profile(func):
        return func
    
system = platform.system()

NUM_PARTITIONS = 4
# dummy file for test
NODE_FEATURES = "./data_small/node_feature_small.txt"
# host = '130.229.152.41'
# testIp = host
# serverDict = {0:('130.229.166.49',12345), 1:(testIp,12346), 2:(testIp,12347), 3:(testIp,12348)}

host = 'localhost'
testIp = host
serverDict = {0:(testIp,12345), 1:(testIp,12346), 2:(testIp,12347), 3:(testIp,12348)}
NODE_DEFAULT_FEATURE = 0

class NodeForOtherWorker(Exception):
    def __init__(self):
        pass
class Worker:
    worker_id = None
    node_data = {}
    graph = {}
    # epoch = {}
    # updateFlag = True

    def __init__(self, wid):
        self.worker_id = int(wid)

    @profile
    def load_node_data(self):
        with open(NODE_FEATURES, 'r') as file:
            lines = file.readlines()
        for line in lines:
            parts = line.strip().split()[:2]
            # self.epoch[parts[0]] = 0
            if int(parts[0]) % NUM_PARTITIONS == self.worker_id:
                self.node_data[parts[0]] = {0:int(parts[1])}

    @profile
    def load_graph_dict(self):
        self.graph = ConvertFile.toGraph(f"./data_small/partition_{self.worker_id}_small.txt", " ")
        
    @profile   
    def node_feature(self, nid, epoch):
        history = self.node_data.get(nid, {})
        return history.get(epoch, NODE_DEFAULT_FEATURE)
        
    def feature_and_neighborhood(self, nid, delta, epoch):
        node_neighbors_list = list()
        if nid in self.node_data.keys():
            node_neighbors_list = list(self.graph.neighbors(nid))
        random_neighbors = random.sample(node_neighbors_list, delta if len(node_neighbors_list) > delta else len(node_neighbors_list))
        
        return self.node_feature(nid, epoch), random_neighbors
    
    @profile
    def khop_neighborhood(self, nid, k, deltas, epoch):
        try:
            sums = self.node_feature(nid, epoch)
            
            node_neighbors_set = set()
            if nid in self.node_data.keys():
                node_neighbors_set = set(self.graph.neighbors(nid))
            
            for j in range(k):
                random_neighbors = random.sample(list(node_neighbors_set), deltas[j] if len(node_neighbors_set) > deltas[j] else len(node_neighbors_set))
                node_neighbors_set = set()
                
                # for node in random_neighbors:
                #     node_epoch = self.epoch.get(node, self.epoch[nid])
                #     if node_epoch < self.epoch[nid]:
                #         return None
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    feature_and_neighborhood_list = []
                    feature_and_neighborhood_list_ask = []
                    for node in random_neighbors:
                        if (int(node) % NUM_PARTITIONS) == self.worker_id:
                            # print(f'!!!self get: {node}!!!')
                            if j < k - 1:
                                future = executor.submit(self.feature_and_neighborhood, node, deltas[j + 1], epoch)
                            else: 
                                future = executor.submit(self.node_feature, node, epoch)
                            feature_and_neighborhood_list.append(future)
                        else:
                            if j < k - 1:
                                request_data = {
                                    'feature_and_neighborhood' : {
                                        'nid' : node,
                                        'delta' : deltas[j + 1],
                                        'epoch' : epoch
                                    }
                                }
                                future = executor.submit(ask, node, json.dumps(request_data))
                            else:
                                request_data = {
                                    'node_feature' : node,
                                    'epoch' : epoch
                                }
                                future = executor.submit(ask, node, json.dumps(request_data))
                            feature_and_neighborhood_list_ask.append(future)
                concurrent.futures.wait(feature_and_neighborhood_list)
                concurrent.futures.wait(feature_and_neighborhood_list_ask)

                node_neighbors_set = set()
                
                for future in feature_and_neighborhood_list:
                    if j < k - 1:
                        node_feature, neighborhood = future.result()
                        node_neighbors_set.update(neighborhood)
                    else:
                        node_feature = future.result()
                    sums += node_feature
                for ask_future in feature_and_neighborhood_list_ask:
                    msg = ask_future.result()
                    data = json.loads(msg)
                    if j < k - 1:
                        node_neighbors_set.update(data['neighborhood'])
                    sums += data['node_feature']
        except Exception as e:
            with open('khop_neighborhood', 'a') as f:
                f.write(str(msg) + '\n' + str(e) + '\n' + str(traceback.format_exc()) + '\n\n\n\n\n')
        return sums
    
    def aggregate_neighborhood_sync(self, k, deltas, epoch):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for node in list(self.node_data.keys()):
                executor.submit(self.update_node_epoch_sync, node, k, deltas, epoch)
        return {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == epoch}

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
    
    def update_node_epoch_sync(self, node, k, deltas, epoch):
        new_feature = self.khop_neighborhood(node, k, deltas, epoch - 1)
        
        history = self.node_data.get(node, {})
        # my_epoch = sorted(list(history.keys()), reverse=True)[0]
        history[epoch] = new_feature

        # self.epoch[node] += 1
        
        # request_data = {
        #     'update_node_epoch': {
        #         'nid': node,
        #         'epoch': self.epoch[node]
        #     }
        # }
        # request_json = json.dumps(request_data)

        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     for server in range(4):
        #         if server != self.worker_id:
        #             executor.submit(tell, server, request_json)
    
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

    def handle_msg(self, message):
        request_data = json.loads(message)
        
        try:
            if 'node_feature' in request_data:
                nid = request_data['node_feature']
                # epoch = int(request_data.get('epoch', self.epoch.get(nid, 0)))
                epoch = int(request_data.get('epoch', 0))
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                request_data = {
                    'node_feature' : self.node_feature(nid, epoch)
                }
                
            elif 'khop_neighborhood' in request_data:
                nid = request_data['khop_neighborhood']['nid']
                k = request_data['khop_neighborhood']['k']
                deltas = request_data['khop_neighborhood']['deltas']
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                sums = self.khop_neighborhood(nid, k, deltas, 0)
                
                request_data = {
                    'node_feature' : sums if sums is not None else 'Not available.'
                }
                
            elif 'feature_and_neighborhood' in request_data:
                nid = request_data['feature_and_neighborhood']['nid']
                delta = request_data['feature_and_neighborhood']['delta']
                epoch = request_data['feature_and_neighborhood']['epoch']
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                feature, neighborhoodSet = self.feature_and_neighborhood(nid, delta, epoch)
                request_data = {
                    'node_feature' : feature, 
                    'neighborhood' : neighborhoodSet 
                }
            
            elif 'neighborhood_aggregation_sync' in request_data:
                final_epoch = request_data['neighborhood_aggregation_sync']['epochs']
                k = request_data['neighborhood_aggregation_sync']['k']
                deltas = request_data['neighborhood_aggregation_sync']['deltas']
            
                for epoch in range(1, final_epoch + 1):
                    request_data = {
                        'graph_weight_sync': {
                            'target_epoch': epoch,
                            'k': k,
                            'deltas': deltas
                        }
                    }
                    request_json = json.dumps(request_data)

                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        futures = [executor.submit(ask, server, request_json) for server in range(4)]

                    # if epoch <= final_epoch:
                    if epoch == final_epoch:
                        epoch_dict = {}
                        for future in futures:
                            try:
                                response = future.result()
                                request_data = json.loads(response)
                                epoch_dict.update(request_data['graph_weight_sync'])
                            except Exception as exc:
                                print(f"neighborhood_aggregation generated an exception: {exc}")
                    
                request_data = {
                    'epoch_dict' : epoch_dict
                }

            # elif 'neighborhood_aggregation_async' in request_data:
            #     final_epoch = request_data['neighborhood_aggregation_async']['epochs']
            #     k = request_data['neighborhood_aggregation_async']['k']
            #     deltas = request_data['neighborhood_aggregation_async']['deltas']

            #     request_data = {
            #         'graph_weight_async': {
            #             'target_epoch': final_epoch,
            #             'k': k,
            #             'deltas': deltas
            #         }
            #     }
            #     request_json = json.dumps(request_data)

            #     epoch_dict = {}
            #     with concurrent.futures.ThreadPoolExecutor() as executor:
            #         futures = []
            #         for server in range(4):
            #             future = executor.submit(ask, server, request_json)
            #             futures.append(future)
            #     for future in futures:
            #         try:
            #             response = future.result()
            #             request_data = json.loads(response)
            #             epoch_dict.update(request_data['graph_weight_async'])
            #         except Exception as exc:
            #             print(f"neighborhood_aggregation generated an exception: {exc}")

            #     request_data = {
            #         'epoch_dict' : epoch_dict
            #     }    
                        
            elif 'graph_weight_sync' in request_data:
                target_epoch = request_data['graph_weight_sync']['target_epoch']
                k = request_data['graph_weight_sync']['k']
                deltas = request_data['graph_weight_sync']['deltas']

                # if target_epoch <= sorted(list(set(self.epoch.values())))[0]:
                #     request_data = {
                #         'graph_weight_sync' : {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
                #     } 
                # else:
                request_data = {
                    'graph_weight_sync' : self.aggregate_neighborhood_sync(k, deltas, target_epoch)
                }
            
            # elif 'graph_weight_async' in request_data:
            #     target_epoch = request_data['graph_weight_async']['target_epoch']
            #     k = request_data['graph_weight_async']['k']
            #     deltas = request_data['graph_weight_async']['deltas']

            #     while target_epoch > min(value for key, value in self.epoch.items() if (int(key) % NUM_PARTITIONS) == self.worker_id):
            #         # print('do one more time')
            #         if self.updateFlag:
            #             self.aggregate_neighborhood_async(target_epoch, k, deltas)
            #             self.updateFlag = False
            #         else:
            #             sleep(0.5)
            #     request_data = {
            #         'graph_weight_async' : {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
            #     }
            
            # elif 'update_node_epoch' in request_data:
            #     node = request_data['update_node_epoch']['nid']
            #     epoch = request_data['update_node_epoch']['epoch']
                
            #     if epoch > self.epoch[node]:
            #         self.epoch[node] = epoch
            #         self.updateFlag = True

            #     return
            request_json = json.dumps(request_data)
        except NodeForOtherWorker:
            return ask(nid, message)
        
        return request_json
        
def handle_client(client_socket, worker):
    global system
    try:
        data = client_socket.recv(102400)
        print('get msg:', data)
        
        # if b'__TELL__' not in data:
        message = worker.handle_msg(data.decode())
        print('send out:', message)
        client_socket.send(message.encode())
        # else:
        #     worker.handle_msg(data.replace(b'__TELL__', b'', 1).decode())
    finally:
        if system == 'Darwin':
            client_socket.shutdown(socket.SHUT_WR)
        client_socket.close()

def ask(node, msg):
    print('ask:', msg)
    while True:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            client_socket.connect(serverDict.get(int(node) % NUM_PARTITIONS))
            client_socket.send(msg.encode())
            
            data = client_socket.recv(102400).decode()
            
            print('get reply:', data)

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
#             client_socket.connect(serverDict.get(int(server) % NUM_PARTITIONS))
#             client_socket.send(b'__TELL__'+msg.encode())
            
#             Mac bug:
#             阶段性关闭：当一方完成数据发送后，它首先使用shutdown(socket.SHUT_WR)来关闭写方向，表明已经发送完毕。这允许对方知道不会有更多的数据发送过来，但仍然可以继续发送数据到另一方。
#             等待对方完成：当一方执行了写方向的关闭后，它继续从对方读取数据，直到收到对方的结束信号和/或直到对方也执行了写方向的关闭。
#             全面关闭：在双方都执行了写方向的关闭，并且确认各自都接收到了对方的所有数据和关闭信号后，才执行shutdown(socket.SHUT_RDWR)或close()来完全关闭socket。
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

def start_worker(wid, port):
    worker = Worker(wid)
    worker.load_node_data()
    worker.load_graph_dict()
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
    server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    server_socket.bind((host, port))
    server_socket.listen(3000)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            client_socket, _ = server_socket.accept()
            executor.submit(handle_client, client_socket, worker)

if __name__ == "__main__":
    start_worker(sys.argv[1], 12345 + int(sys.argv[1]))
