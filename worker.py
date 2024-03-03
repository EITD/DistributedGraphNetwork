import random
import socket
from multiprocessing import Process
import threading
from ConvertFile import ConvertFile, nx
import json
import sys
import concurrent.futures

NUM_PARTITIONS = 4
NODE_FEATURES = "./data/node_features.txt"
host = 'localhost'
testIp = host
serverDict = {0:(testIp,12345), 1:(testIp,12346), 2:(testIp,12347), 3:(testIp,12348)}

class NodeForOtherWorker(Exception):
    def __init__(self):
        pass
class Worker:
    worker_id = None
    node_data = {}
    graph = {}
    epoch = {}
    update = False

    def __init__(self, wid):
        self.worker_id = int(wid)

    def load_node_data(self):
        with open(NODE_FEATURES, 'r') as file:
            lines = file.readlines()
        for line in lines:
            parts = line.strip().split()[:2]
            self.epoch[parts[0]] = 0
            if int(parts[0]) % NUM_PARTITIONS == self.worker_id:
                self.node_data[parts[0]] = {0:int(parts[1])}
                # self.graph_weight[0] += int(parts[1])

    def load_graph_dict(self):
        self.graph = ConvertFile.toGraph(f"./data/partition_{self.worker_id}.txt", " ")
        
    def node_feature(self, nid, epoch):
        history = self.node_data.get(nid, {})
        return history.get(epoch, 0)
        
    def feature_and_neighborhood(self, nid, delta, epoch):
        node_neighbors_list = list()
        if nid in self.node_data.keys():
            node_neighbors_list = list(self.graph.neighbors(nid))
        random_neighbors = random.sample(node_neighbors_list, delta if len(node_neighbors_list) > delta else len(node_neighbors_list))
        
        return self.node_feature(nid, epoch), random_neighbors
    
    def khop_neighborhood(self, nid, k, deltas):
        sums = self.node_feature(nid, self.epoch[nid])
        
        if nid in self.node_data.keys():
            node_neighbors_set = set(self.graph.neighbors(nid))
        else:
            return sums
        
        for j in range(k): # [2,3,2]
            random_neighbors = random.sample(list(node_neighbors_set), deltas[j] if len(node_neighbors_set) > deltas[j] else len(node_neighbors_set))
            
            for node in random_neighbors:
                node_epoch = self.epoch.get(node, self.epoch[nid])
                if node_epoch < self.epoch[nid]:
                    return None
            
            feature_and_neighborhood_list = []
            only_node_feature_list = []
            feature_and_neighborhood_list_ask = []
            only_node_feature_list_ask = []
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for node in random_neighbors:
                    if j < k - 1:
                        if (int(node) % NUM_PARTITIONS) == self.worker_id:
                            print(f'!!!self get: {node}!!!')
                            future = executor.submit(self.feature_and_neighborhood, node, deltas[j + 1], self.epoch[nid])
                            feature_and_neighborhood_list.append(future)
                        else:
                            request_data = {
                                'feature_and_neighborhood' : {
                                    'nid' : node,
                                    'delta' : deltas[j + 1],
                                    'epoch' : self.epoch[nid]
                                }
                            }
                            request_json = json.dumps(request_data)
                            future = executor.submit(ask, node, request_json)
                            feature_and_neighborhood_list_ask.append(future)
                    else:
                        if (int(node) % NUM_PARTITIONS) == self.worker_id:
                            future = executor.submit(self.node_feature, node, self.epoch[nid])
                            only_node_feature_list.append(future)
                        else:
                            request_data = {
                                'node_feature' : node,
                                'epoch' : self.epoch[nid]
                            }
                            request_json = json.dumps(request_data)
                            future = executor.submit(ask, node, request_json)
                            only_node_feature_list_ask.append(future)

            node_neighbors_set = set()
            
            for future in feature_and_neighborhood_list:
                node_feature, neighborhood = future.result()
                node_neighbors_set.update(neighborhood)
                sums += node_feature
            for future in only_node_feature_list:
                node_feature = future.result()
                sums += node_feature
            for future in feature_and_neighborhood_list_ask:
                data = json.loads(future.result())
                node_feature = data['node_feature']
                neighborhood = data['neighborhood']
                node_neighbors_set.update(neighborhood)
                sums += node_feature
            for future in only_node_feature_list_ask:
                data = json.loads(future.result())
                node_feature = data['node_feature']
                sums += node_feature
        
        return sums
    
    def aggregate_neighborhood(self, target_epoch):
        filter_nodes = self.filter_nodes(target_epoch)
        needDo = filter_nodes.copy()
        temp = needDo.copy()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            while True:
                for node in temp:
                    needDo.remove(node)
                    filter_nodes.remove(node)
                    executor.submit(self.update_node_epoch_and_wait_for_ack, node, target_epoch, 1, [5000], filter_nodes, needDo)
                    if self.update:
                        break
                
                if self.update:
                    print('epoch update')
                    needDo = random.shuffle(filter_nodes.copy())
                    self.update = False
                    continue
                else:
                    temp = needDo.copy()
                
                result = {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
                if len(result) == len(self.node_data):
                    break
        
        return {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
    
    def filter_nodes(self, target_epoch):
        return [node for node in list(self.node_data.keys())
                if self.epoch[node] < target_epoch and (int(node) % NUM_PARTITIONS == self.worker_id)]
    
    def update_node_epoch_and_wait_for_ack(self, node, target_epoch, k, deltas, filter_nodes, needDo):
        new_feature = self.khop_neighborhood(node, k, deltas)

        if new_feature is not None:
            history = self.node_data.get(node, {})
            my_epoch = sorted(list(history.keys()), reverse=True)[0]
            history[my_epoch + 1] = new_feature
            
            self.epoch[node] += 1

            request_data = {
                'update_node_epoch': {
                    'nid': node,
                    'epoch': self.epoch[node]
                }
            }
            request_json = json.dumps(request_data)

            with concurrent.futures.ThreadPoolExecutor() as executor1:
                for server in range(4):
                    if server != self.worker_id:
                        executor1.submit(self.s.tell, server, request_json)
            
            if self.epoch[node] < target_epoch:
                needDo.append(node)
                filter_nodes.append(node)
        else:
            filter_nodes.append(node)

    def handle_msg(self, message):
        request_data = json.loads(message)
        
        try:
            if 'node_feature' in request_data:
                nid = request_data['node_feature']
                epoch = int(request_data.get('epoch', self.epoch.get(nid, 0)))
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                request_data = {
                    'node_feature' : self.node_feature(nid, epoch), # feature
                }
                
            elif 'khop_neighborhood' in request_data:
                nid = request_data['khop_neighborhood']['nid']
                k = request_data['khop_neighborhood']['k']
                deltas = request_data['khop_neighborhood']['deltas']
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                sums = self.khop_neighborhood(nid, k, deltas)
                
                request_data = {
                    'node_feature' : sums if sums is not None else 'Not available.', # feature
                }
                
            elif 'feature_and_neighborhood' in request_data:
                nid = request_data['feature_and_neighborhood']['nid']
                delta = request_data['feature_and_neighborhood']['delta']
                epoch = request_data['feature_and_neighborhood']['epoch']
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                feature, neighborhoodSet = self.feature_and_neighborhood(nid, delta, epoch)
                request_data = {
                    'node_feature' : feature, # feature
                    'neighborhood' : neighborhoodSet # [nid, nid, nid...]
                }
            
            elif 'neighborhood_aggregation' in request_data:
                final_epoch = request_data['neighborhood_aggregation']['epochs']
                    
                request_data = {
                    'graph_weight': {
                        'target_epoch': final_epoch
                    }
                }
                request_json = json.dumps(request_data)

                for server in list(self.s.serverDict.keys()):
                    self.s.ask(threading.current_thread().name + str(server), node=server, msg=request_json)
                
                # sum_graph = 0
                epoch_dict = {}
                okDict = {server:False for server in list(self.s.serverDict.keys())}
                while not all(value for value in okDict.values()):
                    for server in list(self.s.serverDict.keys()):
                        if threading.current_thread().name + str(server) in self.s.ask_reply_dict:
                            message = self.s.ask_reply_dict.pop(threading.current_thread().name + str(server))
                            request_data = json.loads(message)
                            epoch_dict.update(request_data['graph_weight'])
                            okDict[server] = True

                request_data = {
                    'epoch_dict' : epoch_dict
                }     
                        
            elif 'graph_weight' in request_data:
                target_epoch = request_data['graph_weight']['target_epoch']

                if target_epoch <= sorted(list(set(self.epoch.values())))[0]:
                    request_data = {
                        'graph_weight' : {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
                    } 
                else:
                    request_data = {
                        'graph_weight' : self.aggregate_neighborhood(target_epoch)
                    }
            
            elif 'update_node_epoch' in request_data:
                node = request_data['update_node_epoch']['nid']
                epoch = request_data['update_node_epoch']['epoch']

                self.epoch[node] = epoch
                
                self.update = True

                request_data = 'ok'
            
            request_json = json.dumps(request_data)
        except NodeForOtherWorker:
            return ask(nid, message)
        
        return request_json
        
def handle_client(client_socket, worker):
    data = client_socket.recv(20480)
    print('get msg:', data)
    
    if b'__TELL__' not in data:
        message = worker.handle_msg(data.decode())
    else:
        message = worker.handle_msg(data.replace(b'__TELL__', b'', 1).decode())
        # client_socket.send('ok'.encode())
        client_socket.shutdown(socket.SHUT_RDWR)
        client_socket.close()
    
    print('send out:', message)
    
    client_socket.send(message.encode())

def ask(node, msg):
    while True:
        # client_socket = self.conn_pool.get_conn()
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            client_socket.connect(serverDict.get(int(node) % NUM_PARTITIONS))
            client_socket.send(msg.encode())
            print('ask:', msg)
            
            data = client_socket.recv(102400).decode()
            
            print('get reply:', data)

            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()
            
            return data
        except (ConnectionRefusedError):
            print('ask error')
            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()
            continue

def tell(server, msg):
    while True:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            client_socket.connect(serverDict.get(int(server) % NUM_PARTITIONS))
            client_socket.send(b'__TELL__'+msg.encode())
            print('tell:', msg)
            
            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()
            break
        except (ConnectionRefusedError):
            print('tell error')
            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()
            continue

def start_worker(wid, port):
    worker = Worker(wid)
    worker.load_node_data()
    worker.load_graph_dict()
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    server_socket.bind((host, port))
    server_socket.listen(30000)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            client_socket, _ = server_socket.accept()
            executor.submit(handle_client, client_socket, worker)

if __name__ == "__main__":
        # portList = [12345 + sys.argv[1] + (i*4) for i in range(1000)]
        start_worker(sys.argv[1], 12345 + int(sys.argv[1]))
