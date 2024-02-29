import random
import socket
from multiprocessing import Process
import threading
import time
from ConvertFile import ConvertFile, nx
import json
from MySocket import MySocket
import sys
import concurrent.futures
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
from concurrent.futures import ThreadPoolExecutor, as_completed
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCRequestHandler

NUM_PARTITIONS = 4
NODE_FEATURES = "./data/node_features_dummy.txt"

class NodeForOtherWorker(Exception):
    def __init__(self):
        pass

class Worker:
    worker_id = None
    # s = None
    node_data = {}
    graph = {}
    # acc = 0
    epoch = {}
    # graph_weight = {0:0}
    
    node_data_lock = threading.Lock()
    epoch_lock = threading.Lock()

    def __init__(self, wid):
        self.worker_id = int(wid)
        # self.s = MySocket(myNode=wid, portList=portList, NUM_PARTITIONS=NUM_PARTITIONS)
        
    def write_node_data(self, key, value):
        with self.node_data_lock:
            self.node_data[key] = value

    def read_node_data(self, key):
        with self.node_data_lock:
            return self.node_data.get(key, {})
    
    def write_epoch(self, key, value):
        with self.epoch_lock:
            self.epoch[key] = value

    def read_epoch(self, key, defalut=0):
        with self.epoch_lock:
            return self.epoch.get(key, defalut)

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
        # history = self.node_data.get(nid, {})
        history = self.read_node_data(nid)
        temp = history.get(epoch, 1)
        return temp
        
    def feature_and_neighborhood(self, nid, delta, epoch):
        node_neighbors_list = list()
        if nid in self.node_data.keys():
            node_neighbors_list = list(self.graph.neighbors(nid))
        random_neighbors = random.sample(node_neighbors_list, delta if len(node_neighbors_list) > delta else len(node_neighbors_list))
        
        return self.node_feature(nid, epoch), random_neighbors
    
    def khop_neighborhood(self, nid, k, deltas):
        sums = self.node_feature(nid, self.read_epoch(nid))
        node_neighbors_set = set()
        if nid in self.node_data.keys():
            node_neighbors_set = set(self.graph.neighbors(nid))
        
        for j in range(k): # [2,3,2]
            random_neighbors = random.sample(list(node_neighbors_set), deltas[j] if len(node_neighbors_set) > deltas[j] else len(node_neighbors_set))

            # inMyself = []
            for node in random_neighbors:
                node_epoch = self.read_epoch(node, float('inf'))
                if node_epoch < self.read_epoch(nid):
                    return None
                
                if (int(node) % NUM_PARTITIONS) == self.worker_id:
                    # inMyself.append(node)
                    if j < k - 1:
                        node_feature, neighborhood = self.feature_and_neighborhood(node, deltas[j + 1], self.read_epoch(nid))
                        node_neighbors_set.update(neighborhood)
                        sums += node_feature
                    else:
                        node_feature = self.node_feature(node, self.read_epoch(nid))
                        sums += node_feature
                    random_neighbors.remove(node)

            with ThreadPoolExecutor() as executor:
                future_to_node = {}
                for node in random_neighbors:
                    if j < k - 1:
                        request_data = {
                            'feature_and_neighborhood' : {
                                'nid' : node,
                                'delta' : deltas[j + 1],
                                'epoch' : self.read_epoch(nid)
                            }
                        }
                    else:
                        request_data = {
                            'node_feature' : node,
                            'epoch' : self.read_epoch(nid)
                        }
                    future = executor.submit(self.send_message, node, json.dumps(request_data))
                    future_to_node[future] = node
                
                for future in as_completed(future_to_node):
                    node = future_to_node[future]
                    try:
                        response = future.result()
                        result = json.loads(response)
                        if j < k - 1:
                            node_neighbors_set.update(result['neighborhood'])
                        sums += result['node_feature']
                    except Exception as exc:
                        print(f"khop_neighborhood generated an exception: {exc}")
                        # exc.with_traceback()

                    # self.s.ask(threading.current_thread().name + node, node, request_json)

            # random_neighbors = [node for node in random_neighbors if node not in inMyself]
            # okDict = {node:False for node in random_neighbors}
            # node_neighbors_set = set()
            
            # for node in inMyself:
            #     print(f'!!!self get: {node}!!!')
            #     if j < k - 1:
            #         node_feature, neighborhood = self.feature_and_neighborhood(node, deltas[j + 1], self.epoch[nid])
            #         node_neighbors_set.update(neighborhood)
            #         sums += node_feature
            #     else:
            #         node_feature = self.node_feature(node, self.epoch[nid])
            #         sums += node_feature
            
            # while not all(value for value in okDict.values()):
            #     for node in random_neighbors:
            #         if threading.current_thread().name + node in self.s.ask_reply_dict:
            #             request_data = json.loads(self.s.ask_reply_dict.pop(threading.current_thread().name + node))
                        
            #             if j < k - 1:
            #                 node_neighbors_set.update(request_data['neighborhood'])
            #             sums += request_data['node_feature']
                        
            #             okDict[node] = True
            #             random_neighbors.remove(node)
            #             break
        
        return sums
    
    def aggregate_neighborhood(self, target_epoch):
        filter_nodes = self.filter_nodes(target_epoch)
        while filter_nodes:
            with ThreadPoolExecutor() as executor:
                for node in filter_nodes: 
                    filter_nodes.remove(node)
                    executor.submit(self.update_node_epoch_and_wait_for_ack, node, target_epoch, filter_nodes)
                # update_node_epoch_thread = threading.Thread(target=self.update_node_epoch_and_wait_for_ack, args=(node, target_epoch, filter_nodes))
                # update_node_epoch_thread.start()
                    
        return {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
    
    def filter_nodes(self, target_epoch):
        return [node for node in list(self.node_data.keys())
                if self.epoch[node] < target_epoch and (int(node) % NUM_PARTITIONS == self.worker_id)]
    
    def update_node_epoch_and_wait_for_ack(self, node, target_epoch, filter_nodes):
        new_feature = self.khop_neighborhood(node, 1, [5000])
        if new_feature is not None:
            # history = self.node_data.get(node, {})
            history = self.read_node_data(node)
            my_epoch = sorted(list(history.keys()), reverse=True)[0]
            history[my_epoch + 1] = new_feature

            # if self.graph_weight.get(my_epoch + 1, None) is not None:
            #     self.graph_weight[my_epoch + 1] += new_feature
            # else:
            #     self.graph_weight[my_epoch + 1] = new_feature

            temp = self.read_epoch(node)
            if (temp + 1) < target_epoch:
                filter_nodes.append(node)
            self.write_epoch(node, temp + 1)

            request_data = {
                'update_node_epoch': {
                    'nid': node,
                    'epoch': temp + 1
                }
            }
            request_json = json.dumps(request_data)

            with ThreadPoolExecutor() as executor:
                for server in range(4) and server != self.worker_id:
                    executor.submit(self.send_message, server, request_json)
            
            # for server in list(self.s.serverDict.keys()):
            #     self.s.ask(threading.current_thread().name + str(server), node=server, msg=request_json)

            # okDict = {server:False for server in list(self.s.serverDict.keys())}
            # while not all(value for value in okDict.values()):
            #     for server in list(self.s.serverDict.keys()):
            #         if threading.current_thread().name + str(server) in self.s.ask_reply_dict:
            #             message = self.s.ask_reply_dict.pop(threading.current_thread().name + str(server))
            #             request_data = json.loads(message)
            #             if request_data['update_epoch_ack'] == "ok":
            #                 okDict[server] = True

    # def send_to_other(self, node, message):
    #      with ThreadPoolExecutor() as executor:
    #         future = executor.submit(self.send_message, node, message)
    #         response = future.result()
    #         print("Receive response: ", response)
    #         return response

    def send_message(self, node, message):
        print("Send message: ", message)
        try:
            port = 12345 + int(node) % NUM_PARTITIONS
            proxy = xmlrpc.client.ServerProxy(f"http://localhost:{port}")
            response = proxy.handle_msg(message)
            if response != "":
                print("Received response message: ", response)
            return response
        except Exception as e:
            print("!!!!!!RPC exception!!!!!!")

# TODO: improve: rpc call different methods
    def handle_msg(self, message):
        # port, client_socket, message = self.s.message_get_queue.get()
        print("Received handle message: ", message)
        request_data = json.loads(message)
        
        # try:
        if 'node_feature' in request_data:
            nid = request_data['node_feature']
            epoch = int(request_data.get('epoch', self.read_epoch(nid)))
            

            if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                response = self.send_message(nid, message)
                return response
                # raise NodeForOtherWorker()
            
            request_data = {
                'node_feature' : self.node_feature(nid, epoch), # feature
            }
            
        elif 'khop_neighborhood' in request_data:
            nid = request_data['khop_neighborhood']['nid']
            k = request_data['khop_neighborhood']['k']
            deltas = request_data['khop_neighborhood']['deltas']
            
            if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                response = self.send_message(nid, message)
                return response
                # raise NodeForOtherWorker()
            
            sums = self.khop_neighborhood(nid, k, deltas)
            
            request_data = {
                'node_feature' : sums if sums is not None else 'Not available.', # feature
            }
            
        elif 'feature_and_neighborhood' in request_data:
            nid = request_data['feature_and_neighborhood']['nid']
            delta = request_data['feature_and_neighborhood']['delta']
            epoch = request_data['feature_and_neighborhood']['epoch']
            
            if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                response = self.send_message(nid, message)
                return response
                # raise NodeForOtherWorker()
            
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

            epoch_dict = {}
            with ThreadPoolExecutor() as executor:
                futures = {executor.submit(self.send_message, server, request_json): server for server in range(4)}
                for future in as_completed(futures):
                    try:
                        response = future.result()
                        request_data = json.loads(response)
                        epoch_dict.update(request_data['graph_weight'])
                    except Exception as exc:
                        print(f"neighborhood_aggregation generated an exception: {exc}")
            
            # sum_graph = 0
            # epoch_dict = {}
            # okDict = {server:False for server in list(self.s.serverDict.keys())}
            # while not all(value for value in okDict.values()):
            #     for server in list(self.s.serverDict.keys()):
            #         if threading.current_thread().name + str(server) in self.s.ask_reply_dict:
            #             message = self.s.ask_reply_dict.pop(threading.current_thread().name + str(server))
            #             request_data = json.loads(message)
            #             epoch_dict.update(request_data['graph_weight'])
            #             okDict[server] = True

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

            self.write_epoch(node, epoch)

            # request_data = {
            #     'update_epoch_ack' : "ok"
            # }
            return
        
        request_json = json.dumps(request_data)
        # except NodeForOtherWorker:
            
            # self.s.ask(threading.current_thread().name + nid, node=nid, msg=message)
        
            # while True:
            #     if threading.current_thread().name + nid in self.s.ask_reply_dict:
            #         request_json = self.s.ask_reply_dict.pop(threading.current_thread().name + nid)
            #         break
        
        return request_json
        # self.s.message_send_queue_dict[port].put((client_socket, request_json))
    
    # def send_to_other(self, nid, message):
    #     with ThreadPoolExecutor() as executor:
    #         future = executor.submit(self.send_message, nid, message)
    #         result = future.result()
    #         return result        

# def start_worker(wid, portList):
    # for port in portList:
    #     threading.Thread(target=run_server, args=(wid, port,)).start()
    # worker = Worker(wid, portList)
    # worker.load_node_data()
    # worker.load_graph_dict()
    # while True:
    #     if not worker.s.message_get_queue.empty():
    #         handle_thread = threading.Thread(target=worker.handle_msg)
    #         handle_thread.start()

class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

def run_server(wid, port):
    server = ThreadedXMLRPCServer(('localhost', port), requestHandler=RequestHandler)
    worker = Worker(wid)
    worker.load_node_data()
    worker.load_graph_dict()
    server.register_instance(worker)  
    server.serve_forever()

if __name__ == "__main__":
        # portList = [12345 + int(sys.argv[1]) + (i*4) for i in range(3000)]
        port = 12345 + int(sys.argv[1])
        run_server(sys.argv[1], port)
        # start_worker(sys.argv[1], portList)
