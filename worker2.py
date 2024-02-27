import random
import socket
from multiprocessing import Process
import threading
from ConvertFile import ConvertFile, nx
import json
from MySocket import MySocket
import sys
import concurrent.futures
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
from concurrent.futures import ThreadPoolExecutor
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCRequestHandler

NUM_PARTITIONS = 4
NODE_FEATURES = "./data/node_features.txt"

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

    def __init__(self, wid):
        self.worker_id = int(wid)
        # self.s = MySocket(myNode=wid, portList=portList, NUM_PARTITIONS=NUM_PARTITIONS)

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
        node_neighbors_list = list(self.graph.neighbors(nid))
        random_neighbors = random.sample(node_neighbors_list, delta if len(node_neighbors_list) > delta else len(node_neighbors_list))
        
        return self.node_feature(nid, epoch), random_neighbors
    
    def khop_neighborhood(self, nid, k, deltas):
        sums = self.node_feature(nid, self.epoch[nid])
        node_neighbors_set = set(self.graph.neighbors(nid))
        
        for j in range(k): # [2,3,2]
            random_neighbors = random.sample(list(node_neighbors_set), deltas[j] if len(node_neighbors_set) > deltas[j] else len(node_neighbors_set))
            
            # inMyself = []
            for node in random_neighbors:
                node_epoch = self.epoch.get(node, self.epoch[nid])
                if node_epoch < self.epoch[nid]:
                    return None
                
                if (int(node) % NUM_PARTITIONS) == self.worker_id:
                    # inMyself.append(node)
                    if j < k - 1:
                        node_feature, neighborhood = self.feature_and_neighborhood(node, deltas[j + 1], self.epoch[nid])
                        node_neighbors_set.update(neighborhood)
                        sums += node_feature
                    else:
                        node_feature = self.node_feature(node, self.epoch[nid])
                        print("self node feature: ", node_feature)
                        sums += node_feature
                else:
                    if j < k - 1:
                        request_data = {
                            'feature_and_neighborhood' : {
                                'nid' : node,
                                'delta' : deltas[j + 1],
                                'epoch' : self.epoch[nid]
                            }
                        }
                    else:
                        request_data = {
                            'node_feature' : node,
                            'epoch' : self.epoch[nid]
                        }
                    request_json = json.dumps(request_data)

                    # TODO: multi-thread
                    response = self.send_message(node, request_json)
                    result = json.loads(response)
                    if j < k - 1:
                        node_neighbors_set.update(result['neighborhood'])
                    sums += result['node_feature']

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
            for node in filter_nodes: 
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(self.update_node_epoch_and_wait_for_ack, node, target_epoch, filter_nodes)
                # update_node_epoch_thread = threading.Thread(target=self.update_node_epoch_and_wait_for_ack, args=(node, target_epoch, filter_nodes))
                # update_node_epoch_thread.start()
                    
        return {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
    
    def filter_nodes(self, target_epoch):
        return [node for node in list(self.node_data.keys())
                if self.epoch[node] < target_epoch and (int(node) % NUM_PARTITIONS == self.worker_id)]
    
    def update_node_epoch_and_wait_for_ack(self, node, target_epoch, filter_nodes):
        new_feature = self.khop_neighborhood(node, 0, [0])
        if new_feature is not None:
            history = self.node_data.get(node, {})
            my_epoch = sorted(list(history.keys()), reverse=True)[0]
            history[my_epoch + 1] = new_feature

            # if self.graph_weight.get(my_epoch + 1, None) is not None:
            #     self.graph_weight[my_epoch + 1] += new_feature
            # else:
            #     self.graph_weight[my_epoch + 1] = new_feature

            self.epoch[node] += 1
            if self.epoch[node] >= target_epoch:
                filter_nodes.remove(node)
            
            request_data = {
                'update_node_epoch': {
                    'nid': node,
                    'epoch': self.epoch[node]
                }
            }
            request_json = json.dumps(request_data)
            for server in list(self.s.serverDict.keys()):
                self.s.ask(threading.current_thread().name + str(server), node=server, msg=request_json)

            okDict = {server:False for server in list(self.s.serverDict.keys())}
            while not all(value for value in okDict.values()):
                for server in list(self.s.serverDict.keys()):
                    if threading.current_thread().name + str(server) in self.s.ask_reply_dict:
                        message = self.s.ask_reply_dict.pop(threading.current_thread().name + str(server))
                        request_data = json.loads(message)
                        if request_data['update_epoch_ack'] == "ok":
                            okDict[server] = True

    # def send_to_other(self, node, message):
    #      with ThreadPoolExecutor() as executor:
    #         future = executor.submit(self.send_message, node, message)
    #         response = future.result()
    #         print("Receive response: ", response)
    #         return response

    def send_message(self, node, message):
        port = 12345 + int(node) % NUM_PARTITIONS
        proxy = xmlrpc.client.ServerProxy(f"http://localhost:{port}")
        print("Send message: ", message)
        response = proxy.handle_msg(message)
        print("Received response message: ", message)
        return response

    def handle_msg(self, message):
        # port, client_socket, message = self.s.message_get_queue.get()
        print("Received handle message: ", message)
        request_data = json.loads(message)
        
        # try:
        if 'node_feature' in request_data:
            nid = request_data['node_feature']
            epoch = int(request_data.get('epoch', self.epoch.get(nid, 0)))
            

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

            request_data = {
                'update_epoch_ack' : "ok"
            }
            # return
        
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
