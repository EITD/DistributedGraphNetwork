import ast
import random
import socket
from multiprocessing import Process
import threading
from ConvertFile import ConvertFile, nx
# import json
from MySocket import MySocket
import sys
import concurrent.futures

NUM_PARTITIONS = 4
# NUM_PARTITIONS_START = 0
# NUM_PARTITIONS_END = 4
# PARTITION_LIST = [i for i in range(NUM_PARTITIONS_START, NUM_PARTITIONS_END)]
NODE_FEATURES = "./data/node_features.txt"

class NodeForOtherWorker(Exception):
    def __init__(self):
        pass
class Worker:
    worker_id = None
    s = None
    node_data = {}
    graph = {}
    # acc = 0
    epoch = {}
    # graph_weight = {0:0}

    def __init__(self, wid, p):
        self.worker_id = int(wid)
        self.s = MySocket(myNode=wid, port=p, NUM_PARTITIONS=NUM_PARTITIONS)

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
        # print(self.worker_id)
        self.graph = ConvertFile.toGraph(f"./data/partition_{self.worker_id}.txt", " ")
        # self.graph = ConvertFile.toGraph(f"./data/test_{self.worker_id}.txt", " ")
        
    def node_feature(self, nid, epoch):
        history = self.node_data.get(nid, {})
        # node_epoch = self.epoch.get(nid, None)
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
            
            inMyself = []
            for node in random_neighbors:
                node_epoch = self.epoch.get(node, self.epoch[nid])
                if node_epoch < self.epoch[nid]:
                    return None
                
                if (int(node) % NUM_PARTITIONS) == self.worker_id:
                    inMyself.append(node)
                else:
                    if j < k - 1:
                        # request_data = {
                        #     'feature_and_neighborhood' : {
                        #         'nid' : node,
                        #         'delta' : deltas[j + 1],
                        #         'epoch' : self.epoch[nid]
                        #     }
                        # }
                        self.s.ask(threading.current_thread().name + node, node, str(('feature_and_neighborhood', node, deltas[j + 1], self.epoch[nid])))
                    else:
                        # request_data = {
                        #     'node_feature' : node,
                        #     'epoch' : self.epoch[nid]
                        # }
                        self.s.ask(threading.current_thread().name + node, node, str(('node_feature', node, self.epoch[nid])))
                    # request_json = json.dumps(request_data)
                    # self.s.ask(threading.current_thread().name + node, node, request_json)

            random_neighbors = [node for node in random_neighbors if node not in inMyself]
            okDict = {node:False for node in random_neighbors}
            node_neighbors_set = set()
            
            for node in inMyself:
                print(f'!!!self get: {node}!!!')
                if j < k - 1:
                    node_feature, neighborhood = self.feature_and_neighborhood(node, deltas[j + 1], self.epoch[nid])
                    node_neighbors_set.update(neighborhood)
                    sums += node_feature
                else:
                    node_feature = self.node_feature(node, self.epoch[nid])
                    sums += node_feature
            
            while not all(value for value in okDict.values()):
                for node in random_neighbors:
                    if threading.current_thread().name + node in self.s.ask_reply_dict:
                        # request_data = json.loads(self.s.ask_reply_dict.pop(threading.current_thread().name + node))
                        request_data = ast.literal_eval(self.s.ask_reply_dict.pop(threading.current_thread().name + node))
                        
                        if j < k - 1:
                            node_neighbors_set.update(request_data[1])
                        sums += request_data[0]
                        
                        okDict[node] = True
                        random_neighbors.remove(node)
                        break
        
        return sums
    
    def aggregate_neighborhood(self, target_epoch):
        # start = self.epoch + 1
        # for e in range(start, target_epoch + 1):
        filter_nodes = self.filter_nodes(target_epoch)
        # while not all(value == target_epoch for key, value in self.epoch.items() if (int(key) % NUM_PARTITIONS) == self.worker_id):
        while filter_nodes:
            # random.shuffle(filter_nodes)
            for node in filter_nodes: 
                # if self.epoch[node] < target_epoch:
                    # with concurrent.futures.ThreadPoolExecutor() as executor:
                    #     executor.submit(self.update_node_epoch_and_wait_for_ack, node, target_epoch, filter_nodes)
                    update_node_epoch_thread = threading.Thread(target=self.update_node_epoch_and_wait_for_ack, args=(node, target_epoch, filter_nodes))
                    update_node_epoch_thread.start()
                    
        # return self.graph_weight[target_epoch]

    # def aggregate_neighborhood_improve(self, target_epoch):
    #     # start = self.epoch + 1
    #     # for e in range(start, target_epoch + 1):
    #     node_apoch_dic = []
    #     for node in self.filter_nodes(target_epoch): 
    #             # print("node value " + str(node))
    #             new_feature = self.khop_neighborhood(node, 1, [3])
    #             if new_feature is not None:
    #                 history = self.node_data.get(node, {})
    #                 my_epoch = sorted(list(history.keys()), reverse=True)[0]  + 1
    #                 history[my_epoch] = new_feature
    #                 self.graph_weight[my_epoch] = self.graph_weight.get(my_epoch, 0) + new_feature    
    #                 self.epoch[node] += 1
    #                 node_apoch_dic.append((node, self.epoch[node])) 
    #     self.update_node_epoch_and_wait_for_ack(node_apoch_dic)
    #     return self.graph_weight[target_epoch]
    
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
            
            # request_data = {
            #     'update_node_epoch': {
            #         'nid': node,
            #         'epoch': self.epoch[node]
            #     }
            # }
            # request_json = json.dumps(request_data)
            for server in [server for server in list(self.s.serverDict.keys()) if server != self.worker_id]:
                self.s.ask(threading.current_thread().name + str(server), server, str(('update_node_epoch', node, self.epoch[node])))

            # okDict = {server:False for server in list(self.s.serverDict.keys())}
            # while not all(value for value in okDict.values()):
            #     for server in list(self.s.serverDict.keys()):
            #         if threading.current_thread().name + str(server) in self.s.ask_reply_dict:
            #             message = self.s.ask_reply_dict.pop(threading.current_thread().name + str(server))
            #             request_data = json.loads(message)
            #             if request_data['update_epoch_ack'] == "ok":
            #                 okDict[server] = True
        
    # def update_node_epoch_and_wait_for_ack(self, node_apoch_dic):
    #     for node, epoch in node_apoch_dic:
    #         request_data = {
    #             'update_node_epoch': {
    #                 'nid': node,
    #                 'epoch': epoch
    #             }
    #         }
    #         request_json = json.dumps(request_data)
    #         for server in self.s.serverDict.keys():
    #             self.s.ask(threading.current_thread().name + str(server), node=server, msg=request_json)
    #         # Initialize okDict for all servers
    #         okDict = {server: False for server in self.s.serverDict.keys()}
    #         # Wait for all acknowledgements
    #         while not all(okDict.values()):
    #             for server in self.s.serverDict.keys():
    #                 thread_name = threading.current_thread().name + str(server)
    #                 if thread_name in self.s.ask_reply_dict:
    #                     message = self.s.ask_reply_dict.pop(thread_name)
    #                     request_data = json.loads(message)
    #                     if request_data.get('update_epoch_ack') == "ok":
    #                         okDict[server] = True

    def handle_msg(self, client_socket, message):
        # request_data = json.loads(message)
        request_data = ast.literal_eval(message)
        
        try:
            if 'node_feature' in request_data:
                nid = request_data[1]
                if len(request_data) > 2:
                    epoch = int(request_data[2])
                else:
                    epoch = self.epoch.get(nid, 0)
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                # request_data = {
                #     'node_feature' : self.node_feature(nid, epoch), # feature
                # }
                request_data = str((self.node_feature(nid, epoch), ))
                
            elif 'khop_neighborhood' in request_data:
                nid = request_data[1]
                k = request_data[2]
                deltas = request_data[3]
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                sums = self.khop_neighborhood(nid, k, deltas)
                
                # request_data = {
                #     'node_feature' : sums if sums is not None else 'Not available.', # feature
                # }
                
                request_data = str((sums if sums is not None else 'Not available.', ))
                
            elif 'feature_and_neighborhood' in request_data:
                nid = request_data[1]
                delta = request_data[2]
                epoch = request_data[3]
                
                if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                    raise NodeForOtherWorker()
                
                feature, neighborhoodSet = self.feature_and_neighborhood(nid, delta, epoch)
                # request_data = {
                #     'node_feature' : feature,
                #     'neighborhood' : neighborhoodSet
                # }
                request_data = str((feature, neighborhoodSet))
            
            elif 'neighborhood_aggregation' in request_data:
                final_epoch = request_data[1]
                    
                # request_data = {
                #     'graph_weight': {
                #         'target_epoch': final_epoch
                #     }
                # }
                # request_json = json.dumps(request_data)
                request_data = str(('graph_weight', final_epoch))

                for server in list(self.s.serverDict.keys()):
                    self.s.ask(threading.current_thread().name + str(server), server, request_data)
                
                new_epoch = {}
                okDict = {server:False for server in list(self.s.serverDict.keys())}
                while not all(value for value in okDict.values()):
                    for server in list(self.s.serverDict.keys()):
                        if threading.current_thread().name + str(server) in self.s.ask_reply_dict:
                            message = self.s.ask_reply_dict.pop(threading.current_thread().name + str(server))
                            request_data = ast.literal_eval(message)
                            # new_epoch.update(request_data[0])
                            okDict[server] = True

                # request_data = {
                #     'new_epoch' : new_epoch
                # }
                request_data = str(("all_epoch_ok"))
                        
            elif 'graph_weight' in request_data:
                target_epoch = request_data[1]

                if target_epoch > sorted(list(set(self.epoch.values())))[0]:
                    self.aggregate_neighborhood(target_epoch)
                #     request_data = {
                #         'new_epoch' : 
                #     } 
                # else:
                # request_data = {
                #         'new_epoch' : {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
                #     }
                # request_data = str(({nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}, ))
                request_data = str(("graph_epoch_ok"))
            
            elif 'update_node_epoch' in request_data:
                node = request_data[1]
                epoch = request_data[2]

                self.epoch[node] = epoch

                # request_data = {
                #     'update_epoch_ack' : "ok"
                # }
                # return
                request_data = str(("ok"))
            
            # request_json = json.dumps(request_data)
        except NodeForOtherWorker:
            self.s.ask(threading.current_thread().name + nid, node=nid, msg=message)
        
            while True:
                if threading.current_thread().name + nid in self.s.ask_reply_dict:
                    request_data = self.s.ask_reply_dict.pop(threading.current_thread().name + nid)
                    break
        
        self.s.message_send_queue.put((client_socket, request_data))


def start_worker(wid, port):
    worker = Worker(wid, port)
    worker.load_node_data()
    worker.load_graph_dict()
    while True:
        if not worker.s.message_get_queue.empty():
            client_socket, message = worker.s.message_get_queue.get()
            # with concurrent.futures.ThreadPoolExecutor() as handle_thread:
            #     handle_thread.submit(worker.handle_msg, client_socket, message)
            handle_thread = threading.Thread(target=worker.handle_msg, args=(client_socket, message))
            handle_thread.start()

if __name__ == "__main__":
        start_worker(sys.argv[1], 12345+int(sys.argv[1]))
        # w = Process(target=start_worker, args=(sys.argv[1], 12345+int(sys.argv[1])))
        # w.start()
