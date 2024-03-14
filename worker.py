import random
from time import sleep
from ConvertFile import ConvertFile
import json
import sys
from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import xmlrpc.client
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from socketserver import ThreadingMixIn
try:
    profile
except NameError:
    def profile(func):
        return func

NUM_PARTITIONS = 4
# dummy file for test
NODE_FEATURES = "./data/node_features.txt"
# node without feature default value
NODE_DEFAULT_FEATURE = 0
server_list = ['130.229.183.193', '130.229.153.122', '130.229.153.122', '130.229.153.122']

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

    @profile
    def load_node_data(self):
        with open(NODE_FEATURES, 'r') as file:
            lines = file.readlines()
        for line in lines:
            parts = line.strip().split()[:2]
            self.epoch[parts[0]] = 0
            if int(parts[0]) % NUM_PARTITIONS == self.worker_id:
                self.node_data[parts[0]] = {0:int(parts[1])}

    @profile
    def load_graph_dict(self):
        self.graph = ConvertFile.toGraph(f"./data/partition_{self.worker_id}.txt", " ")
        
    @profile   
    def node_feature(self, nid, epoch):
        history = self.node_data.get(nid, {})
        temp = history.get(epoch, NODE_DEFAULT_FEATURE)
        # asy debug: history only contains epoch 0 
        # if epoch == 1 and nid in self.node_data.keys() and len(list(self.graph.neighbors(nid))) > 0 and temp == 0:
        #     with open('return_feature_error', 'a') as f: 
        #         f.write(nid + " " + str(history) + "\n")
        return temp
        
    @profile    
    def feature_and_neighborhood(self, nid, delta, epoch):
        node_neighbors_list = list()
        if nid in self.node_data.keys():
            node_neighbors_list = list(self.graph.neighbors(nid))
        random_neighbors = random.sample(node_neighbors_list, delta if len(node_neighbors_list) > delta else len(node_neighbors_list))
        
        return self.node_feature(nid, epoch), random_neighbors
    
    @profile
    def khop_neighborhood(self, nid, k, deltas):
        sums = self.node_feature(nid, self.epoch.get(nid, 0))
        node_neighbors_set = set()
        if nid in self.node_data.keys():
            node_neighbors_set = set(self.graph.neighbors(nid))
        
        for j in range(k): 
            random_neighbors = random.sample(list(node_neighbors_set), deltas[j] if len(node_neighbors_set) > deltas[j] else len(node_neighbors_set))
            node_neighbors_set = set()

            for node in random_neighbors:
                node_epoch = self.epoch.get(node, self.epoch[nid])
                if node_epoch < self.epoch[nid]:
                    return None

            with ThreadPoolExecutor() as executor:
                future_to_node = {}
                for node in random_neighbors:
                    if (int(node) % NUM_PARTITIONS) == self.worker_id:
                        if j < k - 1:
                            node_feature, neighborhood = self.feature_and_neighborhood(node, deltas[j + 1], self.epoch.get(nid, 0))
                            node_neighbors_set.update(neighborhood)
                            sums += node_feature
                        else:
                            node_feature = self.node_feature(node, self.epoch.get(nid, 0))
                            sums += node_feature
                    else:        
                        if j < k - 1:
                            request_data = {
                                'feature_and_neighborhood' : {
                                    'nid' : node,
                                    'delta' : deltas[j + 1],
                                    'epoch' : self.epoch.get(nid, 0)
                                }
                            }
                        else:
                            request_data = {
                                'node_feature' : node,
                                'epoch' : self.epoch.get(nid, 0)
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
                        # asy debug: node's epoch is 0, which means bug in above return None
                        # if self.epoch[nid] == 1 and result['node_feature'] != result_syn_1_1_5000.get(node, 0):
                        #     with open('read_feature_error', 'a') as f: 
                        #         f.write(str(self.worker_id) + " " + nid + " " + node + " " + str(self.epoch.get(node, self.epoch[nid])) + " " + str(result['node_feature']) + " " + str(result_syn_1_1_5000.get(node, 0)) + "\n")
                        sums += result['node_feature']
                    except Exception as exc:
                        print(f"khop_neighborhood generated an exception: {exc}")

        return sums
    
    @profile
    def aggregate_neighborhood_sync(self, target_epoch, k, deltas):
        with ThreadPoolExecutor() as executor:
            for node in list(self.node_data.keys()):
                executor.submit(self.update_node_epoch_sync, node, k, deltas)
                
        return {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
    
    @profile
    def aggregate_neighborhood_async(self, target_epoch, k, deltas):
        minEpoch = min(value for key, value in self.epoch.items() if (int(key) % NUM_PARTITIONS) == self.worker_id)
        filter_nodes_1 = self.filter_nodes(minEpoch + 1)
        filter_nodes_2 = self.filter_nodes(target_epoch)
        filter_nodes = filter_nodes_1.copy()
        filter_nodes.extend(node for node in filter_nodes_2 if node not in filter_nodes_1)
        with ThreadPoolExecutor() as executor:
            futures = []
            for node in filter_nodes:
                future = executor.submit(self.update_node_epoch_async, node, target_epoch, k, deltas, executor)
                futures.append(future)
            # update = true的时候跳出循环，然后重新开始
            wait(futures)

    @profile
    def filter_nodes(self, target_epoch):
        return [node for node in list(self.node_data.keys())
                if self.epoch[node] < target_epoch and (int(node) % NUM_PARTITIONS == self.worker_id)]
    
    @profile
    def update_node_epoch_sync(self, node, k, deltas):
        new_feature = self.khop_neighborhood(node, k, deltas)
        
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

        with ThreadPoolExecutor() as executor:
            for server in range(4):
                if server != self.worker_id:
                    executor.submit(self.send_message, server, request_json)
    
    @profile
    def update_node_epoch_async(self, node, target_epoch, k, deltas, executor):
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

            with ThreadPoolExecutor() as executor1:
                for server in range(4):
                    if server != self.worker_id:
                        executor1.submit(self.send_message, server, request_json)
            
            if self.epoch[node] < target_epoch:
                future = executor.submit(self.update_node_epoch_async, node, target_epoch, k, deltas, executor)
                wait(future)

    
    # simple rpc server, start thread in each request but not work
    # def send_to_other(self, node, message):
    #      with ThreadPoolExecutor() as executor:
    #         future = executor.submit(self.send_message, node, message)
    #         response = future.result()
    #         print("Receive response: ", response)
    #         return response

    @profile
    def send_message(self, node, message):
        print("Send message: ", message)
        while True:
            try:
                port = 12345 + int(node) % NUM_PARTITIONS
                server = server_list[int(node) % NUM_PARTITIONS]
                proxy = xmlrpc.client.ServerProxy(f"http://{server}:{port}")
                response = proxy.handle_msg(message)
                print("Received response message: ", response)
                return response
            except Exception as e:
                # print(e)
                # print("!!!!!!RPC exception!!!!!!, retrying...")
                continue

# TODO: improve: rpc call different methods
    @profile
    def handle_msg(self, message):
        print("Received handle message: ", message)
        request_data = json.loads(message)
        
        if 'node_feature' in request_data:
            nid = request_data['node_feature']
            epoch = int(request_data.get('epoch', self.epoch.get(nid, 0)))

            if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                response = self.send_message(nid, message)
                return response

            request_data = {
                'node_feature' : self.node_feature(nid, epoch)
            }
            
        elif 'khop_neighborhood' in request_data:
            nid = request_data['khop_neighborhood']['nid']
            k = request_data['khop_neighborhood']['k']
            deltas = request_data['khop_neighborhood']['deltas']
            
            if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                response = self.send_message(nid, message)
                return response
            
            sums = self.khop_neighborhood(nid, k, deltas)
            
            request_data = {
                'node_feature' : sums if sums is not None else 'Not available.'
            }
            
        elif 'feature_and_neighborhood' in request_data:
            nid = request_data['feature_and_neighborhood']['nid']
            delta = request_data['feature_and_neighborhood']['delta']
            epoch = request_data['feature_and_neighborhood']['epoch']
            
            if (int(nid) % NUM_PARTITIONS) != self.worker_id:
                response = self.send_message(nid, message)
                return response
            
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

                with ThreadPoolExecutor() as executor:
                    futures = {executor.submit(self.send_message, server, request_json): server for server in range(4)}

                if epoch == final_epoch:
                    epoch_dict = {}
                    for future in as_completed(futures):
                        try:
                            response = future.result()
                            request_data = json.loads(response)
                            epoch_dict.update(request_data['graph_weight_sync'])
                        except Exception as exc:
                            print(f"neighborhood_aggregation generated an exception: {exc}")
                
            request_data = {
                'epoch_dict' : epoch_dict
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
            with ThreadPoolExecutor() as executor:
                futures = {executor.submit(self.send_message, server, request_json): server for server in range(4)}
                for future in as_completed(futures):
                    try:
                        response = future.result()
                        request_data = json.loads(response)
                        epoch_dict.update(request_data['graph_weight_async'])
                    except Exception as exc:
                        print(f"neighborhood_aggregation generated an exception: {exc}")

            request_data = {
                'epoch_dict' : epoch_dict
            }    
                    
        elif 'graph_weight_sync' in request_data:
            target_epoch = request_data['graph_weight_sync']['target_epoch']
            k = request_data['graph_weight_sync']['k']
            deltas = request_data['graph_weight_sync']['deltas']

            if target_epoch <= sorted(list(set(self.epoch.values())))[0]:
                request_data = {
                    'graph_weight_sync' : {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
                } 
            else:
                request_data = {
                    'graph_weight_sync' : self.aggregate_neighborhood_sync(target_epoch, k, deltas)
                }
        
        elif 'graph_weight_async' in request_data:
            target_epoch = request_data['graph_weight_async']['target_epoch']
            k = request_data['graph_weight_async']['k']
            deltas = request_data['graph_weight_async']['deltas']

            while target_epoch > min(value for key, value in self.epoch.items() if (int(key) % NUM_PARTITIONS) == self.worker_id):
                # print('do one more time')
                if self.update:
                    self.aggregate_neighborhood_async(target_epoch, k, deltas)
                    self.update = False
                else:
                    sleep(0.5)

            request_data = {
                'graph_weight_async' : {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
            }

        elif 'update_node_epoch' in request_data:
            node = request_data['update_node_epoch']['nid']
            epoch = request_data['update_node_epoch']['epoch']

            if epoch > self.epoch[node]:
                self.epoch[node] = epoch
                self.update = True
            
            # self.update = True
            # 如果更新的是最小的epoch，就把update改成true。

            return 'ok'
        
        request_json = json.dumps(request_data)
        
        print('reply:', request_json)
        return request_json
    
class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

def run_server(wid, port):
    server = ThreadedXMLRPCServer((server_list[int(wid)], port), requestHandler=RequestHandler)
    worker = Worker(wid)
    worker.load_node_data()
    worker.load_graph_dict()
    server.register_instance(worker)  
    server.serve_forever()

if __name__ == "__main__":
        port = 12345 + int(sys.argv[1])
        run_server(sys.argv[1], port)
