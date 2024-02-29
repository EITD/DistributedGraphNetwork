import random
from ConvertFile import ConvertFile
import json
import sys
from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import xmlrpc.client
from concurrent.futures import ThreadPoolExecutor, as_completed
from socketserver import ThreadingMixIn

NUM_PARTITIONS = 4
# dummy file for test
NODE_FEATURES = "./data/node_features_dummy.txt"

class NodeForOtherWorker(Exception):
    def __init__(self):
        pass
class Worker:
    worker_id = None
    node_data = {}
    graph = {}
    epoch = {}

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

    def load_graph_dict(self):
        self.graph = ConvertFile.toGraph(f"./data/partition_{self.worker_id}.txt", " ")
        
    def node_feature(self, nid, epoch):
        history = self.node_data.get(nid, {})
        # node without feature default value
        return history.get(epoch, 1)
        
    def feature_and_neighborhood(self, nid, delta, epoch):
        node_neighbors_list = list()
        if nid in self.node_data.keys():
            node_neighbors_list = list(self.graph.neighbors(nid))
        random_neighbors = random.sample(node_neighbors_list, delta if len(node_neighbors_list) > delta else len(node_neighbors_list))
        
        return self.node_feature(nid, epoch), random_neighbors
    
    def khop_neighborhood(self, nid, k, deltas):
        sums = self.node_feature(nid, self.epoch.get(nid, 0))
        node_neighbors_set = set()
        if nid in self.node_data.keys():
            node_neighbors_set = set(self.graph.neighbors(nid))
        
        for j in range(k): 
            random_neighbors = random.sample(list(node_neighbors_set), deltas[j] if len(node_neighbors_set) > deltas[j] else len(node_neighbors_set))

            for node in random_neighbors:
                node_epoch = self.epoch.get(node, self.epoch[nid])
                if node_epoch < self.epoch[nid]:
                    return None
                
                if (int(node) % NUM_PARTITIONS) == self.worker_id:
                    if j < k - 1:
                        node_feature, neighborhood = self.feature_and_neighborhood(node, deltas[j + 1], self.epoch.get(nid, 0))
                        node_neighbors_set.update(neighborhood)
                        sums += node_feature
                    else:
                        node_feature = self.node_feature(node, self.epoch.get(nid, 0))
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
                        sums += result['node_feature']
                    except Exception as exc:
                        print(f"khop_neighborhood generated an exception: {exc}")
        
        return sums
    
    def aggregate_neighborhood(self, target_epoch):
        filter_nodes = self.filter_nodes(target_epoch)
        while filter_nodes:
            with ThreadPoolExecutor() as executor:
                for node in filter_nodes: 
                    filter_nodes.remove(node)
                    # synchronous add future
                    executor.submit(self.update_node_epoch_and_wait_for_ack, node, target_epoch, filter_nodes)
                    
        return {nodeKey:value for nodeKey, nodeEpochDict in self.node_data.items() for key, value in nodeEpochDict.items() if key == target_epoch}
    
    def filter_nodes(self, target_epoch):
        return [node for node in list(self.node_data.keys())
                if self.epoch[node] < target_epoch and (int(node) % NUM_PARTITIONS == self.worker_id)]
    
    def update_node_epoch_and_wait_for_ack(self, node, target_epoch, filter_nodes):
        # customize khop parameter
        new_feature = self.khop_neighborhood(node, 1, [5000])
        if new_feature is not None:
            history = self.node_data.get(node, {})
            my_epoch = sorted(list(history.keys()), reverse=True)[0]
            history[my_epoch + 1] = new_feature

            self.epoch[node] += 1
            if self.epoch[node] < target_epoch:
                filter_nodes.append(node)

            request_data = {
                'update_node_epoch': {
                    'nid': node,
                    'epoch': self.epoch[node]
                }
            }
            request_json = json.dumps(request_data)

            with ThreadPoolExecutor() as executor:
                for server in range(4) and server != self.worker_id:
                    executor.submit(self.send_message, server, request_json)
    
    # simple rpc server, start thread in each request but not work
    # def send_to_other(self, node, message):
    #      with ThreadPoolExecutor() as executor:
    #         future = executor.submit(self.send_message, node, message)
    #         response = future.result()
    #         print("Receive response: ", response)
    #         return response

    def send_message(self, node, message):
        print("Send message: ", message)
        while True:
            try:
                port = 12345 + int(node) % NUM_PARTITIONS
                proxy = xmlrpc.client.ServerProxy(f"http://localhost:{port}")
                response = proxy.handle_msg(message)
                if response != "":
                    print("Received response message: ", response)
                return response
            except Exception as e:
                # print("!!!!!!RPC exception!!!!!!")
                continue

# TODO: improve: rpc call different methods
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
                'node_feature' : self.node_feature(nid, epoch), 
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
                'node_feature' : sums if sums is not None else 'Not available.', 
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

            return
        
        request_json = json.dumps(request_data)
        
        return request_json
    
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
        port = 12345 + int(sys.argv[1])
        run_server(sys.argv[1], port)
