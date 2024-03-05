import json
import xmlrpc.client
import threading
from ConvertFile import ConvertFile
from decorators import timeit

# graph all edges from 4 partition
all_graph = ConvertFile.toGraph(f"./data/neighbor.txt", " ")
host = 'http://192.168.1.102:12345'

@timeit
def send_message(message):
    # default send to worker 0
    proxy = xmlrpc.client.ServerProxy(host)
    print("Send message: ", message)
    response = proxy.handle_msg(message)
    print(f"Server response: {response}")
    
    # long response write into file
    # with open('check', 'w') as f: 
    #     f.write(str(json.loads(response)['epoch_dict'])) 

    # train test
    # test_mult_epochs(response, epoch)
    # test_all_neighbors(response)

# when node feature all 1(load dummmy), default is 2 ** epoch, multiple epochs, k = 1, deltas = [1]
def test_mult_epochs(response, epoch):
    dic = json.loads(response)['epoch_dict']
    for key, value in dic.items():
            if 2 ** epoch != value:
                    print('False at:', key, 'get:', value, 'should be:', 2 ** epoch)

# when node feature all 1(load dummmy), default is 1, epoch = 1, k = 1, deltas = [5000]
def test_all_neighbors(response):
    data = json.loads(response)['epoch_dict']
    for key, value in data.items():
        sums = 1
        neighborsList = list(all_graph.neighbors(key))
        sums += len(neighborsList)
        if sums != value:
            print("Warning:", key, 'value:', value, 'should be:', sums)
            

def query_node_feature(nid):
    request_data = {
        'node_feature': str(nid)
    }
    request_json = json.dumps(request_data)
    threading.Thread(target=send_message, args=(request_json,)).start()

def query_khop_neighborhood(nid, k, deltas):
    if type(deltas) is int:
        deltas = [deltas]
    request_data = {
        'khop_neighborhood': {
            'nid': str(nid),
            'k': k,
            'deltas': deltas
        }
    }
    request_json = json.dumps(request_data)
    threading.Thread(target=send_message, args=(request_json,)).start()
    
def train_synchronize(epochs, k, deltas):
    if type(deltas) is int:
        deltas = [deltas]
    request_data = {
        'neighborhood_aggregation_sync': {
            'epochs': epochs,
            'k': k,
            'deltas': deltas
        }
    }
    request_json = json.dumps(request_data)
    threading.Thread(target=send_message, args=(request_json,)).start()

def train_asynchronize(epochs, k, deltas):
    if type(deltas) is int:
        deltas = [deltas]
    request_data = {
        'neighborhood_aggregation_async': {
            'epochs': epochs,
            'k': k,
            'deltas': deltas
        }
    }
    request_json = json.dumps(request_data)
    threading.Thread(target=send_message, args=(request_json,)).start()


# query_node_feature(0)

# query_node_feature(1)

# query_khop_neighborhood(8, 2, [5000, 5000**2])

query_khop_neighborhood(3, 3, [2, 18, 32])
    
# query_khop_neighborhood(0, 1, 5000)

# train_synchronize(2, 1, 5000)

# train_asynchronize(2, 1, 5000)

# train_asynchronize(5, 1, 1)

# train_asynchronize(1, 1, 5000)
