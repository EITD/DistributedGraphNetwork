import json
import time
import xmlrpc.client
import threading
from ConvertFile import ConvertFile

# graph all edges from 4 partition
all_graph = ConvertFile.toGraph(f"./data/neighbor.txt", " ")

def send_message(message):
    start_time = time.time()
    # default send to worker 0
    with xmlrpc.client.ServerProxy("http://localhost:12345") as proxy:
        response = proxy.handle_msg(message)
        print(f"Server response: {response}")
        end_time = time.time()
        print("time: ", end_time - start_time)

        # train test
        # test(response)

# TODO: refactor test code
def test(response):
    # when node feature all 1(load dummmy), default is 2 ** epoch, multiple epochs, k = 1, deltas = [1]
    # epoch = 6
        # dic = json.loads(response)['epoch_dict']
        # for key, value in dic.items():
        #         if 2 ** epoch != value:
        #                 print('False at:', key, 'get:', value, 'should be:', 2 ** epoch)
    
    # when node feature all 1(load dummmy), default is 1, epoch = 1, customize k, deltas = [5000, 5000**2...]
        data = json.loads(response)['epoch_dict']
        k = 2
        for key, value in data.items():
            sums = 1
            neighborsList = list(all_graph.neighbors(key))
            sums += len(neighborsList)
            sums += recursion(k - 1, neighborsList)
            if sums != value:
                print("Warning:", key, 'value:', value, 'should be:', sums)

def recursion(k, neighborsList):
    sums = 0
    for n in neighborsList:
        temp = list(all_graph.neighbors(n))
        sums += len(temp)
        if k == 1: return sums
        else: recursion(k-1, temp)
    return sums

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
        'neighborhood_aggregation': {
            'epochs': epochs,
            'k': k,
            'deltas': deltas,
            'sync': True
        }
    }
    request_json = json.dumps(request_data)
    threading.Thread(target=send_message, args=(request_json,)).start()

def train_asynchronize(epochs, k, deltas):
    if type(deltas) is int:
        deltas = [deltas]
    request_data = {
        'neighborhood_aggregation': {
            'epochs': epochs,
            'k': k,
            'deltas': deltas,
            'sync': False
        }
    }
    request_json = json.dumps(request_data)
    threading.Thread(target=send_message, args=(request_json,)).start()


# query_node_feature(0)

# query_node_feature(1)

# query_khop_neighborhood(4031, 2, [5000, 5000**2])

# query_khop_neighborhood(3, 3, [2, 18, 32])
    
# query_khop_neighborhood(0, 1, 5000)

# train_synchronize(2, 1, 5000)

# train_asynchronize(2, 1, 5000)
    
train_asynchronize(1, 2, [5000, 5000**2])
