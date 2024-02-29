import json
import time
import networkx
import xmlrpc.client
import threading
from ConvertFile import ConvertFile

def send_message(message):
    start_time = time.time()
    with xmlrpc.client.ServerProxy("http://localhost:12345") as proxy:
        response = proxy.handle_msg(message)
        print(f"Server response: {response}")
        end_time = time.time()
        print("time: ", end_time - start_time)

        # aggregate_neighborhood test
        test(response)


def test(response):
    # when node feature all 1(load dummmy), default is 2 ** epoch
    # epoch = 6
        # dic = json.loads(response)['epoch_dict']
        # for key, value in dic.items():
        #         if 2 ** epoch != value:
        #                 print('False at:', key, 'get:', value, 'should be:', 2 ** epoch)
    
    # when node feature all 1(load dummmy), default is 1, 1 epoch all neighbors
        all_graph = ConvertFile.toGraph(f"./data/neighbor.txt", " ")
        data = json.loads(response)['epoch_dict']
        for key, value in data.items():
                try:
                    v = len(list(all_graph.neighbors(key)))
                    if (v + 1) != value:
                        # if v != 0:
                            print("Warning:", key, value, 'should be:', v + 1)
                except (networkx.exception.NetworkXError):
                    pass

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

# train method
def aggregate_neighborhood(epochs):
    request_data = {
        'neighborhood_aggregation': {
            'epochs': epochs
        }
    }
    request_json = json.dumps(request_data)
    threading.Thread(target=send_message, args=(request_json,)).start()


# query_node_feature(0)

# query_node_feature(1)

# query_khop_neighborhood(3, 1, 5)

# query_khop_neighborhood(3, 3, [2, 18, 32])
    
# query_khop_neighborhood(0, 1, 5000)

aggregate_neighborhood(0, 1)
