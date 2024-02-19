import socket
import json
from MySocket import MySocket

def query_node_feature():
    request_data = {
        'node_feature': "1"
    }
    request_json = json.dumps(request_data)
    s = MySocket(myNode=None, port=10000, NUM_PARTITIONS=4, client=True)
    s.ask(0, node=1, msg=request_json)

query_node_feature()


def query_khop_neighborhood(nid, k, deltas):
    request_data = {
        'khop_neighborhood': {
            'nid': nid,
            'k': k,
            'deltas': deltas
        }
    }

    request_json = json.dumps(request_data)

    s = MySocket(myNode=None, port=10000, NUM_PARTITIONS=4, client=True)
    s.ask(0, node=nid, msg=request_json)

query_khop_neighborhood('0', 2, [2, 2])  
