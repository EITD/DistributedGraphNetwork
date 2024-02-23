import signal
import socket
import json
import sys
from MySocket import MySocket

s = MySocket(myNode=None, port=10000, NUM_PARTITIONS=4, client=True)

def query_node_feature(nid):
    request_data = {
        'node_feature': str(nid)
    }
    request_json = json.dumps(request_data)
    s.ask(0, node=nid, msg=request_json)

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
    s.ask(0, node=nid, msg=request_json)

def aggregate_neighborhood(nid, epochs):
    request_data = {
        'neighborhood_aggregation': {
            'epochs': epochs
        }
    }
    request_json = json.dumps(request_data)
    s.ask(0, node=nid, msg=request_json)


# query_node_feature(0)

# query_node_feature(1)

# query_khop_neighborhood(3, 1, 5)

# query_khop_neighborhood(3, 3, [2, 18, 32])

aggregate_neighborhood(0, 1)

aggregate_neighborhood(0, 1)
