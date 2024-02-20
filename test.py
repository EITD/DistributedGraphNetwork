import random
from ConvertFile import ConvertFile
import networkx as nx

request_data = {
        'khop_ask_phase': {
                'nid': '1',
                'k': 2,
                'deltas': [1,2,3]
                }
        }

print(type(request_data['khop_ask_phase']['deltas']))
# for i in random_neighbors:
#     print(i, type(i))