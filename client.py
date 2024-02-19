import socket
import json

def query_node_feature(port, nid, k, deltas):
    request_data = {
        'nid': nid,
        'k': k,
        'deltas': deltas
    }

    request_json = json.dumps(request_data)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('localhost', port))
        s.sendall(request_json.encode())
        response = s.recv(1024)
        print(f"Response: {response.decode()}")

# Example query
# This should match the port of the worker responsible for the node being queried
query_node_feature(10000, '0', 2, [2, 2])  # Querying worker 0 for node 1's feature
