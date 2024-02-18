import socket

def query_node_feature(port, nid):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('localhost', port))
        s.sendall(nid.encode())
        feature = s.recv(1024)
        print(f"Node {nid} feature: {feature.decode()}")

# Example query
# This should match the port of the worker responsible for the node being queried
query_node_feature(10000, '16')  # Querying worker 0 for node 1's feature
