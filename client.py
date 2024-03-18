import json
import time
import socket
import concurrent.futures

def ask(msg, i=0, asy=False):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    try:
        if asy:
            client_socket.connect(('localhost', 10000 + i))
        else:
            client_socket.connect(('localhost', 12345 + i))
    except Exception as e:
        print(e)
    # client_socket.connect(('130.229.166.49',12345))
    
    start = time.time()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)), end=' ')
    
    client_socket.send(msg.encode())
    print('ask:', msg)
    
    data = client_socket.recv(102400).decode()
    
    end = time.time()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)), end=' ')
    
    print('get reply:', data)
    with open('check', 'a') as f: 
        f.write('\n\n\n\n' + str(json.loads(data))) 

    client_socket.shutdown(socket.SHUT_WR)
    client_socket.close()
    
    print('duration:', end - start)
    
    return data

def query_node_feature(nid):
    request_data = {
        'node_feature': str(nid)
    }
    request_json = json.dumps(request_data)
    return ask(request_json)

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
    return ask(request_json)
    
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
    return ask(request_json)

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
    return ask(request_json)

def train_asynchronize_marker(epochs):
    ask_worker_list = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for i in range(4):
            future = executor.submit(ask, f"epoch_{epochs}", i, True)
            ask_worker_list.append(future)
    concurrent.futures.wait(ask_worker_list)
    return "ok"


# response = query_node_feature(0)

# response = query_node_feature(1)

# response = query_khop_neighborhood(4031, 2, [5000, 5000**2])

# response = query_khop_neighborhood(3, 3, [2, 18, 32])

# response = query_khop_neighborhood(0, 1, 5000)


# response = train_synchronize(1, 1, 5000)

# response = train_synchronize(1, 2, [5000, 5000**2])

# response = train_synchronize(2, 1, 5000)

# response = train_synchronize(2, 2, [5000, 5000])


# response = train_asynchronize(1, 1, 5000)

# response = train_asynchronize(1, 2, [5000, 5000**2])

# response = train_asynchronize(2, 1, 5000)

# response = train_asynchronize(2, 2, [5000, 5000**2])


response = train_asynchronize_marker(1)
# response = train_asynchronize_marker(2)
# response = train_asynchronize_marker(3)

# response = train_synchronize(1, 2, [20, 20])
# response = train_synchronize(2, 1, 20)
# response = train_synchronize(3, 1, 20)