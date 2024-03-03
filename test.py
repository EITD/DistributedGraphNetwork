import json
from xmlrpc.server import XMLRPCDocGenerator
from MySocket import MySocket
import time

request_data = {
        'khop_ask_phase': {
                'nid': '1',
                'k': 2,
                'deltas': [1,2,3]
                }
        }

s = MySocket(None, 10000, client= True)

################################

start = time.time()

data = json.dumps(request_data)

end = time.time()

print('dumps', end - start) # dumps 0.0

################################

start_ = time.time()

s.ask(0, 0, data)

end = time.time()

print('send ask', end - start_) # send ask 0.030954837799072266

# 0 duration: 0.09431672096252441

################################

start = time.time()

while True:
        if 0 in s.ask_reply_dict:
                (s,e) = s.ask_reply_dict.pop(0)
                break

end_ = time.time()

print(s - start_, e - s, end_ - e) # 0.13992094993591309 0.12463498115539551 0.1725156307220459

print('send -> get reply', end_ - start_) # send -> get reply 0.3279869556427002

################################

# start = time.time()

# resp = json.loads(request_json)

# end = time.time()

# print('loads', end - start) # loads 0.0

def send_message(message):
    start_ = time.time()
    print("Send message: ", message)
    while True:
        try:
            proxy = XMLRPCDocGenerator.client.ServerProxy(f"http://localhost:12345")
            response = proxy.handle_msg(message)
            print("Received response message: ", response)
            end_ = time.time()
            print('send -> get reply', end_ - start_)
            print()
            return response
        except Exception as e:
            # print(e)
            # print("!!!!!!RPC exception!!!!!!, retrying...")
            with open('error', 'w') as f: 
                f.write(message)
            continue

        