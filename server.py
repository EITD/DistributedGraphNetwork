import time
from MySocket import MySocket

s = MySocket(1, 12346, client= True)

while True:
        if not s.message_get_queue.empty():
                start = time.time()
                client_socket, message = s.message_get_queue.get()
                end = time.time()
                print(end - start)
                
                start = time.time()
                s.message_send_queue.put((client_socket, message))
                end = time.time()
                print(end - start)

