import random
import signal
import socket
import sys
import threading
import queue
import time

# NUM_PARTITIONS = 4
# d = {0:("130.229.150.211",12346), 1:("130.229.150.211",12346), 2:("130.229.150.211",12347), 3:("130.229.150.211",12348)}

class MySocket:
    message_get_queue = queue.Queue()
    message_send_queue_dict = dict()
    ask_reply_dict = dict()
    serverDict = {}
    NUM_PARTITIONS = None
    client = False
    alive = True
    
    def __init__(self, myNode, portList, NUM_PARTITIONS = 4, client = False):
        host = socket.gethostbyname(socket.gethostname())
        print('host:', host)
        print('port:', portList)
        self.NUM_PARTITIONS = NUM_PARTITIONS
        self.client = client
        
        testIp = host
        if not client:
            self.serverDict = {0:[(testIp,12345 + (i*4)) for i in range(3000)], 1:[(testIp,12346 + (i*4)) for i in range(3000)], 
                                2:[(testIp,12347 + (i*4)) for i in range(3000)], 3:[(testIp,12348 + (i*4)) for i in range(3000)]}
            
        if client:
            self.serverDict = {-1:[(testIp,12346)]}
        
        # if not client:
        #     self.serverDict[myNode] = (host, port)
        
        # if client:
        #     print("# add any Server")
        #     n = -1
        #     ip = input("Enter other server ip address:")
        #     p = input("Enter other server port:")
        #     self.serverDict[int(n)] = (ip, int(p))
        
        # while len(self.serverDict) < NUM_PARTITIONS and not client:
        #     print("# have", len(self.serverDict), "Server")
        #     n = input("Enter other server partation:")
        #     ip = input("Enter other server ip address:")
        #     p = input("Enter other server port:")
        #     self.serverDict[int(n)] = (ip, int(p))
        for i in portList:
            socket_thread = threading.Thread(target=self.new_socket, args=(host,i))
            socket_thread.start()
            
        # signal.signal(signal.SIGINT, self.signal_handler)
    
    def new_socket(self, host, port):
        self.message_send_queue_dict[port] = queue.Queue()
        while True:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((host, port))
            server_socket.listen(100)
        
            self.handle_client(server_socket, port)
        
            self.send_back(port)
            # send_thread = threading.Thread(target=self.send_back)
            # send_thread.start()
            
            # msg_thread = threading.Thread(target=self.print_message)
            # msg_thread.start()
            
            # server_thread = threading.Thread(target=self.handle_client, args=(server_socket,))
            # server_thread.start()
            
            server_socket.close()
    
    # def signal_handler(self, sig, frame):
    #     print('Exiting...')
    #     self.alive = False
    #     self.server_socket.close()
    #     sys.exit(0)

    def handle_client(self, server_socket, port):
        # while self.alive:
            client_socket, _ = server_socket.accept()
            
            data = client_socket.recv(102400)
            print('get msg:', data)
            
            self.message_get_queue.put((port, client_socket, data.decode()))
            
    #         client_thread = threading.Thread(target=self.handle_client_connection, args=(client_socket,))
    #         client_thread.start()

    # def handle_client_connection(self, client_socket):
    #     data = client_socket.recv(102400)
    #     print('get msg:', data)
        
    #     self.message_get_queue.put((client_socket, data.decode()))

    def send_back(self, port):
        while self.alive:
            if not self.message_send_queue_dict[port].empty():
                client_socket, message = self.message_send_queue_dict[port].get()
                print('send out:', message)
                
                client_socket.send(message.encode())
                
    #             send_thread = threading.Thread(target=self._send_message, args=(client_socket, message))
    #             send_thread.start()

    # def _send_message(self, client_socket, message):
    #     client_socket.send(message.encode())

    # def print_message(self):
    #     while self.alive:
    #         if not self.message_get_queue.empty():
    #             client_socket, message = self.message_get_queue.get()
    #             # print('from client socket:', client_socket)
    #             print('    get msg:', message)
                
    #             self.message_send_queue.put((client_socket, "get msg, send back res" + message))
    
    def ask(self, mid, node, msg):
        ask_thread = threading.Thread(target=self._ask, args=(mid, node, msg))
        ask_thread.start()

    def _ask(self, mid, node, msg):
        while self.alive:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                if self.client:
                    client_socket.connect(self.serverDict[-1][0])
                else:
                    client_socket.connect(self.serverDict.get(int(node) % self.NUM_PARTITIONS)[random.randint(0, 2999)])
                client_socket.send(msg.encode())
                print('ask:', msg)
                if self.client:
                    start = time.time()
                    print(mid, 'start at:', time.localtime(start))
                
                data = client_socket.recv(102400).decode()
                if self.client:
                    end = time.time()
                    print(mid, 'end at:', time.localtime(end))
                    duration = end - start
                    print(mid, 'duration:', duration)
                if data != "":
                    print('get reply:', data)
                    self.ask_reply_dict[mid] = data

                client_socket.close()
                break
            except (ConnectionRefusedError):
                print('error')
                client_socket.close()
                continue
            # except (BrokenPipeError):
            #     print('break')
            #     client_socket.close()
            #     break

# s = MySocket(myNode=1, port=12345, serverDict=d)

# for i in range(10):
#     s.ask(i, node=i%NUM_PARTITIONS, msg="your_message")

# client_socket, message = s.message_get_queue.get()

# s.message_send_queue.put((client_socket, "get msg, send back res"))