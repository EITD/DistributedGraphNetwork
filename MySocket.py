import signal
import socket
import sys
import threading
import queue
import time

# NUM_PARTITIONS = 4
# d = {0:("130.229.150.211",12346), 1:("130.229.150.211",12346), 2:("130.229.150.211",12347), 3:("130.229.150.211",12348)}

class ConnectionPool:
    def __init__(self, size):
        self.connections = queue.Queue(maxsize=size)
        for _ in range(size):
            self.connections.put(self.create_new_conn())

    def get_conn(self):
        return self.connections.get()

    def release_conn(self, conn):
        self.connections.put(conn)

    def create_new_conn(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        sndbuf_size = 1024 * 1024 * 5  # 1MB
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf_size)

        rcvbuf_size = 1024 * 1024 * 5 # 1MB
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf_size)
        return conn



class MySocket:
    
    conn_pool = ConnectionPool(10000)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    message_get_queue = queue.Queue()
    message_send_queue = queue.Queue()
    ask_reply_dict = dict()
    serverDict = {}
    NUM_PARTITIONS = None
    client = False
    alive = True
    
    def __init__(self, myNode, port, NUM_PARTITIONS = 4, client = False):
        # host = socket.gethostbyname(socket.gethostname())
        host = 'localhost'
        print('host:', host)
        print('port:', port)
        self.NUM_PARTITIONS = NUM_PARTITIONS
        self.client = client
        
        testIp = host
        if not client:
            self.serverDict = {0:(testIp,12345), 1:(testIp,12346), 2:(testIp,12347), 3:(testIp,12348)}
            
        if client:
            self.serverDict = {-1:(testIp,12345)}
        
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
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        sndbuf_size = 1024 * 1024 * 5 # 1MB
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf_size)

        rcvbuf_size = 1024 * 1024 * 5 # 1MB
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf_size)
        self.server_socket.bind((host, port))
        self.server_socket.listen(10000)
        
        send_thread = threading.Thread(target=self.send_back)
        send_thread.start()
        
        # msg_thread = threading.Thread(target=self.print_message)
        # msg_thread.start()
        
        server_thread = threading.Thread(target=self.handle_client)
        server_thread.start()
        
        signal.signal(signal.SIGINT, self.signal_handler)
    
    def signal_handler(self, sig, frame):
        print('Exiting...')
        self.alive = False
        self.server_socket.close()
        sys.exit(0)

    def handle_client(self):
        while self.alive:
            client_socket, _ = self.server_socket.accept()
            
            client_thread = threading.Thread(target=self.handle_client_connection, args=(client_socket,))
            client_thread.start()

    def handle_client_connection(self, client_socket):
        data = client_socket.recv(20480)
        print('get msg:', data)
        
        if b'__TELL__' not in data:
            self.message_get_queue.put((client_socket, data.decode()))
        else:
            self.message_get_queue.put((client_socket, data.replace(b'__TELL__', b'', 1).decode()))
            # client_socket.send('ok'.encode())
            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()

    def send_back(self):
        while self.alive:
            if not self.message_send_queue.empty():
                client_socket, message = self.message_send_queue.get()
                print('send out:', message)
                
                # client_socket.send(message.encode())
                
                send_thread = threading.Thread(target=self._send_message, args=(client_socket, message))
                send_thread.start()

    def _send_message(self, client_socket, message):
        client_socket.send(message.encode())

    # def print_message(self):
    #     while self.alive:
    #         if not self.message_get_queue.empty():
    #             client_socket, message = self.message_get_queue.get()
    #             # print('from client socket:', client_socket)
    #             print('    get msg:', message)
                
    #             self.message_send_queue.put((client_socket, "get msg, send back res" + message))
    
    def tell(self, node, msg):
        tell_thread = threading.Thread(target=self._tell, args=(node, msg))
        tell_thread.start()
    
    def _tell(self, node, msg):
        while self.alive:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            try:
                if self.client:
                    client_socket.connect(self.serverDict[-1])
                else:
                    client_socket.connect(self.serverDict.get(int(node) % self.NUM_PARTITIONS))
                client_socket.send(b'__TELL__'+msg.encode())
                print('tell:', msg)
                
                client_socket.shutdown(socket.SHUT_RDWR)
                client_socket.close()
                break
            except (ConnectionRefusedError):
                print('tell error')
                client_socket.shutdown(socket.SHUT_RDWR)
                client_socket.close()
                continue
    
    def ask(self, mid, node, msg):
        ask_thread = threading.Thread(target=self._ask, args=(mid, node, msg))
        ask_thread.start()

    def _ask(self, mid, node, msg):
        while self.alive:
            client_socket = self.conn_pool.get_conn()
            try:
                if self.client:
                    client_socket.connect(self.serverDict[-1])
                else:
                    client_socket.connect(self.serverDict.get(int(node) % self.NUM_PARTITIONS))
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
                else:
                    print('get reply:', data)
                self.ask_reply_dict[mid] = data
                # self.ask_reply_dict[mid] = (start, end)

                self.conn_pool.release_conn(client_socket)
                # client_socket.shutdown(socket.SHUT_RDWR)
                # client_socket.close()
                break
            except (ConnectionRefusedError):
                print('ask error')
                self.conn_pool.release_conn(client_socket)
                # client_socket.shutdown(socket.SHUT_RDWR)
                # client_socket.close()
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