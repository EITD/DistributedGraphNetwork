import socket
import threading
import queue

NUM_PARTITIONS = 4

class MySocket:
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    message_get_queue = queue.Queue()
    message_send_queue = queue.Queue()
    ask_reply_dict = dict()
    serverDict = {}
    
    def __init__(self, port, serverDict):
        host = socket.gethostbyname(socket.gethostname())
        print('host:', socket.gethostbyname(socket.gethostname()))
        print('port:', port)
        self.serverDict = serverDict
        
        self.server_socket.bind((host, port))
        self.server_socket.listen(5)
        
        send_thread = threading.Thread(target=self.send_back)
        send_thread.start()
        
        msg_thread = threading.Thread(target=self.print_message)
        msg_thread.start()
        
        server_thread = threading.Thread(target=self.handle_client)
        server_thread.start()

    def handle_client(self):
        while True:
            client_socket, _ = self.server_socket.accept()
            
            data = client_socket.recv(1024)
            
            self.message_get_queue.put((client_socket, data.decode()))
            
    def send_back(self):
        while True:
            if not self.message_send_queue.empty():
                client_socket, message = self.message_send_queue.get()
                client_socket.send(message.encode())

    def print_message(self):
        while True:
            if not self.message_get_queue.empty():
                client_socket, message = self.message_get_queue.get()
                print('from client socket:', client_socket)
                print('    get msg:', message)
                
                self.message_send_queue.put((client_socket, "get msg, send back res"))
    
    def ask(self, mid, node, msg):
        ask_thread = threading.Thread(target=self._ask, args=(mid, node, msg))
        ask_thread.start()

    def _ask(self, mid, node, msg):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(self.serverDict.get(node % NUM_PARTITIONS))
        client_socket.send(msg.encode())
        
        data = client_socket.recv(1024).decode()
        print('get reply', data)
        self.ask_reply_dict[mid] = data

        client_socket.close()

d = {1:("130.229.156.171",12345), 0:("130.229.156.171",12346), 2:("130.229.156.171",12347), 3:("130.229.156.171",12348)}

s = MySocket(port=12345, serverDict=d)

# client_socket, message = s.message_get_queue.get()

# s.message_send_queue.put((client_socket, "get msg, send back res"))