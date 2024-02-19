import socket
import threading
import queue

class SocketServer:
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    message_get_queue = queue.Queue()
    message_send_queue = queue.Queue()
    
    def __init__(self):
        host = socket.gethostbyname(socket.gethostname())
        print('host:', socket.gethostbyname(socket.gethostname()))
        port = 12345
        print('port:', port)
        
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

# s = SocketServer()

# client_socket, message = s.message_get_queue.get()

# s.message_send_queue.put((client_socket, "get msg, send back res"))