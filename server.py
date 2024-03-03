from socketserver import ThreadingMixIn
import time
from xmlrpc.server import SimpleXMLRPCRequestHandler, SimpleXMLRPCServer
from MySocket import MySocket

class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass

class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2',)

class Worker:
        def handle_msg(self, message):
                print("Received handle message: ", message)
                print('reply:', message)
                return message

def run_server(port):
        server = ThreadedXMLRPCServer(('localhost', port), requestHandler=RequestHandler)
        worker = Worker()
        server.register_instance(worker)  
        server.serve_forever()

run_server(12345)


########################################################################


# s = MySocket(1, 12346, client= True)

# while True:
#         if not s.message_get_queue.empty():
#                 start = time.time()
#                 client_socket, message = s.message_get_queue.get()
#                 end = time.time()
#                 print(end - start)
                
#                 start = time.time()
#                 s.message_send_queue.put((client_socket, message))
#                 end = time.time()
#                 print(end - start)