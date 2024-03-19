import socket
import struct
import threading
import time

# client send time:  1710799351.11728
# server receive time:  1710799351.11736
# server send back msg:  hi 1710799351.117373
# client get msg: hi 1710799351.117407

def send(msg):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
    client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        client_socket.connect(('localhost', 12345))

        print("client send time: ", time.time())
        client_socket.send(msg.encode())

        data = client_socket.recv(102400).decode()
        print('client get msg:', data, time.time())

        client_socket.shutdown(socket.SHUT_WR)
        client_socket.close()

    except ConnectionRefusedError:
        client_socket.close()
    except OSError:
        client_socket.close()


def receive():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
    server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    server_socket.bind(('localhost', 12345))
    server_socket.listen(1500)

    client_socket, _ = server_socket.accept()
    data = client_socket.recv(102400).decode()
    print("server receive time: ", time.time())

    client_socket.send(data.encode())
    print("server send back msg: ", data, time.time())
    # threading.Thread(target=handle_msg, args=(client_socket, )).start()

# def handle_msg(client_socket):
#     data = client_socket.recv(102400).decode()
#     print("server receive time: ", time.time())

#     client_socket.send(data.encode())
#     print("server send back msg: ", data, time.time())

if __name__ == "__main__":
    threading.Thread(target=receive).start()
    threading.Thread(target=send, args=("hi", )).start()

