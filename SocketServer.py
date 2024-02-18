import socket
import threading
import queue
import time

def handle_client(server_socket, message_queue):
    while True:
        client_socket, addr = server_socket.accept()
        
        addr = client_socket.getpeername()

        # 接收客户端数据
        data = client_socket.recv(1024)
        
        # 将接收到的消息放入消息队列
        message_queue.put((addr, data.decode()))
        
        # 关闭连接
        client_socket.close()

def print_time():
    while True:
        # 每隔一秒打印当前时间
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print("当前时间：", current_time)
        
        time.sleep(1)

def print_message(message_queue):
    while True:
        if not message_queue.empty():
            addr, message = message_queue.get()
            print('from addr:', addr)
            print('    get msg:', message)

def main():
    # 创建 socket 对象
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 获取本地主机名和端口号
    host = socket.gethostname()
    port = 12345

    # 绑定端口号
    server_socket.bind((host, port))

    # 设置最大连接数，超过后排队
    server_socket.listen(5)
    
    # 创建一个消息队列
    message_queue = queue.Queue()

    # 创建一个线程来定期打印时间
    time_thread = threading.Thread(target=print_time)
    time_thread.start()
    
    msg_thread = threading.Thread(target=print_message, args=(message_queue,))
    msg_thread.start()
    
    # 创建一个线程来处理客户端请求
    server_thread = threading.Thread(target=handle_client, args=(server_socket, message_queue))
    server_thread.start()

if __name__ == "__main__":
    main()
