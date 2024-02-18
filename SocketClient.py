import socket

# 创建 socket 对象
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# 获取服务器主机名和端口号
host = socket.gethostname()  # 服务器主机名
port = 12345  # 服务器端口号

# 连接服务，指定主机和端口
client_socket.connect((host, port))

# 发送数据给服务器
client_socket.send('你好，我是客户端！'.encode())

# 接收服务器返回的消息
data = client_socket.recv(1024)
print('接收到的消息：', data.decode())

# 关闭连接
client_socket.close()
