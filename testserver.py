from xmlrpc.server import SimpleXMLRPCServer


class Worker:
    def handle_message(self, message):
        print(f"Received message: {message}")
        # 模拟处理请求所需的时间
        import time
        time.sleep(2)  # 假设处理请求需要2秒钟
        return f"Acknowledged: {message}"

def run_server():
    # 使用自定义的异步服务器
    worker = Worker()
    with SimpleXMLRPCServer(('localhost', 9000)) as server:
        print("Listening on port 9000...")
        server.register_instance(worker)
        
        # 运行服务器，处理请求
        server.serve_forever()

if __name__ == "__main__":
    run_server()
