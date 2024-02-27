from concurrent.futures import ThreadPoolExecutor
import xmlrpc.client

def async_call(proxy, message):
    response = proxy.handle_message(message)
    return response

def main():
    message = "Hello, Server!"
    with ThreadPoolExecutor() as executor:
        # 创建XML-RPC服务器代理
        proxy = xmlrpc.client.ServerProxy("http://localhost:9000")
        # 异步执行RPC调用
        future = executor.submit(async_call, proxy, message)
        # 这里可以执行其他任务，不会被RPC调用阻塞
        print("Doing other tasks while waiting for RPC response...")
        
        # 等待RPC调用完成并获取结果
        response = future.result()
        print(f"RPC response: {response}")

if __name__ == "__main__":
    main()
