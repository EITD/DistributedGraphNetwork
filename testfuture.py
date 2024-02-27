from concurrent.futures import ThreadPoolExecutor, as_completed
import json

class YourClass:
    def __init__(self):
        self.epoch = {'node1': 1, 'node2': 2, 'node3': 3}  # 示例epoch数据

    def send_message(self, node, request_json):
        # 这里是发送消息的逻辑，并模拟返回一些JSON数据
        # 注意：根据实际情况调整此处逻辑
        request_data = {
                'node_feature' : 1, # feature
                'neighborhood' : ["1", "2"] # [nid, nid, nid...]
            }
        return json.dumps(request_data)

def main():
    your_class = YourClass()
    random_neighbors = ["node1", "node2", "node3"]  # 示例邻居节点列表
    k = 3  # 假设的某个条件值
    sums = 0
    node_neighbors_set = set()
    deltas = [0.1, 0.2, 0.3]  # 示例delta列表

    with ThreadPoolExecutor(max_workers=len(random_neighbors)) as executor:
        future_to_node = {}
        for j, node in enumerate(random_neighbors):
            if j < k - 1:
                request_data = {
                    'feature_and_neighborhood': {
                        'nid': node,
                        'delta': deltas[j + 1],
                        'epoch': your_class.epoch[node]
                    }
                }
            else:
                request_data = {
                    'node_feature': node,
                    'epoch': your_class.epoch[node]
                }
            future = executor.submit(your_class.send_message, node, json.dumps(request_data))
            future_to_node[future] = node

        for future in as_completed(future_to_node):
            node = future_to_node[future]
            try:
                response = future.result()
                result = json.loads(response)
                if random_neighbors.index(node) < k - 1:
                    node_neighbors_set.update(result['neighborhood'])
                sums += result['node_feature']
            except Exception as exc:
                print(f"Node {node} generated an exception: {exc}")

    # 所有结果处理完毕后，进行后续操作
    print(f"Sum of node features: {sums}")
    print(f"Updated node neighbors set: {node_neighbors_set}")

if __name__ == "__main__":
    main()
