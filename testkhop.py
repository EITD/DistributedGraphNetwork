
import random

def test():
    neighbor_features = [["v2f1", "v4f1", "v7f1", "v16f0"], ["v2v8f1", "v2v10f1", "v4v7f1", "v4v9f1", "v4v13f0", "v7v8f1", "v7v12f1"]]
    sums = 1    
    node_neighbors_set = set(["2", "4", "7", "16"])
    k = 2
    deltas = [20, 20]
    # prefix = ""
    for j in range(k): # [2,3,2]
        random_neighbors = random.sample(list(node_neighbors_set), deltas[j] if len(node_neighbors_set) > deltas[j] else len(node_neighbors_set))
        node_neighbors_set = set()
        temp_set = set()

        # TODO: 找到有j个v的字符串并且拿到该字符串v后的所有数字表示的feature
        for vertex in random_neighbors:
            for feature in neighbor_features[j]:
                if feature.startswith("v" + vertex):
                    start_index = feature.find("f")
                    sub_text = feature[start_index + 1:] 
                    sums += int(sub_text)
                    # print(sub_text)
            if j < k - 1:
                for v in neighbor_features[j + 1]:
                    if v.startswith("v" + vertex):
                        # start_index = len("v" + vertex)
                        # print(v)
                        end_index = v.find("f")
                        sub_text = v[1:end_index]
                        temp = v[v.rfind("v") + 1 : end_index]
                        print(temp)
                        if temp not in temp_set:
                            node_neighbors_set.add(sub_text) # [2v8 7v8]
                            temp_set.add(temp)
                        # print(sub_text)

    return sums

print(test())