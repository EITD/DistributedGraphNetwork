import random


neighbor_features = [['v1f1', 'v2f2', 'v3f3'], ['v1v4f4', 'v2v4f4', 'v3v5f5'], ['v1v4v6f6', 'v2v4v6f6', 'v3v5v7f7']]
k = 3
deltas = [2, 2, 2]
out_edges_list = ['1','2','3']



node_neighbors_set = set(['v' + i for i in out_edges_list])

sums = 0

for j in range(k): # [2,3,2]
        random_neighbors = random.sample(list(node_neighbors_set), deltas[j] if len(node_neighbors_set) > deltas[j] else len(node_neighbors_set))
        node_neighbors_set = set()
        
        print(random_neighbors)
        temp = random_neighbors.copy()

        for feature in neighbor_features[j]:
                v = feature[feature.rfind('v'):feature.rfind('f')]
                if v in temp:
                        sums += int(feature[feature.rfind('f') + 1:])
                        temp.remove(v)
        
        if j < k - 1:
                for feature in neighbor_features[j+1]:
                        if 'v'+feature.split('v')[-2] in random_neighbors:
                                node_neighbors_set.add(feature[feature.rfind('v'):feature.rfind('f')])

print(sums)