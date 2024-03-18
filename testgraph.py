from ConvertFile import ConvertFile
import networkx as nx


# graph = ConvertFile.toGraph(f"./data_small/neighbor_small.txt", " ")
graph = ConvertFile.toGraph(f"./data/neighbor.txt", " ")

# out_edges = graph.successors('4031')
# in_edges = graph.predecessors('4031')

# print(len(list(out_edges)), len(list(in_edges)))

if nx.is_directed_acyclic_graph(graph):
    print("Graph is a DAG.")
else:
    print("Graph is not a DAG.")

sources = [n for n, d in graph.out_degree() if d == 0]
print("source vertex:", sources)

reachable = set()
for source in sources:
    reachable.update(nx.ancestors(graph, source))
    reachable.add(source)

if len(reachable) == len(graph.nodes):
    print("Can reach all vertex in the graph.")
else:
    print(f"Can't reach all vertex in the graph.")
