from ConvertFile import ConvertFile


graph = ConvertFile.toGraph(f"./data/neighbor.txt", " ")

out_edges = graph.successors('4031')
in_edges = graph.predecessors('4031')

print(len(list(out_edges)), len(list(in_edges)))