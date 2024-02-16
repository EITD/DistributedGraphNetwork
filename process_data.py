import pandas as pd
import random

NUM_PARTITIONS = 4

def hash_partitioning(adjacency_list, num_partitions):
  """Partitions a graph into the specified number of partitions using hash partitioning.

  Args:
    adjacency_list: A dictionary representing the graph as an adjacency list.
    num_partitions: The number of partitions to create.

  Returns:
    A dictionary where the keys are partition indices and the values are lists of edges in that partition.
  """

  # Create an empty dictionary to store the edges in each partition
  partitions = {i: [] for i in range(num_partitions)}

  # Hash each node to a partition using a simple modulo operation
  for node, neighbors in adjacency_list.items():
    partition_index = node % num_partitions
    for neighbor in neighbors:
      partitions[partition_index].append((node, neighbor))

  return partitions

# Assuming the file is named "data.txt" and located in the same directory as your script
filename = "./facebook_combined.txt"
node_edges = {}

with open(filename, "r") as file:
    for line in file:
      # Remove spaces and split the line into two integers
      nodes = map(int, line.strip().split())

      # Or access individual numbers:
      src, dst = nodes

      # Further process numbers1 and number2 as needed
      if src not in node_edges:
        node_edges[src] = set()
      node_edges[src].add(dst)

partitions = hash_partitioning(node_edges, NUM_PARTITIONS)

# Write edges from each partition to a separate file
for partition_index, edges in partitions.items():
  with open(f"partition_{partition_index}.txt", "w") as file:
    for source, destination in edges:
      file.write(f"{source} {destination}\n")

print("Edges written to partition files.")

node_features = {}
for node, neighbors in node_edges.items():
	if node not in node_features:
		node_features[node] = random.randint(0, 100)

# Write node features to a file
with open("node_features.txt", "w") as file:
  for node, feature in node_features.items():
    file.write(f"{node} {feature}\n")

print("Node features written to node_features.txt")