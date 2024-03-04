# ID2203 VT24 Course Project - Distributed Graph Neural Networks Training

In this repository you will find a detailed description of the project course _Distributed Graph Neural Networks Training_.

## Dataset

In addition, you can download the graph dataset to use for your implementation directly from this repository (under `data/`).

We have obtained the files by pre-processing the the SNAP [Social circles: Facebook dataset](https://snap.stanford.edu/data/ego-Facebook.html).
The `partition_*.txt`files contain lists of edges in format `source_id destination_id`, while `node_features.txt` contains the mapping between node IDs and their features.

We provide a pre-processed version of the data which has been partitioned by node ID. The data is ready to be used projects spawning four workers. If you would like to experiment with different number of partitions, you can download the original dataset and run the the `process_data.py` script after updating the number of partitions needed.

## Grading Checklist

Mandatory administratve tasks:

- [x] Your project uses GitHub for the collaboration.
  - The commits are balanced among group members.
  - Your project is private (for the duration of the course VT24; you can make it public later).
- [ ] Your project's README contains the following:
  - A summary of what you have implemented.
  - Instructions on how to run, test, and evaluate the project.
  - A statement of contributions: who has done what.
- [ ] Your solution is documented.

Bonus tasks:

- [ ] Your solution has an appropriate performance evaluation.

## Summary of what implemented

We have implemented a distributed graph store system aimed at managing graph-based data across multiple workers. Here's a brief summary of the implemented features:

1. Basic Infrastructure: We designed a distributed system where graph partitions are distributed over 4 workers. Each worker runs as a separate process, supporting basic `Get(nid)` operations to retrieve node features by their ID. 

2. Neighborhood Queries: We implemented `Khop_Neighborhood(nid, k, (δ0,...,δk-1))` operations to support k-hop neighborhood queries, which calculate the aggregated features of a node's neighborhood up to k hops away.

3. Message-Passing-Based Neighborhood Aggregation: To simulate the neighborhood aggregation process of GNN training, we introduced a `Train(epochs)` operation. This iterates over nodes to compute new weights based on k-hop neighborhood aggregations, with each node storing a history of aggregation weights. `Future` mechanism was considered to manage the completion of epochs and support synchronous training operations.

4. Marker-Based Asynchronous Training: We explored asynchronous training through iterative k-hop neighborhood aggregations, maintaining a `epoch_dict`(marker) recording each node's epoch to keep causality across aggregation steps and epochs. The marker should be disseminated across workers to signal readiness for next epoch's iteration computations.

5. Performance Evaluation: We provided two versions of solutions based on message-passing protocol: Socket and RPC. We conducted benchmarks to evaluate and compare the performance of each solution, focusing on both the time complexity of individual lines of code within our functions and memory usage of each functions. 

## Run

- Start Client: 

  `python3 client.py`

- Start Worker: 

  `python3 worker.py {worker_id}`

## Test

In the client, we provided test cases for each feature. 

We also provided two test functions to verify the accuracy of train result in depth and breadth:

- test_mult_epochs(response, epoch)
- test_all_neighbors(response, k)

> Use node_features_dummy.txt and change default node feature in the code.

## Evaluation

### Line(Time) Analysis

- Start Worker: 

  `kernprof -l worker.py {worker_id}`

- Generate Line Analysis File: 

  For each worker, stop and execute `python3 parse_lprof_to_file.py {worker_id}` 

### Memory Analysis

- Start Worker:

  `mprof run --output mprofile{worker_id}.dat worker.py {worker_id}`

- Generate Memory Analysis Graph:

  `mprof plot mprofile{worker_id}.dat --output memory_results{worker_id}.png`

  > Train result may be too big to transfer into graph. Instead, execute `python3 parse_dat_to_graph.py` to get a general memory usage graph.

### Performance

> Not very clear, but detail.

- Start Worker:

  `python3 -m cProfile -o performance_results{worker_id}.prof worker.py {worker_id}`

- Generate Performance Report:

  For each worker, stop and execute `snakeviz performance_results{worker_id}.prof`
  