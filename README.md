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

## How To Run

- Start Client: 

  `python3 client.py`

- Start Worker: 

  `kernprof -l worker.py {worker_id}`

- Benchmark: 
  
  For each worker, stop and `python3 parse_lprof_to_file.py {worker_id}`