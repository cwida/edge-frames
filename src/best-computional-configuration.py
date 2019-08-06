
# Script to run the best configuration calculator
# Usage:
# best-configuration.py #workers <edges>
# Example python best-configurations.py 64 a b a c b c

import operator as op
from functools import reduce
from typing import List, Tuple, Dict

import sys
from collections import deque
from math import sqrt, ceil


def workload_per_worker(pattern, config: Dict[str, int]):
  w = 0
  v, e = pattern

  for (a, b) in e:
    size = config[a] * config[b]
    p_assign = 1 / size

    w += p_assign

  return w


def best_configuration(workers: int, edges: List[Tuple[str, str]], vertices: List[str]):
  """
  best configuration as defined in "From Theory to Practice":
  most even configuration with lowest workload
  """
  min_workload = 1.0 * len(edges)
  best_conf = dict(zip(vertices, [1 for v in vertices]))

  visited = set()
  toVisit = deque()
  toVisit.append(tuple([1 for _ in vertices]))

  while (len(toVisit) > 0):
    c_tuple = toVisit.pop()
    c = dict(zip(vertices, c_tuple))

    w = workload_per_worker((vertices, edges), c)
    if w < min_workload:
      min_workload = w
      best_conf = c
    elif w == min_workload and (best_conf is None or max(c.values()) < max(best_conf.values())):
      best_conf = c

    for i, d in enumerate(c_tuple):
      new_dim_sizes = (c_tuple[0:i] +
                       tuple([c_tuple[i] + 1]) +
                       c_tuple[i + 1:])
      if (reduce(op.mul, new_dim_sizes) <= workers
          and new_dim_sizes not in visited
          and (workers <= 64 or max(new_dim_sizes) < ceil(sqrt(workers)))):  # Optimization for many workers, takes to long otherwise.
        toVisit.append(new_dim_sizes)
  return best_conf

workers = int(sys.argv[1])
edges = sys.argv[2:]

edgeTuples = []
vertices = set()

# print(workers, file=sys.stderr)
# print(edges, file=sys.stderr)

for (i, src) in enumerate(edges):
  if i % 2 == 0:
    edgeTuples.append((src, edges[i + 1]))
  vertices.add(src)

configuration = best_configuration(workers, edgeTuples, vertices)

print(configuration)



