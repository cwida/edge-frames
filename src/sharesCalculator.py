import csv
import sys
import itertools
from functools import reduce
from typing import List, Tuple, Dict
from string import ascii_lowercase

from math import sqrt, ceil


def workload_per_worker(edges: List[Tuple[str, str]], c: Dict[str, int]):
  w = 0
  for (a, b) in edges:
    size = c[a] * c[b]
    w += 1 / size

  return w


def workload_total(edges, c):
  w = workload_per_worker(edges, c)
  return w / numberOfWorkers(c)


def best_configuration(workers: int, edges: List[Tuple[str, str]], vertices: List[str]):
  """
  best configuration as defined in "From Theory to Practice":
  most even configuration with lowest workload
  """
  min_workload = 1.0
  best_conf = None
  for c_tuple in itertools.combinations_with_replacement(list(range(1, ceil(sqrt(workers)))), len(vertices)):
    if reduce(lambda x, y: x * y, c_tuple) <= workers:
      c = dict(zip(vertices, c_tuple))

      w = workload_total(edges, c)
      if w < min_workload:
        min_workload = w
        best_conf = c
      elif w == min_workload and (best_conf == None or max(c) < max(best_conf)):
        best_conf = c
  return best_conf


def numberOfWorkers(c):
  return reduce(lambda x, y: x * y, c.values())


def clique_pattern(num_vertices):
  vertices = list(map(lambda i: ascii_lowercase[i], range(num_vertices)))
  edges = []
  for v1 in vertices:
    for v2 in vertices:
      if v1 < v2:
        edges.append((v1, v2))

  return vertices, edges


def path_pattern(num_vertices):
  vertices = list(map(lambda i: ascii_lowercase[i], range(num_vertices)))
  edges = []
  for (v1, i) in enumerate(vertices):
    if i < len(vertices) - 1:
      edges.append((v1, vertices[i + 1]))

  return vertices, edges


def diamond_pattern():
  clique = clique_pattern(4)
  clique[1].remove(('a', 'd'))
  return clique


def house_pattern():
  clique = clique_pattern(5)
  clique[1].remove(('a', 'd'))
  clique[1].remove(('a', 'e'))
  return clique

cliquePatterns = list(map(lambda i: clique_pattern(i), range(3, 7)))
pathPatterns = list(map(lambda i: clique_pattern(i), range(2, 10)))

patterns = cliquePatterns + pathPatterns + [diamond_pattern()] + [house_pattern()]
print(patterns)
fieldnames = ['vertices', 'edges', 'workers', 'workers_used', 'config', 'max_percentage']
rows = []
workers = 64

for (v, e) in patterns:
    c = best_configuration(workers, e, v)
    rows.append((len(v), len(e), workers, numberOfWorkers(c), '; '.join(map(str, map(lambda v: c[v], v))), workload_per_worker(e, c)))

with open('output.csv', 'w') as f:
  writer = csv.writer(f)
  writer.writerow(fieldnames)

  writer.writerows(rows)


