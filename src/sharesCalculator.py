import csv
import itertools
from functools import reduce
from typing import List, Tuple, Dict
from string import ascii_lowercase
import operator as op

# from poibin.poibin import PoiBin

from collections import deque
from math import sqrt, ceil, isclose


def workload_per_worker(pattern, config: Dict[str, int]):
  w = 0
  v, e = pattern

  p_unique = 1
  for (a, b) in e:
    size = config[a] * config[b]
    p_assign = 1 / size

    p_unique_assign = p_assign * p_unique

    p_unique = p_unique * (1 - p_assign)

    w += p_unique_assign

  # probabilities = [1 / (config[a] * config[b]) for (a, b) in e]
  # poison_binominial = PoiBin(probabilities)
  #
  # po_bi = 1 - (poison_binominial.pmf([0])[0])
  # assert(isclose(w, po_bi))

  return w


def workload_total(pattern, c):
  w = workload_per_worker(pattern, c)
  return w / number_of_workers(c)


def best_configuration(workers: int, edges: List[Tuple[str, str]], vertices: List[str]):
  """
  best configuration as defined in "From Theory to Practice":
  most even configuration with lowest workload
  """
  min_workload = 1.0
  best_conf = dict(zip(vertices, [0 for v in vertices]))

  visited = set()
  toVisit = deque()
  toVisit.append(tuple([1 for _ in vertices]))

  while (len(toVisit) > 0):
    c_tuple = toVisit.pop()
    c = dict(zip(vertices, c_tuple))

    w = workload_total((vertices, edges), c)
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


def number_of_workers(config):
  return reduce(lambda x, y: x * y, config.values())


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
  for (i, v1) in enumerate(vertices):
    if i < len(vertices) - 1:
      edges.append((v1, vertices[i + 1]))

  return vertices, edges


def circle_pattern(num_vertices):
  v, e = path_pattern(num_vertices)
  e.append((v[0], v[-1]))
  return v, e


def two_rings_pattern():
  v, e = clique_pattern(3)
  v += 'z'
  e += [('a', 'z'), ('b', 'z')]
  return v, e


def diamond_pattern():
  clique = clique_pattern(4)
  clique[1].remove(('a', 'd'))
  return clique


def house_pattern():
  clique = clique_pattern(5)
  clique[1].remove(('a', 'd'))
  clique[1].remove(('a', 'e'))
  return clique


def write_replication_file():
  """
  Produces a table that shows how many percent of the E relationship is hold at each node for the optimal
  configurations, given a fixed number of workers. Optimality is defined as detailed in Chu et al. 2015.
  :return:
  """
  clique_patterns = list(map(lambda i: clique_pattern(i), range(3, 6)))
  path_patterns = list(map(lambda i: clique_pattern(i), range(2, 6)))
  patterns = clique_patterns + path_patterns + [diamond_pattern()] + [house_pattern()]
  field_names = ['vertices', 'edges', 'workers', 'workers_used', 'config', 'max_percentage']
  rows = []
  workers = [64, 256]

  with open('output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(field_names)
    for (v, e) in patterns:
      for w in workers:
        c = best_configuration(w, e, v)
        writer.writerow((len(v), len(e), w, number_of_workers(c), '; '.join(map(str, map(lambda v: c[v], v))), workload_per_worker((v, e), c)))
        f.flush()


def check_against_paper_results():
  # Size of edge relationship in paper (twitter, follower -> followee)
  e_size = 1114289
  workers = 64

  # Patterns of the paper
  patterns = [clique_pattern(3)] + [clique_pattern(4)] + [circle_pattern(4)] + [two_rings_pattern()]

  # Number of tuples shuffled in millions from the paper, same order as patterns
  expected_tuples_to_shuffle = [13, 24, 35, 17]

  calculated_tuple_shuffles = []
  for (v, e) in patterns:
    config = best_configuration(workers, e, v)
    w = workload_per_worker((v, e), config)
    calculated_tuple_shuffles.append(round(e_size * w * number_of_workers(config) / 1000000))

  print("Expected tuples to shuffle:   ", expected_tuples_to_shuffle)
  print("Calculated tuples to shuffle: ", calculated_tuple_shuffles)


write_replication_file()
check_against_paper_results()
