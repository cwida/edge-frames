import csv
import itertools
from functools import reduce
from typing import List, Tuple, Dict
from string import ascii_lowercase

from math import sqrt, ceil


def workload_per_worker(edges: List[Tuple[str, str]], config: Dict[str, int]):
  w = 0
  for (a, b) in edges:
    size = config[a] * config[b]
    w += 1 / size

  return w


def workload_total(edges, c):
  w = workload_per_worker(edges, c)
  return w / number_of_workers(c)


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
      elif w == min_workload and (best_conf is None or max(c) < max(best_conf)):
        best_conf = c
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


clique_patterns = list(map(lambda i: clique_pattern(i), range(3, 7)))
path_patterns = list(map(lambda i: clique_pattern(i), range(2, 10)))


def write_replication_file():
  """
  Produces a table that shows how many percent of the E relationship is hold at each node for the optimal
  configurations, given a fixed number of workers. Optimality is defined as detailed in Chu et al. 2015.
  :return:
  """
  patterns = clique_patterns + path_patterns + [diamond_pattern()] + [house_pattern()]
  field_names = ['vertices', 'edges', 'workers', 'workers_used', 'config', 'max_percentage']
  rows = []
  workers = 64

  for (v, e) in patterns:
      c = best_configuration(workers, e, v)
      rows.append((len(v), len(e), workers, number_of_workers(c), '; '.join(map(str, map(lambda v: c[v], v))), workload_per_worker(e, c)))

  with open('output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(field_names)

    writer.writerows(rows)


def check_against_paper_results():
  # Size of edge relationship in paper (twitter, follower -> followee)
  e_size = 1114289
  workers = 64

  # Patterns of the paper
  # patterns = [clique_pattern(3)] + [clique_pattern(4)] + [circle_pattern(4)] + [two_rings_pattern()]
  patterns = [clique_pattern(4)]

  # Number of tuples shuffled in millions from the paper, same order as patterns
  expected_tuples_to_shuffle = [13, 24, 35, 17]

  calculated_tuple_shuffles = []
  for (v, e) in patterns:
    config = best_configuration(workers, e, v)
    w = workload_per_worker(e, config)
    calculated_tuple_shuffles.append(round(e_size * w * number_of_workers(config) / 1000000))

  print("Expected tuples to shuffle:   ", expected_tuples_to_shuffle)
  print("Calculated tuples to shuffle: ", calculated_tuple_shuffles)


check_against_paper_results()
write_replication_file()