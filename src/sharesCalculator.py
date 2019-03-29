import csv
import sys
import itertools
from functools import reduce

from math import sqrt, ceil

""" Workload as defined in "From Theory to Practice": biggest workload over all nodes."""
def workload(num_edges, c):
  size = sys.maxsize
  for d1 in c:
    for d2 in c:
      size = min(d1 * d2, size)

  return num_edges / pow(size, 2)


"""best configuration as defined in "From Theory to Practice": 
most even configuration with lowest workload"""
def best_configuration(workers, edges, dimensions):
  min_workload = 1.0
  best_conf = None
  for c in itertools.combinations_with_replacement(list(range(1, ceil(sqrt(workers)))), dimensions):
    w = workload(edges, c)
    if reduce(lambda x, y: x * y, c) < workers:
      min_workload = min(min_workload, w)
      best_conf = c
    elif w == min_workload and max(c) < max(best_conf):
      best_conf = c
  return best_conf

def numberOfWorkers(c):
  return reduce(lambda x, y: x * y, c)


def percentageOfTuplesPerWorker(num_edges, size):
  return num_edges / pow(size, 2)


def numberOfEdgesForClique(num_vertices):
  return int(num_vertices * (num_vertices - 1) / 2)


def numberOfEdgesForPath(num_vertices):
  return num_vertices - 1


cliquePatterns = list(map(lambda i: (i, numberOfEdgesForClique(i)), range(3, 7)))
cliquePatterns += [(4, numberOfEdgesForClique(4) - 1), (5, numberOfEdgesForClique(5) - 2)]
pathPatterns = list(map(lambda i: (i, numberOfEdgesForPath(i)), range(5, 10)))

fieldnames = ['vertices', 'edges', 'workers' 'percentage']
rows = []

for (v, e) in cliquePatterns + pathPatterns:
    rows.append((v, e, pow(4, v), percentageOfTuplesPerWorker(e, 4)))

with open('output.csv', 'w') as f:
  writer = csv.writer(f)
  writer.writerow(fieldnames)

  writer.writerows(rows)


