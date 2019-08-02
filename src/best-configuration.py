
# Script to run the best configuration calculator
# Usage:
# best-configuration.py #workers <edges>
# Example python best-configurations.py 64 a b a c b c

import sys
from sharesCalculator import best_configuration

# TODO needs to be fixed for small level of parallism

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
