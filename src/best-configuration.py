
# Script to run the best configuration calculator
# Usage:
# best-configuration.py #workers <edges>
# Example python best-configurations.py 64 a b a c b c

import sys
from src.sharesCalculator import best_configuration

workers = sys.argv[1]
edges = sys.argv[1:]

edgeTuples = []
vertices = set()

for (i, src) in enumerate(edges):
  if i % 2 == 0:
    edgeTuples.append((src, edges[i + 1]))
  vertices.add(src)

configuration = best_configuration(workers, edgeTuples, vertices)

print(",".join(configuration))