import csv


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


