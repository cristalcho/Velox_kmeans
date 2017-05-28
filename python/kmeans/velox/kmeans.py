#!/usr/bin/python

from veloxmr_lib import *

def before_map(options):
  centroids = []

  with open("/home/deukyeon/velox-apps/data/kmeans_centroids.data") as f:
    for line in f:
      centroids.append([float(x) for x in line.split(',')])

  options["centroids"] = centroids

def map(line, output, options):
  import sys
  m = sys.float_info.max

  p = [float(x) for x in line.split(",")]

  for c in options["centroids"]:
    dist = ((c[0] - p[0]) * (c[0] - p[0])) + ((c[1] - p[1]) * (c[1] - p[1]))
    if(dist < m):
      nearest = c
      m = dist

  key = ",".join(str(x) for x in nearest)
  if key not in output:
    output[key] = []
  output[key].append(line)

def reduce(key, values, output):
  if(len(values) == 0):
    return

  count = 0
  total = [0, 0]
  final_output = ""

  for v in values:
    p = [float(x) for x in v.split(",")]
    total[0] += p[0]
    total[1] += p[1]

    count += 1

    final_output += v.strip()
    if(count < len(values)):
      final_output += ' '

  centroid = [x / float(count) for x in total] 

  new_key = ",".join(str(x) for x in centroid)
  output[new_key] = final_output

import errno
import os 
import subprocess
from kmeans_plot import KmeansPlot

def run(iteration = 1, plot_everytime = False):
  for i in range(iteration):
    print "Starting MapReduce.."
    mapreduce("kmeans.input", map, reduce, "kmeans.output", before_map)
    print "Finished."

    result = os.popen("veloxdfs cat kmeans.output").read()

    # To visualization
    plot = KmeansPlot()

    while True:
      try:
        with open("/home/deukyeon/velox-apps/data/kmeans_centroids.data", "w") as f:
          lines = [x for x in result.split(os.linesep) if x]
          cnt = 0
          total_cnt = float(len(lines))
          for line in lines:
            if(len(line) == 0):
              continue

            kv = line.split(':')
            f.write(kv[0].strip() + os.linesep)

            centroid = [float(x) for x in kv[0].strip().split(',')]

            data = []
            for d in kv[1].split():
              data.append([float(x) for x in d.split(',')])

            if i == iteration - 1 or plot_everytime:
              plot.set_data(centroid, data, seed = cnt / (total_cnt - 1))
              cnt += 1

        break
      except IOError as e:
        if e.errno == errno.EWOULDBLOCK:
          continue

    if i == iteration - 1 or plot_everytime:
      plot.draw()

def reset(n = 7):
  while True:
    try:
      with open("/home/deukyeon/velox-apps/data/kmeans_centroids.data", "w") as f:
        subprocess.Popen(("ruby /home/deukyeon/velox-apps/point_generator.ruby " + str(n)).split(), stdout = f)

      break
    except IOError as e:
      if e.errno == errno.EWOULDBLOCK:
        continue
