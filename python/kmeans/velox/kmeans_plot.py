import matplotlib
#matplotlib.use("agg")
import matplotlib.pyplot as plt
import numpy as np

class KmeansPlot(object):
#  def __init__(self):
    #self.fig, self.ax = plt.subplots()
    
  def draw(self):
    plt.show()

  def set_data(self, centroid, inputs, seed = None):
    print "plotting.."
    if seed is None:
      seed = np.random.rand(3)
      color = [c for x in inputs] 
    else:
      color = plt.cm.brg([seed] * len(inputs))
      #color = plt.cm.RdYlGn([seed] * len(inputs))
    plt.scatter(centroid[0], centroid[1], s = 500, c = color[:1], edgecolor = 'none', alpha = 0.75)
    x = [xy[0] for xy in inputs]
    y = [xy[1] for xy in inputs]
    plt.scatter(x, y, s = 2, c = color, edgecolor = 'none')
