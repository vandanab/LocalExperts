'''
Created on Jul 9, 2013
@author: vandana
Analysis about how distance from query location affects precision.
Trying to model optimal epsilon for locations
'''
from src.ole.model.evaluate import get_filename, get_result_from_csv_file, Precision
from src.utilities.color import get_colors_list
import matplotlib.pyplot as plt
import os
import numpy as np

class DistanceFromLocationEffect:
  DISTS = [0, 10, 20, 50, 100]
  EQ_DISTS = np.arange(0, 100, 10)
  def __init__(self, locations_file, queries_file, k_for_prec=20):
    f = open(locations_file, "r")
    self.locations = [x.strip() for x in f.readlines()]
    f.close()
    f = open(queries_file, "r")
    self.queries = [x.strip() for x in f.readlines()]
    f.close()
    self.k_for_prec = k_for_prec
  
  """
  input_dir: contains all labeled files for this experiment
    in the appropriate folder in goldset
  """
  def process_data(self, input_dir):
    self.queries_data = []
    self.results = {"ole": []}
    all_files, all_fnames = [], []
    for r, _, fs in os.walk(input_dir):
      for f in fs:
        all_files.append(r+"/"+f)
        all_fnames.append(f)
    for i in self.queries:
      tname = "".join(i.split())
      for j in self.locations:
        lname = "".join(j.split())
        for k in DistanceFromLocationEffect.DISTS:
          fname = get_filename("ole", tname, lname, k)
          if fname in all_fnames:
            ind = all_fnames.index(fname)
            self.queries_data.append({"q": i, "l":j, "e": k})
            self.results["ole"].append(get_result_from_csv_file(all_files[ind]))
  
  def epsilon_plots_for_locations(self):
    prec_calculator = Precision()
    prec_calculator.evaluate_precision_at_k(self.queries_data,
                                            self.results,
                                            self.k_for_prec, False)
    num_queries = len(self.queries_data)
    """
    for i in self.queries:
      for j in self.locations:
        queries_to_plot = []
        results_to_plot = []
        for k in range(num_queries):
          if self.queries_data[k]["q"] == i and self.queries_data[k]["l"] == j:
            queries_to_plot.append(self.queries_data[k])
            results_to_plot.append(self.results["ole"][k])
        self.epsilon_plot_for_location_per_query(i, j,
                                                 queries_to_plot,
                                                 results_to_plot)
    """
    for j in self.locations:
      to_plot = {}
      for k in range(num_queries):
        if self.queries_data[k]["l"] == j:
          if self.queries_data[k]["q"] not in to_plot:
            to_plot[self.queries_data[k]["q"]] = []
          to_plot[self.queries_data[k]["q"]].append({"e": self.queries_data[k]["e"],
                                                     "ap": self.results["ole"][k]["ap"]})
      #print to_plot
      self.epsilon_plot_per_location(j, to_plot)
  
  def epsilon_plot_per_location(self, l, data):
    title = "local expert perimeter - " + l
    colors_list = get_colors_list()
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    #ind = range(len(DistanceFromLocationEffect.DISTS))
    ind = DistanceFromLocationEffect.EQ_DISTS
    #xtick_labels = [str(x) for x in DistanceFromLocationEffect.DISTS]
    #width = 0.05
    cq = 0
    max_eps = []
    for query in data:
      sorted_query_data = sorted(data[query], key=lambda k: k["e"])
      Y = []
      eps = [x["e"] for x in sorted_query_data]
      #for i in range(len(DistanceFromLocationEffect.DISTS)):
      max_y, max_y_index = 0, -1
      for i in range(len(ind)):
        if ind[i] in eps:
          index = eps.index(ind[i])
          Y.append(sorted_query_data[index]["ap"])
          if Y[i] > max_y:
            max_y = Y[i]
            max_y_index = eps[index]
        else:
          Y.append(Y[i-1])
      if max_y_index >= 0:
        max_eps.append(max_y_index)
      #ax.bar(ind+(width*cq), Y, width, color=colors_list[cq], label=query)
      ax.plot(ind, Y, color=colors_list[cq], linewidth=3, label=query)
      cq += 1
    print max_eps
    optimal_ep = float(sum(max_eps))/len(max_eps)
    ax.vlines([optimal_ep], 0, 1,
              color="red", linewidth=1, linestyles="solid" )
    ax.annotate("x = "+str(optimal_ep), xy=(optimal_ep,0),
                xytext=(+10,+30), textcoords= "offset points",
                ha="left", va="top",
                bbox=dict(boxstyle="round,pad=0.5", fc="yellow", alpha=0.5),
                arrowprops=dict(arrowstyle="->", connectionstyle="arc3, rad=0"))
    #ax.set_xticks(ind+width*(cq/2))
    #ax.set_xticks(ind)
    #ax.set_xticklabels(xtick_labels)
    ax.set_ylabel("Average Precision at" + str(self.k_for_prec))
    ax.set_xlabel("Epsilon")
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels)
    #ax.set_xlim(-0.1, 4.1)
    ax.set_xlim(-1, 101)
    ax.set_ylim(0, 1.1)
    plt.title(title)
    plt.show()
  
  def epsilon_plot_for_location_per_query(self, q, l, queries, results):
    title = q + "-" + l
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    X = range(len(queries))
    Y = [y["ap"] for y in results]
    ax.bar(X, Y, color="green")
    ax.set_xticks([x["e"] for x in queries])
    ax.set_ylabel("Average Precision at" + str(self.k_for_prec))
    plt.title(title)
    plt.show()

def main():
  experiment_base = "/home/vandana/workspace/LocalExperts/data/goldset/analysis/epsilon/"
  input_files_dir = "/home/vandana/workspace/LocalExperts/data/goldset/ole/topics/"
  dle = DistanceFromLocationEffect(experiment_base+"locations.txt",
                                   experiment_base+"queries.txt")
  dle.process_data(input_files_dir)
  dle.epsilon_plots_for_locations()

if __name__ == "__main__":
  main()
  

    
