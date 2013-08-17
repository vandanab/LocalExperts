'''
Created on Jul 15, 2013
@author: vandana
Local Experts can be of different types:
1. Local vs Global
2. Comprehensive vs Incomprehensive
This module does some analysis regarding types of local experts
'''
from src.ole.model.evaluate import get_filename, get_result_from_csv_file
import matplotlib.pyplot as plt
import os
import numpy as np

class GoodExperts:
  def __init__(self, locations_file, queries_file):
    f = open(locations_file, "r")
    self.locations = [x.strip() for x in f.readlines()]
    f.close()
    f = open(queries_file, "r")
    self.queries = [x.strip() for x in f.readlines()]
    f.close()
  
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
        fname = get_filename("ole", tname, lname)
        if fname in all_fnames:
          ind = all_fnames.index(fname)
          self.queries_data.append({"q": i, "l":j})
          self.results["ole"].append(get_result_from_csv_file(all_files[ind]))
  
  def compute_num_of_comprehensive_experts(self, methods, plot=True):
    for m in methods:
      for i in self.results[m]: 
        nc = 0.0 #num of comprehensive experts
        nr = 0.0 #num of relevant experts:
        for j in i["expert_type"]:
          if j == "1" or j == "2":
            nr += 1
            if j == "2":
              nc += 1
        if nr > 0:
          i["nc"] = nc/nr
        else:
          i["nc"] = 0.0
    num_comp_experts_map = {}
    for m in methods:
      map_for_method = num_comp_experts_map[m] = {}
      for i in range(len(self.queries_data)):
        query = self.queries_data[i]
        if query["q"] not in map_for_method:
          map_for_method[query["q"]] = {}
        if query["l"] not in map_for_method[query["q"]]:
          map_for_method[query["q"]][query["l"]] = self.results[m][i]["nc"]
      if plot:
        self.plot_comprehensive_experts_across_queries(map_for_method)
    return num_comp_experts_map
  
  def plot_comprehensive_experts_across_queries(self, result_map):
    print result_map
    Y, labels = [], []
    for i in result_map:
      labels.append(i)
      avg_num_comp = 0.0 #avg num of comprehensive experts across locations
      nl = 0
      for j in result_map[i]:
        nl += 1
        avg_num_comp += result_map[i][j]
      avg_num_comp = float(avg_num_comp/nl)
      Y.append(avg_num_comp)
    print Y
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    margin = 0.1
    X = np.arange(len(Y)) + margin
    ax.bar(X, Y, 0.2, color="#B22222")
    ax.set_ylabel("Avg. Percentage of Comprehensive Experts across locations")
    ax.set_title("Number of comprehensive experts for queries")
    ax.set_xticks(X+margin)
    ax.set_xticklabels(labels)
    plt.show()

def main():
  experiment_base = "/home/vandana/workspace/LocalExperts/data/goldset/analysis/experttype/"
  input_files_dir = "/home/vandana/workspace/LocalExperts/data/goldset/ole/topics/"
  dle = GoodExperts(experiment_base+"locations.txt",
                    experiment_base+"queries.txt")
  dle.process_data(input_files_dir)
  dle.compute_num_of_comprehensive_experts(["ole"])

if __name__ == "__main__":
  main()
