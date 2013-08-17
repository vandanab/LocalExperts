'''
Created on Jun 30, 2013
@author: vandana
Comparison between various OLE models
'''
import csv
import matplotlib.pyplot as plt
import os
from math import log
from numpy import arange

class Precision:
  def evaluate_precision_at_k(self, queries, results, k=10, plot=True, cmap=False):
    print results
    for i in range(len(queries)):
      topic = queries[i]["q"]
      location = queries[i]["l"]
      print "query: ", topic + "-" + location
      precs_by_method, recs_by_method = {}, {}
      for method in results:
        print results[method][i]
        labels = results[method][i]["labels"]
        p, r = [], []
        num_relevant,  avg_prec = 0.0, 0.0
        num_comprehensive_experts = 0.0
        for j in range(k):
          if j >= len(labels):
            break
          if labels[j] == "1":
            num_relevant += 1.0
          p.append(float(num_relevant/float(j+1)))
          if labels[j] == "1":
            avg_prec += p[j]
            if results[method][i]["expert_type"][j] == "2":
              num_comprehensive_experts += 1.0
          r.append(num_relevant)
        precs_by_method[method] = results[method][i]["precs"] = p
        if num_relevant > 0:
          recs_by_method[method] = results[method][i]["recs"] = [x/num_relevant for x in r]
          results[method][i]["ap"] = float(avg_prec/num_relevant)
          results[method][i]["ac"] = float(num_comprehensive_experts/num_relevant)
          print "avg precision("+method+"): ", results[method][i]["ap"]
        else:
          results[method][i]["ac"] = results[method][i]["ap"] = 0.0
      """
      if plot:
        plot_title = topic + "-" + location
        self.plot_precision_at_k(plot_title, precs_by_method, k)
        #self.plot_prec_recall(plot_title, precs_by_method, recs_by_method, k)
      """
    if cmap:
      self.compute_map_across_queries(results, plot)
    
    self.results = results
  
  def compute_map_across_queries(self, results, plot=False):
    result_map = {}
    for method in results:
      num_queries = len(results[method])
      mean_avg_prec = 0.0
      mean_num_comprehensive_experts = 0.0
      for i in range(num_queries):
        mean_avg_prec += results[method][i]["ap"]
        mean_num_comprehensive_experts += results[method][i]["ac"]
      mean_avg_prec = (mean_avg_prec/num_queries)
      mean_num_comprehensive_experts = (mean_num_comprehensive_experts/num_queries)
      result_map[method] = {"map": mean_avg_prec,
                            "anc": mean_num_comprehensive_experts}
    if plot:
      self.plot_mean_average_precision_for_methods(result_map)
    return result_map

  def plot_best_five_maps(self):
    pass
    #plot best five average precisions
  
  def plot_precision_at_k(self, title, precs_by_method, k):
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    cs = {"ole": "red", "local": "green", "topic": "blue",
          "topic_lists": "orange", "local_topic": "black"} #color spectrum
    lengths = []
    for i in precs_by_method:
      ax.plot(precs_by_method[i], color=cs[i],
              linestyle="solid", marker="o", label=i)
      lengths.append(len(precs_by_method[i]))
    len_x = max(lengths)
    ax.set_xticks(range(len_x))
    ax.set_ylabel("Precision at " + str(k))
    handles, labels = ax.get_legend_handles_labels()
    ax.set_ylim(-0.1,1.1)
    ax.legend(handles, labels)
    plt.title(title)
    plt.show()
  
  def plot_mean_average_precision_for_methods(self, map_result_by_methods):
    labels, Y1, Y2 = [], [], []
    for method in map_result_by_methods:
      Y1.append(map_result_by_methods[method]["map"])
      """Y2.append(map_result_by_methods[method]["anc"] * \
                map_result_by_methods[method]["map"])"""
      Y2.append(map_result_by_methods[method]["anc"])
      labels.append(method)
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    margin = 0.1
    X = arange(len(Y1))+2*margin
    ax.bar(X, Y1, 0.1, color="#B22222", label="MAP")
    ax.bar(X+0.1, Y2, 0.1, color="#BDB76B", label="FCE")
    ax.set_ylabel("Mean Average Precision (MAP) / \nFraction of Comprehensive Experts (FCE)")
    ax.set_title("Comparing methods for Finding Local Experts")
    ax.set_xticks(X+0.1)
    ax.set_xticklabels(labels)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels)
    plt.show()

class Measures:
  @staticmethod
  def avg_precision(results_rel_nonrel):
    #print results_rel_nonrel
    num_relevant, avg_prec = 0.0, 0.0
    p, r = [], []
    for index, j in enumerate(results_rel_nonrel):
      #if j >= 10:
        #break
      if j > 1.0:
        num_relevant += 1.0
      p.append(float(num_relevant/float(index+1)))
      if j > 1.0:
        avg_prec += p[index]
      r.append(num_relevant)
    if num_relevant > 0:
      #recalls = [x/num_relevant for x in r]
      avg_prec = float(avg_prec/num_relevant)
      #avg_prec = float(avg_prec/(index+1))
      print "avg precision: ", avg_prec
    return (p, avg_prec)
  
  @staticmethod
  def ndcg(results, ideal_results):
    gain, dcg = [], []
    for index, rel in enumerate(results):
      gain.append(float(2**rel - 1))
      if index > 0:
        dcg.append(dcg[index-1] + gain[index]/log(index+2, 2))
      else:
        dcg.append(gain[index]/log(index+2, 2))
    
    igain, idcg = [], []
    for index, rel in enumerate(ideal_results):
      igain.append(float(2**rel - 1))
      if index > 0:
        idcg.append(idcg[index-1] + igain[index]/log(index+2, 2))
      else:
        idcg.append(igain[index]/log(index+2, 2))
    #print dcg
    #print idcg
    ndcg = []
    k = len(dcg)
    for i in range(k):
      ndcg.append(float(dcg[i]/idcg[i]))
    if len(ndcg) == 0:
      print "ndcg: 0.0"
      return 0.0
    print "ndcg: ", ndcg[-1]
    return ndcg[-1]

class Experiment1:
  
  def __init__(self, queries_file, locations_file):
    f = open(locations_file, "r")
    self.locations = [x.strip() for x in f.readlines()]
    f.close()
    f = open(queries_file, "r")
    self.queries = [x.strip() for x in f.readlines()]
    f.close()

  def process_data_for_different_methods(self, input_folders):
    """
    Various methods to find local experts
    1. locally active
    2. topic experts (using @ mentions)
    3. topic experts (using lists)
    4. local experts ole
    Assuming input_folders contain data in the given order 
    """
    results = {} #{"ole": [q1, q2, ...], ...}, q1 = {"labels": [], ...}
    self.queries_data = []
    for i in self.queries:
      for j in self.locations:
        self.queries_data.append({"q": i, "l": j})
    for method in input_folders:
      all_files, all_fnames = [], []
      for r, _, fs in os.walk(input_folders[method]):
        for f in fs:
          all_files.append(r+"/"+f)
          all_fnames.append(f)
      results[method] = []
      for i in range(len(self.queries_data)):
        tname = "".join(self.queries_data[i]["q"].split())
        lname = "".join(self.queries_data[i]["l"].split())
        f_name = get_filename(method, tname, lname)
        if f_name in all_fnames:
            ind = all_fnames.index(f_name)
            results[method].append(self.get_result_from_file(method,
                                                             self.queries_data[i]["q"],
                                                             self.queries_data[i]["l"],
                                                             all_files[ind]))
        #print method+tname+" "+lname+str(len(results[method][i]["labels"]))
    return results

  def get_result_from_file(self, method, topic, location, csv_file):
    if method == "ole":
      return get_result_from_csv_file(csv_file)
    else:
      return get_result_from_csv_file_localortopic(method, topic,
                                                   location, csv_file)

def get_filename(method, tname, lname, epsilon=0):
  if method == "ole" or method == "local_topic":
    return tname+"_"+lname+"_"+str(epsilon)+".csv"
  elif method == "local":
    return lname+"_"+str(epsilon)+".csv"
  elif method == "topic" or method == "topic_lists":
    return tname+".csv"

def get_result_from_csv_file(fl, with_entropy=False):
  f = open(fl, "r")
  csvreader = csv.reader(f)
  result = {"labels": [], "expert_type": []}
  if with_entropy:
    result["entropies"] = []
  for row in csvreader:
    if row[-2] == "is_expert":
      continue
    result["labels"].append(row[-2])
    result["expert_type"].append(row[-1])
    if with_entropy:
      result["entropies"].append(row[-3])
  f.close()
  return result

def get_result_from_csv_file_localortopic(method, topic, location, fl):
  f = open(fl, "r")
  csvreader = csv.reader(f)
  result = {"labels": [], "expert_type": []}
  col_to_read = -1
  for row in csvreader:
    if row[0] == "handle":
      for i in range(len(row)):
        if method == "topic" or method == "topic_lists":
          if row[i] == location:
            break
        elif method == "local":
          if row[i] == topic:
            break
      col_to_read = i
      continue
    result["labels"].append("0" if row[col_to_read] == "0" else "1")
    result["expert_type"].append(row[col_to_read])
  f.close()
  return result

def get_fields_from_csv_file(fl):
  f = open(fl, "r")
  csvreader = csv.reader(f)
  headers = []
  result = {}
  for row in csvreader:
    if row[-2] == "is_expert":
      headers = row
      for i in range(len(row)):
        result[row[i]] = []
      continue
    for i in range(len(row)):
      result[headers[i]].append(row[i])
  f.close()
  return result

def main():
  experiment_base = "/home/vandana/workspace/LocalExperts/data/goldset/analysis/allmethods/"
  base = "/home/vandana/workspace/LocalExperts/data/goldset/"
  input_folders = {"ole": base+"ole/topics/", "local": base+"local/files/", 
                   "topic": base+"topic/topics/",
                   "topic_lists": base+"topic_lists/topics/"}
  exp1 = Experiment1(experiment_base+"queries.txt",
                     experiment_base+"locations.txt")
  results = exp1.process_data_for_different_methods(input_folders)
  prec = Precision()
  prec.evaluate_precision_at_k(exp1.queries_data, results, 20, cmap=True)

if __name__ == "__main__":
  main()
  