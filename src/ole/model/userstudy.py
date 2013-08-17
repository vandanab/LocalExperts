'''
Created on Jul 27, 2013
@author: vandana
regenerate results for the userstudy queries to generate better
comparison graphs (ndcg, map, etc).
'''
from numpy import arange
from pymongo import Connection
from src.ole.model.data import search_ole, add_entropy_to_result, \
                                search_cognos, Groundtruth
from src.ole.model.evaluate import get_fields_from_csv_file, Measures
from src.ole.model.ranking import Rank
import csv
import matplotlib.pyplot as plt
import os
import time

def get_userstudy_queries(k=10):
  queries_data, results = [], {"ole":[], "cognos":[],
                               "compare":{"ole": 0, "cognos": 0, "neutral": 0}}
  conn = Connection("wheezy.cs.tamu.edu", 27017)
  db = conn["ole_evaluation"]
  it = db["user_response"].find({"ur": {"$ne": {}}})
  
  #get information from the db
  for record in it:
    user_eval = record["ur"]
    if "arel1" not in user_eval:
      continue
    old_style_user_eval = False
    if type(user_eval["arel1"]) is unicode:
      old_style_user_eval = True
    ole_labels, cognos_labels = [], []
    compare = 0
    for i in range(k):
      if "arel"+str(i+1) in user_eval:
        e = user_eval["arel"+str(i+1)] if old_style_user_eval \
                                       else user_eval["arel"+str(i+1)]["e"]
        ole_labels.append("1" if e == "1" else "0")
      if "crel"+str(i) in user_eval:
        e = user_eval["crel"+str(i)] if old_style_user_eval \
                                     else user_eval["crel"+str(i)]["e"]
        cognos_labels.append("1" if e == "1" else "0")
    if "compare" in user_eval:
      compare = user_eval["compare"] if old_style_user_eval \
                                     else user_eval["compare"]["e"]
      if compare == "1":
        results["compare"]["ole"] += 1
      elif compare == "2":
        results["compare"]["cognos"] += 1
      else:
        results["compare"]["neutral"] += 1
    results["ole"].append({"labels": ole_labels})
    results["cognos"].append({"labels": cognos_labels})
    queries_data.append({"q": record["q"], "l": record["l"]})
  
  f0 = open("eval_queries.txt", "w")
  for x,y,z in zip(queries_data, results["ole"], results["cognos"]):
    f0.write(str(x)+" "+str(y)+" "+str(z))
    f0.write("\n")
  f0.close()
    
  return (queries_data, results)

class Data:
  def results_ole(self, outfolder, queries_data, userstudy_results, 
                  num_results=10, entropy=False):
    dup_count = 0
    fnames = []
    for c in queries_data:
      tname = "".join(c["q"].split(" OR "))
      lname = "".join(c["l"].split())
      #regular ole search
      fname = outfolder+tname+"_"+lname+"_"+str(0)
      if os.path.exists(fname):
        dup_count += 1
        fname = fname+"_"+str(dup_count)
      fname= fname+"_nl.csv"
      print fname
      result = search_ole(" ".join(c["q"].split(" OR ")), c["l"],
                          num_results=num_results)
      if entropy:
        result = add_entropy_to_result(result)
      f = open(fname, "w")
      Groundtruth.write_result_to_file(result, f, entropy)
      f.close()
      fnames.append(fname)
      time.sleep(5)
    add_userstudy_labels_to_result(fnames, userstudy_results)
  
  def results_cognos(self, outfolder, queries_data, userstudy_results,
                     num_results=10):
    dup_count = 0
    fnames = []
    for c in queries_data: 
      tname = "".join(c["q"].split(" OR "))
      lname = "".join(c["l"].split())
      fname = outfolder+tname+"_"+lname
      if os.path.exists(fname):
        dup_count += 1
        fname = fname+"_"+str(dup_count)
      fname = fname+"_nl.csv"
      print fname
      result = search_cognos("+".join(c["q"].split(" OR ")) + \
                             "+" + "+".join(c["l"].split()))
      f = open(fname, "w")
      Groundtruth.write_cognos_result_to_file(result, f)
      f.close()
      fnames.append(fname)
      time.sleep(5)
    add_userstudy_labels_to_result(fnames, userstudy_results)

def add_userstudy_labels_to_result(fnames, userstudy_labels):
  if len(fnames) != len(userstudy_labels):
    print "number of files not equal to number of results"
  fc = 0 #file count
  for fn in fnames:
    f = open(fn, "r")
    f1 = open(fn[:-7]+".csv", "w")
    csvreader = csv.reader(f)
    csvwriter = csv.writer(f1)
    rc = 0
    for row in csvreader:
      if row[0] == "handle":
        csvwriter.writerow(row)
      else:
        if len(userstudy_labels[fc]) >= rc:
          row[-2] = userstudy_labels[fc][rc-1]
        csvwriter.writerow(row)
      rc += 1
    f.close()
    f1.close()
    fc += 1

class Evaluation:
  def __init__(self, queries, ole_input_dir, ole_result_files,
               cognos_input_dir, cognos_result_files, ole_lr_dir):
    self.queries_data, self.results = [], {"ole":[], "cognos":[]}
    for c in queries:
      tname = "".join(c["q"].split(" OR "))
      lname = "".join(c["l"].split())
      fn = tname+"_"+lname+"_"+str(0)+".csv"
      if fn in ole_result_files:
        fn_cognos = tname+"_"+lname+".csv"
        self.queries_data.append(c)
        self.results["ole"].append(get_fields_from_csv_file(ole_input_dir+fn))
        self.results["cognos"].append(get_fields_from_csv_file(cognos_input_dir+\
                                                               fn_cognos))
    self.get_results_by_ranking_model(ole_lr_dir)
  
  def get_results_by_ranking_model(self, ole_lr_dir):
    outf = os.path.abspath("./ranking_model/")
    outf = outf + "/"
    data = []
    l = len(self.queries_data)
    for i in range(l):
      data.append(Rank.get_data_obj(self.queries_data[i],
                                    self.results["ole"][i], i))
    #print data
    self.results["ole_lr"] = Rank.rank_data(data, outf,
                                                outf+"ole_rank_model.pkl")
    write_new_result_to_csv(self.results, ole_lr_dir)
  
  def get_map_and_ndcg(self, plot=True):
    results = []
    l = len(self.queries_data)
    for i in range(l):
      result = {"q": self.queries_data[i]["q"], "l": self.queries_data[i]["l"],
                "res": {}}
      for m in self.results:
        labels = []
        if m == "ole_lr":
          res = self.results[m][i]["predicted_result"]["shuffled_result"]["r"]
          labels.extend(res)
        else:
          res = self.results[m][i]["local_global_012"]
          labels.extend([float(x)+1 for x in res])
        res_m = result["res"][m] = {}
        (res_m["p"], res_m["ap"]) = Measures.avg_precision(labels)
        res_m["ndcg"] = Measures.ndcg(labels, sorted(labels, reverse=True))
      results.append(result)
    if plot:
      #Evaluation.plot_prec_at_10(results)
      #Evaluation.plot_compare_methods_map(results)
      #Evaluation.plot_compare_methods_ndcg(results)
      Evaluation.plot_compare_methods(results)
    #Evaluation.save_results_to_csv(results, "map_ndcg_results.csv")
    return results
  
  def get_entropy_expertness_correlation(self):
    base = "temp_entropy/"
    ref_map = []
    for i in range(len(self.queries_data)):
      result = self.results["ole_lr"][i]
      mode = "a"
      if result["q"] not in ref_map:
        mode = "w"
        ref_map.append(result["q"])
      f = open(base+"expert_entropy_"+result["q"]+".csv", mode)
      csvwriter = csv.writer(f)
      if mode == "w":
        csvwriter.writerow(["entropy", "rank"])
      res = result["predicted_result"]["shuffled_result"]
      for j in range(len(res["r"])):
        if res["r"][j] > 1:
          csvwriter.writerow([res["r"][j], res["ent"][j]])
      f.close()
  
  @staticmethod
  def save_results_to_csv(results, outfile):
    if len(results) <= 0:
      return
    methods = [m+"_map" for m in results[0]["res"]]
    methods.extend([m+"_ndcg" for m in results[0]["res"]])
    f0 = open(outfile, "w")
    csvwriter = csv.writer(f0)
    csvwriter.writerow(methods)
    for i in results:
      row = []
      for m in i["res"]:
        row.append(i["res"][m]["ap"])
      for m in i["res"]:
        row.append(i["res"][m]["ndcg"])
      csvwriter.writerow(row)
    f0.close()
  
  @staticmethod
  def plot_prec_at_10(results):
    cs = {"cognos": "green", "ole": "red", "ole_lr": "blue"}
    for j in results:
      fig = plt.figure()
      ax = fig.add_subplot(1,1,1)
      len_x = 0
      for method in j["res"]:
        ax.plot(j["res"][method]["p"], cs[method],
                linestyle="solid", marker="o", label=method)
        if len(j["res"][method]["p"]) > len_x:
          len_x = len(j["res"][method]["p"])
      ax.set_xticks(range(len_x))
      ax.set_ylabel("Precision @ k")
      handles, labels = ax.get_legend_handles_labels()
      ax.legend(handles, labels)
      ax.set_ylim(-0.1,1.1)
      plt.title("Comparing Precision @ k - "+" ".join(j["q"].split(" OR "))+" "+j["l"])
      plt.show()
        
  @staticmethod
  def plot_compare_methods_map(results):
    x_labels, Y = [], []
    sum_avg_precs = {}
    for method in results[0]["res"]:
      x_labels.append(method)
      sum_avg_precs[method] = 0.0
    for j in results:
      for method in j["res"]:
        sum_avg_precs[method] += j["res"][method]["ap"]
    nr = len(results)
    for i in x_labels:
      Y.append(sum_avg_precs[i]/nr)  
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    margin = 0.5
    X = arange(len(Y))+margin
    ax.bar(X, Y, 0.15, color='g')
    ax.set_ylabel("Mean Average Precision")
    ax.set_title("Comparing Mean Average Precisions")
    ax.set_xticks(X+(0.15/2))
    ax.set_xticklabels(x_labels)
    plt.show()
  
  @staticmethod
  def plot_compare_methods_ndcg(results):
    x_labels, Y = [], []
    sum_ndcgs = {}
    for method in results[0]["res"]:
      x_labels.append(method)
      sum_ndcgs[method] = 0.0
    for j in results:
      for method in j["res"]:
        sum_ndcgs[method] += j["res"][method]["ndcg"]
    nr = len(results)
    for i in x_labels:
      Y.append(sum_ndcgs[i]/nr)  
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    margin = 0.5
    X = arange(len(Y))+margin
    ax.bar(X, Y, 0.15, color='g')
    ax.set_ylabel("NDCG")
    ax.set_title("Comparing NCDG")
    ax.set_xticks(X+(0.15/2))
    ax.set_xticklabels(x_labels)
    plt.show()
  
  @staticmethod
  def plot_compare_methods(results):
    x_labels, Y1, Y2 = [], [], []
    sum_avg_precs, sum_ndcgs = {}, {}
    for method in results[0]["res"]:
      x_labels.append(method)
      sum_avg_precs[method] = 0.0
      sum_ndcgs[method] = 0.0
    for j in results:
      for method in j["res"]:
        sum_avg_precs[method] += j["res"][method]["ap"]
        sum_ndcgs[method] += j["res"][method]["ndcg"]
    nr = len(results)
    for i in x_labels:
      Y1.append(sum_avg_precs[i]/nr)
      Y2.append(sum_ndcgs[i]/nr)
    print Y1, Y2
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    margin = 0.1
    X = arange(len(Y1))+2*margin
    ax.bar(X, Y1, 0.1, color="#B22222", label="MAP")
    ax.bar(X+0.1, Y2, 0.1, color="#BDB76B", label="NDCG")
    ax.set_ylabel("Mean Average Precision (MAP)/\nNormalized Discounted Cumulative Gain(NDCG)")
    ax.set_title("Comparing methods for Finding Local Experts")
    ax.set_xticks(X+0.1)
    ax.set_xticklabels(x_labels)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc="upper left")
    plt.show()

def write_new_result_to_csv(results, ole_lr_dir):
  for i in results["ole_lr"]:
    res = i["predicted_result"]["shuffled_result"]
    tname = "".join(i["q"].split(" OR "))
    lname = "".join(i["l"].split())
    fn = tname+"_"+lname+".csv"
    f0 = open(ole_lr_dir+fn, "w")
    csvwriter0 = csv.writer(f0)
    f_str = ["handle", "name", "description", "h_wt", "lsts",
             "des_wt", "entropy", "local_global_012"]
    csvwriter0.writerow(f_str)
    for j in range(len(res["sn"])):
      f_str = [res["sn"][j], res["name"][j], res["desc"][j], res["h"][j],
               res["lsts"][j], res["des"][j], res["ent"][j], res["r"][j]-1]
      csvwriter0.writerow(f_str)
    f0.close() 
  
def main():
  (queries, results) = get_userstudy_queries()
  outfolder = os.path.expanduser("~/workspace/LocalExperts/data/goldset/userstudy/")
  dt = Data()
  ole_labels = [x["labels"] for x in results["ole"]]
  dt.results_ole(outfolder+"ole/", queries, ole_labels, entropy=True)
  cognos_labels = [x["labels"] for x in results["cognos"]]
  dt.results_cognos(outfolder+"cognos/", queries, cognos_labels)

def eval_main():
  (queries, _) = get_userstudy_queries()
  base_dir = os.path.expanduser("~/workspace/LocalExperts/data/goldset/userstudy/")
  ole_files, cognos_files = [], []
  for _,_, fs in os.walk(base_dir+"ole/"):
    ole_files.extend(fs)
  for _,_,fs in os.walk(base_dir+"cognos/"):
    cognos_files.extend(fs)
  ev = Evaluation(queries, base_dir+"ole/", ole_files,
                    base_dir+"cognos/", cognos_files, base_dir+"ole_lr/")
  #ev.get_map_and_ndcg()
  ev.get_entropy_expertness_correlation()
  
if __name__ == "__main__":
  #main()
  eval_main()