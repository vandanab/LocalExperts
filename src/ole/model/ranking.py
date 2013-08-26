'''
Created on Jul 22, 2013
@author: vandana
Learn to rank using regression trees (random forests)
'''
from copy import deepcopy
from numpy import arange
from random import shuffle
from src.ole.model.evaluate import get_filename, get_fields_from_csv_file, Measures
import matplotlib.pyplot as plt
import os
import subprocess

class Rank:
  topic_hash = {}
  topic_ctr = 0
  def __init__(self, locations_file, queries_file):
    f = open(locations_file, "r")
    self.locations = [x.strip() for x in f.readlines()]
    f.close()
    f = open(queries_file, "r")
    self.queries = [x.strip() for x in f.readlines()]
    f.close()
  
  def format_data(self, input_dir, train_percent=70):
    self.queries_data = []
    self.results = {"ole": []}
    all_files, all_fnames = [], []
    for r, _, fs in os.walk(input_dir):
      for f in fs:
        all_files.append(r+"/"+f)
        all_fnames.append(f)
    for i in self.queries:
      tname = "".join(i.split())
      if tname not in Rank.topic_hash:
        Rank.topic_hash[tname] = Rank.topic_ctr
        Rank.topic_ctr += 1
      for j in self.locations:
        lname = "".join(j.split())
        fname = get_filename("ole", tname, lname)
        #include epsilon data also later
        if fname in all_fnames:
          ind = all_fnames.index(fname)
          self.queries_data.append({"q": i, "l":j})
          self.results["ole"].append(get_fields_from_csv_file(all_files[ind]))
    self.data = []
    indexes = range(len(self.queries_data))
    shuffle(indexes)
    l = len(indexes)
    l_train = l*train_percent/100
    train_indexes = indexes[:l_train]
    test_indexes = indexes[l_train:]
    self.train_data, self.test_data = [] , []
    for i in train_indexes:
      query = self.queries_data[i]
      result = self.results["ole"][i]
      self.train_data.append(Rank.get_data_obj(query, result, i))
    for i in test_indexes:
      query = self.queries_data[i]
      result = self.results["ole"][i]
      self.test_data.append(Rank.get_data_obj(query, result, i))
    write_to_file("train.dat", self.train_data)
    write_to_file("test.dat", self.test_data)    
  
  @staticmethod
  def get_data_obj(query, result, i):
    #reviving the data object
    data_obj = {"q": query["q"],
                "l": query["l"],
                "index": i,
                "result": {
                           "sn": result["handle"],
                           "name": result["name"],
                           "desc": result["description"],
                           "lsts": result["lsts"],
                           "h": result["h_wt"],
                           "des": result["des_wt"],
                           "ent": result["entropy"],
                           "ls": []
                           }
                }
    if "local_global_012" in result:
      data_obj["result"]["r"] = [float(x)+1 if x != "" else 1 for x in result["local_global_012"]]
    else:
      data_obj["result"]["r"] = [0]*10
    
    k = len(result["lsts"])
    for j in range(k):
      data_obj["result"]["ls"].append((float(result["score"][j]) - \
                        (float(result["lsts"][j]) + float(result["h_wt"][j]) + \
                         float(result["des_wt"][j]))*100)/10)
    return data_obj
  
  def mltool_rank(self):
    outf = os.path.abspath("./temp_rank_model_files/")
    if not os.path.exists(outf):
      os.makedirs(outf)
    outf = outf + "/"
    subprocess.call(["mltool", "conv", "train.dat", outf+"train.tsv"])
    subprocess.call(["mltool", "conv", "test.dat", outf+"test.tsv"])
    #train
    subprocess.call(["mltool", "rf-train", "-t", "5", "-s", "1",
                     outf+"train.tsv",
                     outf+"test.tsv",
                     "-o", outf+"ole_rank_model.pkl"])
    #test
    Rank.rank_data(self.test_data, outf, outf+"ole_rank_model.pkl", "test.dat")
  
  #data is the test_data in the format used by the ranking class
  @staticmethod
  def rank_data(data, outf, model_file, data_file=None):
    if not data_file:
      write_to_file(outf+"data.dat", data)
      subprocess.call(["mltool", "conv", outf+"data.dat", outf+"data.tsv"])
    else:
      subprocess.call(["mltool", "conv", data_file, outf+"data.tsv"])
    subprocess.call(["mltool", "eval", "-o", outf+"preds.txt",
                     model_file, outf+"data.tsv"])
    f = open(outf+"preds.txt", "r")
    f1 = open(outf+"preds1.txt", "w")
    f1.write("\n")
    f1.writelines(f.readlines())
    f1.close()
    f.close()
    proc = subprocess.Popen(["paste", outf+"preds1.txt", outf+"data.tsv"],
                              stdout=subprocess.PIPE)
    output = proc.stdout.read()
    f = open(outf+"result.out", "w")
    f.write(output)
    f.close()
    #end of mltool related processing
    
    f_preds = open(outf+"result.out", "r")
    #the expert type labels predicted by the ranking model
    new_rank_labels_test_data = []
    q_c = -1
    temp = []
    l = f_preds.readline()
    for l in f_preds:
      l = l.split()
      if q_c != int(l[1]):
        if q_c != -1:
          new_rank_labels_test_data.append(temp)
        q_c = int(l[1])
        temp = []
      temp.append(l[0])
    f_preds.close()
    new_rank_labels_test_data.append(temp)
    #print new_rank_labels_test_data
    
    for i in range(len(data)):
      res = data[i]["result"]
      p_res = data[i]["predicted_result"] = {}
      p_res["newr"] = [float(x) for x in \
                     new_rank_labels_test_data[i]]
      sorted_rank_labels = sorted(enumerate(p_res["newr"]), key=lambda k:k[1],
                                  reverse=True)
      p_res["shuffle_order"] = [x for (x, _) in sorted_rank_labels]
      
      p_res["shuffled_result"] = get_shuffled_result(res, p_res["shuffle_order"])
    
    return data
  
  def rank_eval(self, map_not_ndcg=True, plot=False):
    c_ap, r_ap = [], []
    c_ndcg, r_ndcg = [], []
    for i in self.test_data:
      res, p_res = i["result"], i["predicted_result"]["shuffled_result"]
      print i["q"], " ", i["l"]
      labels = deepcopy(res["r"][:11])
      rank_as_per_prediction = deepcopy(p_res["r"])
      if map_not_ndcg:
        _, ap = Measures.avg_precision(labels)
        c_ap.append(ap)
        _, ap = Measures.avg_precision(rank_as_per_prediction)
        r_ap.append(ap)
      else:
        print "current: ", res["name"][:11]#labels
        print "after prediction: ", p_res["name"]#rank_as_per_prediction
        ideal = sorted(labels, reverse=True)
        print "ideal: ", ideal
        c_ndcg.append(Measures.ndcg(labels, ideal))
        r_ndcg.append(Measures.ndcg(rank_as_per_prediction, ideal))
    if map_not_ndcg:
      print "mean avg precision current: ", sum(c_ap)/len(c_ap)
      print "mean avg precision after ranking: ", sum(r_ap)/len(r_ap)
    else:
      print c_ndcg
      print r_ndcg
      print "mean ndcg current: ", sum(c_ndcg)/len(c_ndcg)
      print "mean ndcg after ranking: ", sum(r_ndcg)/len(r_ndcg)
      if plot:
        queries = [x["q"]+" "+x["l"] for x in self.test_data]
        plot_values([c_ndcg, r_ndcg], queries,
                    ["NDCG Fixed Rank", "NDCG Learned Rank"],
                    "NDCG Comparison before and after ranking model",
                    plot_means=True)

def write_to_file(filename, data):
  f = open(filename, "w")
  for i in data:
    f.write("#query: "+i["q"]+" - "+i["l"]+" - "+str(i["index"])+"\n")
    k = len(i["result"]["lsts"])
    res = i["result"]
    for j in range(k):
      if j > 10:
        continue
      line = str(res["r"][j]) + " " + "qid:" + str(i["index"])
      line = line + " " + get_features_str(res, j)
      f.write(line+"\n")
  f.close()
    
def get_features_str(data, index):
  line = "1:"+data["lsts"][index]
  line += " 2:"+data["h"][index]
  line += " 3:"+data["des"][index]
  line += " 4:"+(data["ent"][index] if data["ent"][index] != "" else "0")
  line += " 5:"+str(data["ls"][index])
  #e = (data["ent"][index] if data["ent"][index] != "" else "0")
  #line += " 5:"+str(float(data["lsts"][index]) * float(e))
  return line

def get_shuffled_result(data, order):
  result = {}
  #the various array fields: lsts, h, des, ent, r, ls
  for i in data:
    result[i] = [data[i][x] for x in order]
  return result

def plot_values(values, xlabels, labels, title, plot_means=False):
  fig = plt.figure()
  ax = fig.add_subplot(1,1,1)
  cs = ["red", "green", "blue"]
  c = 0
  X = arange(len(xlabels))
  for i in values:
    ax.plot(i, color=cs[c],
            linestyle="solid", marker="o", label=labels[c])
    c += 1
  ax.set_xticks(X) 
  ax.set_xticklabels(xlabels)
  ax.set_ylabel("NDCG")
  handles, labels = ax.get_legend_handles_labels()
  ax.set_ylim(-0.1,1.1)
  ax.legend(handles, labels)
  plt.title(title)
  fig.autofmt_xdate()
  plt.show()
  
  if plot_means:
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    Y = []
    for i in values:
      Y.append(sum(i)/len(i))
    margin = 0.05
    X = arange(len(Y))+margin
    ax.bar(X, Y, 0.05, color='green')
    ax.set_xticks(X+(0.05/2))
    ax.set_xticklabels(labels)
    ax.set_ylabel("Mean NDCG")
    ax.set_ylim(0,1.1)
    plt.title(title)
    plt.show()

def main():
  experiment_base = "/home/vandana/workspace/LocalExperts/data/goldset/analysis/ranking/"
  input_files_dir = "/home/vandana/workspace/LocalExperts/data/goldset/ole/topics/"
  rankm = Rank(experiment_base+"locations.txt",
               experiment_base+"queries.txt")
  rankm.format_data(input_files_dir)
  rankm.mltool_rank()
  rankm.rank_eval(False, True)

if __name__ == "__main__":
  main()