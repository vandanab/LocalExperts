'''
Created on May 14, 2013
@author: vandana
OLE Evaluation: Precision, Recall, MAP, NDCG, etc.
'''
import httplib2
import matplotlib.pyplot as plt
from numpy import arange
from pymongo import Connection
    
"""
Checks the user responses recorded for OLE and calculates the MAP and precision
"""
class Precision:
  def __init__(self):
    self.conn = Connection("wheezy.cs.tamu.edu", 27017)
    self.db = self.conn["ole_evaluation"]
    self.http = httplib2.Http()
  
  def evaluate_precision_at_k(self, k=10, plot=True):
    it = self.db["user_response"].find({"ur": {"$ne": {}}})
    total_queries, valid_evals, valid_evals_with_results = 0, 0, 0
    num_ole_rated_better, num_cognos_rated_better, num_both_same, num_compare = 0, 0, 0, 0
    map_ole, map_cognos = [], []
    for record in it:
      old_style_user_eval = False
      num_relevant_docs_ole = 0.0
      num_relevant_docs_cognos = 0.0
      prec_ole = []
      prec_cognos = []
      rec_ole, rec_cognos = [], []
      map_ole.append(0.0)
      map_cognos.append(0.0)
      user_eval = record["ur"]
      if "compare" in user_eval and "arel1" not in user_eval:
        continue
      valid_evals += 1
      if type(user_eval["arel1"]) is unicode:
        #maybe use this later, depending on valid_evals
        old_style_user_eval = True
        #continue
      else:
        valid_evals_with_results += 1
      for i in range(k+1):
        if "arel"+str(i+1) in user_eval:
          e = user_eval["arel"+str(i+1)] if old_style_user_eval else user_eval["arel"+str(i+1)]["e"]
          if e == "1":
            num_relevant_docs_ole += 1
          prec_ole.append(float(num_relevant_docs_ole/float(i+1)))
          if e == "1":
            map_ole[total_queries] += prec_ole[i]
          """
          else:
            if i == 0:
              prec_ole.append(0.0)
            else:
              prec_ole.append(prec_ole[i-1])
          """
          rec_ole.append(num_relevant_docs_ole)
        if "crel"+str(i) in user_eval:
          e = user_eval["crel"+str(i)] if old_style_user_eval else user_eval["crel"+str(i)]["e"]
          if e == "1":
            num_relevant_docs_cognos += 1
          prec_cognos.append(float(num_relevant_docs_cognos/float(i+1)))
          if e == "1":
            map_cognos[total_queries] += prec_cognos[i]
          """
          else:
            if i == 0:
              prec_cognos.append(0.0)
            else:
              prec_cognos.append(prec_ole[i-1])
          """
          rec_cognos.append(num_relevant_docs_cognos)
      if "compare" in user_eval:
        e = user_eval["compare"] if old_style_user_eval else user_eval["compare"]["e"]
        if e == "1": num_ole_rated_better += 1
        elif e == "2": num_cognos_rated_better += 1
        else: num_both_same += 1
        num_compare += 1
      
      if num_relevant_docs_ole > 0:
        rec_ole = [x/num_relevant_docs_ole for x in rec_ole[:]]
      if num_relevant_docs_cognos > 0:
        rec_cognos = [x/num_relevant_docs_cognos for x in rec_cognos[:]]
      
      #update map
      if num_relevant_docs_ole > 0:
        map_ole[total_queries] /= num_relevant_docs_ole
      if num_relevant_docs_cognos > 0:
        map_cognos[total_queries] /= num_relevant_docs_cognos
      total_queries += 1
      
      #print details
      print "For query: q = " + record["q"] + " and l = " + record["l"]
      print "Precision at " + str(k) + " (OLE): ", str(prec_ole)
      print "Recall (OLE): ", str(rec_ole)
      print "Precision at " + str(k) + " (Cognos): ", str(prec_cognos)
      print "Recall (Cognos): ", str(rec_cognos)
      if plot:
        plot_title = record["q"] + "-" + record["l"]
        #self.plot_precision_at_k(plot_title, prec_ole, prec_cognos, k)
        
        #self.plot_prec_recall(plot_title, prec_ole, rec_ole,
                              #prec_cognos, rec_cognos)
          
    print "total queries: ", str(total_queries)
    print "num of valid evals: ", str(valid_evals)
    print "num of valid evals with results: ", str(valid_evals_with_results)
    print "num ole rated better: ", str(num_ole_rated_better)
    print "num cognos rated better: ", str(num_cognos_rated_better)
    print "num both same: ", str(num_both_same)
    print "num compare: ", str(num_compare)
    self.plot_pie(num_ole_rated_better, num_cognos_rated_better, num_both_same)
    print "MAP (OLE): ", str(sum(map_ole)/len(map_ole))
    print "MAP (Cognos): ", str(sum(map_cognos)/len(map_cognos))
    self.plot_map_compare(sum(map_ole)/len(map_ole),
                          sum(map_cognos)/len(map_cognos))
    
  def plot_precision_at_k(self, title, prec_ole, prec_cognos, k):
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    ax.plot(prec_ole, color="red", linestyle="solid", marker="o")
    ax.plot(prec_cognos, color="green", linestyle="solid", marker="o")
    len_x = len(prec_ole) if len(prec_ole) > len(prec_cognos) else len(prec_cognos)
    ax.set_xticks(range(len_x))
    ax.set_ylabel("Precision at " + str(k))
    """
    max_y = 1.0
    if len(prec_ole) > 0 and len(prec_cognos) > 0:
      max_y = max(prec_ole) if max(prec_ole) > max(prec_cognos) else max(prec_cognos)
    elif len(prec_ole) > 0:
      max_y = max(prec_ole)
    else:
      max_y = max(prec_cognos)
    ax.set_ylim(-0.1,max_y+0.1)
    """
    ax.set_ylim(-0.1,1.1)
    plt.title(title)
    plt.show()
  
  def plot_prec_recall(self, title, prec_ole, rec_ole, prec_cognos, rec_cognos):
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    ax.plot(rec_ole, prec_ole, color="red", linestyle="solid", marker="o")
    ax.plot(rec_cognos, prec_cognos, color="green", linestyle="solid", marker="o")
    ax.set_ylabel("Precision")
    ax.set_xlabel("Recall")
    #max_y = max(prec_ole) if max(prec_ole) > max(prec_cognos) else max(prec_cognos)
    #ax.set_ylim(-0.1,max_y+0.1)
    ax.set_ylim(-0.1,1.1)
    #max_x = max(rec_ole) if max(rec_ole) > max(rec_cognos) else max(rec_cognos)
    #ax.set_xlim(-0.1,max_x+0.1)
    ax.set_xlim(-0.2, 1.1)
    plt.title(title)
    plt.show()
  
  def plot_map_compare(self, map_ole, map_cognos):
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    Y = [map_ole, map_cognos]
    labels = ["OLE", "Cognos"]
    margin = 0.5
    X = arange(len(Y))+margin
    ax.bar(X, Y, 0.15, color='g')
    ax.set_ylabel("Mean Average Precision")
    ax.set_title("Mean Average Precision (OLE vs Cognos)")
    ax.set_xticks(X+(0.15/2))
    ax.set_xticklabels(labels)
    plt.show()
  
  def plot_pie(self, num_ole, num_cognos, num_both):
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    total = float(num_ole + num_cognos + num_both)
    Y = [num_ole/total, num_cognos/total, num_both/total]
    ax.pie(Y, explode=None, labels=["OLE", "Cognos", "Both"],
           colors=("#E01B5D", "#1B8EE0", "#E0A21B"),
           autopct=None, pctdistance=0.6, shadow=False,
           labeldistance=1.1)
    plt.show()


def main_precision():
  prec = Precision()
  #prec.evaluate_precision_at_k(plot=False)
  prec.evaluate_precision_at_k()

if __name__ == "__main__":
  main_precision()