'''
Created on May 14, 2013
@author: vandana
OLE analysis and evaluation of various factors
'''
import httplib2
import urllib
import cjson
import json
import os
import re
import matplotlib.pyplot as plt
from pymongo import Connection
from math import log
from bs4 import BeautifulSoup

"""
Entropy measurements
"""
class LocationEntropy:
  def __init__(self, ole_search_url):
    self.service_url = ole_search_url
    self.conn = Connection("wheezy.cs.tamu.edu", 27017)
    self.db = self.conn["local_expert_tweets"]
    self.http = httplib2.Http()
  
  """
  no user evaluation, pure results with profile and tweets
  """
  def get_results(self, query_topics, query_location, epsilon=0, experts=[]):
    url = self.create_request(query_topics, query_location, epsilon)
    self.gtexperts = experts
    response, content = self.http.request(url, 'GET')
    if response['status'] == '200':
      content = cjson.decode(content)
      return content
    else:
      print "search call failed. internal server error"
    return None
  
  def create_request(self, query_topics, query_location, epsilon):
    topics = " ".join(query_topics)
    #get results with request processing params
    url = self.service_url + urllib.quote_plus("q=" + topics + \
                                               "&l=" + query_location + \
                                               "&p=yes&wr=yes&n=50")
    return url
  
  def enrich_results(self, results):
    if not results:
      print "results none; cannot be enriched"
      return results
    
    query = results["q"]
    self.get_all_tweets(query["terms"], results["e"])
    return results
  
  def get_all_tweets(self, query_terms, experts):
    def filter_tweets(query_terms, tweet):
      for j in query_terms:
        if j in tweet.lower():
          return True
      return False
    for i in experts:
      it = self.db["user_location_tweets"].find({"sn": i["u"]})
      tweets = {}
      for x in it:
        temp = [json.dumps(item) for item in x["t"] if filter_tweets(query_terms, item)]
        if len(temp) > 0:
          tweets[x["l"]] = temp
      i["all_tweets"] = tweets
    return experts
  
  def calculate_entropy(self, experts, plot=False, query=None, location=None):
    #experts = results["e"]
    for i in experts[:]:
      entropy = 0.0
      num_tweets = 0.0
      for j in i["all_tweets"]:
        num_tweets += len(i["all_tweets"][j])
      if num_tweets > 0:
        for j in i["all_tweets"]:
          p = float(len(i["all_tweets"][j])/num_tweets)
          entropy += p * log(p)
        entropy *= -1.0
        i["entropy"] = entropy
      else:
        #entropy = 1.0
        #instead of a default entropy we drop the record
        experts.remove(i)

    if plot:
      self.plot_entropy_vs_expertscore(experts, query, location)
    return experts
  
  def calculate_entropy_experts_list(self, results, category):
    category_results = results[category]
    for cr in category_results:
      query_terms = re.findall(r'"[\w\s]+"|\w+', cr["q"])
      localexperts_specific = [{"u": e} for e in cr["les"]]
      self.get_all_tweets(query_terms, localexperts_specific)
      #self.calculate_entropy(localexperts_specific, True, cr["q"], cr["l"])
      self.calculate_entropy(localexperts_specific, False)
      cr["les"] = localexperts_specific
      
      localexperts_generic = [{"u": e} for e in cr["ges"]]
      self.get_all_tweets(query_terms, localexperts_generic)
      #self.calculate_entropy(localexperts_generic, True, cr["q"], cr["l"])
      self.calculate_entropy(localexperts_generic, False)
      cr["ges"] = localexperts_generic
  
  def plot_all_experts(self, results):
    for category in results:
      category_results = results[category]
      if category == "food":
        all_beverages = []
        all_restaurants = []
        all_others = []
        for cr in category_results:
          if "wine" in cr["q"] or "beer" in cr["q"]:
            all_beverages.append(cr)
          elif "restaurant" in cr["q"]:
            all_restaurants.append(cr)
          else:
            all_others.append(cr)
        self.plot_query_set(all_restaurants)
        self.plot_query_set(all_beverages)
        self.plot_query_set(all_others)
      elif category == "interests_music_sports":
        sets = {}
        for cr in category_results:
          if cr["s"] in sets:
            sets[cr["s"]].append(cr)
          else:
            sets[cr["s"]] = [cr]
        for i in sets:
          self.plot_query_set(sets[i])
        
  def plot_query_set(self, qset):
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.05)
    for i in range(len(qset)):
      if len(qset[i]["les"]) > 0:
        ax = fig.add_subplot(len(qset),2,2*i+1)
        Y = [item["entropy"] for item in qset[i]["les"]]
        K = [item["u"] for item in qset[i]["les"]]
        ax.plot(Y, 'r', marker='o')
        ax.set_xticks(range(len(Y)))
        ax.set_ylabel("Topic-Location Entropy")
        ax.set_xticklabels(K, rotation=30, ha="right", fontsize=8)
        ax.set_title(qset[i]["q"]+"-"+qset[i]["l"]+" (local businesses)")
      
      if len(qset[i]["ges"]) > 0:
        ax1 = fig.add_subplot(len(qset),2,2*i+2)
        Y = [item["entropy"] for item in qset[i]["ges"]]
        K = [item["u"] for item in qset[i]["ges"]]
        ax1.plot(Y, 'g', marker='o')
        ax1.set_xticks(range(len(Y)))
        ax1.set_ylabel("Topic-Location Entropy")
        ax1.set_xticklabels(K, rotation=30, ha="right", fontsize=8)
        ax1.set_title(qset[i]["q"]+"-"+qset[i]["l"]+" (generic experts)")
    plt.show()

  def plot_entropy_vs_expertscore(self, experts, query=None, location=None):
    #experts = results["e"]
    if len(experts) == 0:
      return
    X = [] #entropy
    #Y = [] #score
    K = [] #xlabels
    for i in experts:
      """
      if i["u"] not in self.gtexperts:
        continue
      """
      X.append(i["entropy"])
      #Y.append(i["d"]["s"])
      K.append(i["u"])
    """
    #plt.plot(X, Y, color="#FF6136", linestyle="solid")
    plt.plot(Y, color="#FF6136", linestyle="solid", marker="o")
    """
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    ax.plot(X, color="#FF6136", linestyle="solid", marker="o")
    ax.set_xticks(range(len(X)))
    ax.set_ylabel("Expert Score")
    ax.set_xticklabels(K)
    fig.autofmt_xdate()
    """
    plt.ylabel("Expert Score")
    plt.xlabel("Entropy")
    """
    if query != None and location != None:
      plt.title(query + "-" + location)
    plt.show()
    
    
"""
Reads queries (for which OLE has good number of experts) from a file
and performs entropy based evaluations and plots results
"""
class TestData:
  
  """
  No constructor, nothing to initialize as of now...
  """
  
  def entropy_evaluation(self):
    le = LocationEntropy("http://localhost:8080/textsearch/")
    for category in self.data:
      le.calculate_entropy_experts_list(self.data, category)
  
  def entropy_evaluation_all(self):
    le = LocationEntropy("http://localhost:8080/textsearch/")
    for category in self.data:
      le.calculate_entropy_experts_list(self.data, category)
    le.plot_all_experts(self.data)
  
  def get_data(self, input_folder):
    self.input_location = input_folder
    self.data = {}
    for root, _, files in os.walk(input_folder):
      for f in files:
        if ".html" in f:
          filepath = root + '/' + f
          category = f.split(".")[0]
          self.data[category] = self.parse(filepath)
    print self.data        

  def parse(self, filepath):
    f = open(filepath, "r")
    filecontent = f.read()
    f.close()
    results = []
    soup = BeautifulSoup(filecontent)
    table = soup.find('table', id='tblMain')
    rows = table.findAll("tr")
    for j in range(len(rows)):
      if j <= 1:
        continue
      row = rows[j]
      cols = row.findAll("td")
      result_obj = {}
      result_obj["q"] = cols[1].text
      result_obj["l"] = cols[2].text
      result_obj["les"] = re.split(",\s+", cols[3].text)
      result_obj["ges"] = re.split(",\s+", cols[4].text)
      if len(cols) > 6:
        result_obj["s"] = cols[6].text
      results.append(result_obj)
    return results


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
    num_ole_rated_better, num_cognos_rated_better, num_both_same = 0, 0, 0
    for record in it:
      old_style_user_eval = False
      total_queries += 1
      user_eval = record["ur"]
      if "compare" in user_eval and "arel1" not in user_eval:
        continue
      valid_evals += 1
      if type(user_eval["arel1"]) is unicode:
        #maybe use this later, depending on valid_evals
        old_style_user_eval = True
        continue
      else:
        valid_evals_with_results += 1
        num_relevant_docs_ole = 0.0
        num_relevant_docs_cognos = 0.0
        prec_ole = []
        prec_cognos = []
        for i in range(k+1):
          if "arel"+str(i+1) in user_eval:
            e = user_eval["arel"+str(i+1)] if old_style_user_eval else user_eval["arel"+str(i+1)]["e"]
            if e == "1":
              num_relevant_docs_ole += 1
              prec_ole.append(float(num_relevant_docs_ole/float(i+1)))
            else:
              prec_ole.append(0.0)
          if "crel"+str(i) in user_eval:
            e = user_eval["crel"+str(i)] if old_style_user_eval else user_eval["crel"+str(i)]["e"]
            if e == "1":
              num_relevant_docs_cognos += 1
              prec_cognos.append(float(num_relevant_docs_cognos/float(i+1)))
            else:
              prec_cognos.append(0.0)
        if "compare" in user_eval:
          e = user_eval["compare"] if old_style_user_eval else user_eval["compare"]["e"]
          if e == "1": num_ole_rated_better += 1
          elif e == "2": num_cognos_rated_better += 1
          else: num_both_same += 1
        print "For query: q = " + record["q"] + " and l = " + record["l"]
        print "Precision at " + str(k) + " (OLE): ", str(prec_ole)
        print "Precision at " + str(k) + " (Cognos): ", str(prec_cognos)
        if plot:
          plot_title = record["q"] + "-" + record["l"]
          self.plot_precision_at_k(plot_title, prec_ole, prec_cognos, k)
    print "total queries: ", str(total_queries)
    print "num of valid evals: ", str(valid_evals)
    print "num of valid evals with results: ", str(valid_evals_with_results)
    print "num ole rated better: ", str(num_ole_rated_better)
    print "num cognos rated better: ", str(num_cognos_rated_better)
    print "num both same: ", str(num_both_same)
    
  def plot_precision_at_k(self, title, prec_ole, prec_cognos, k):
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    ax.plot(prec_ole, color="red", linestyle="solid", marker="o")
    ax.plot(prec_cognos, color="green", linestyle="solid", marker="o")
    len_x = len(prec_ole) if len(prec_ole) > len(prec_cognos) else len(prec_cognos)
    ax.set_xticks(range(len_x))
    ax.set_ylabel("Precision at " + str(k))
    plt.title(title)
    plt.show()

def main_entropy():
  """
  le = LocationEntropy("http://localhost:8080/textsearch/")
  """
  
  #experts = ["NextRestaurant", "NRAShow", "WildfireRest", "BillRancic", "Chicago_Picks", "GrubStreetCHI", "RPMItalianChi", "longmanandeagle", "QuartinoChicago", "Waldorf_Chicago", "BigBowlRiverNth", "cemitaspuebla", "JohnTheBristol", "grahamelliot", "alpanasingh", "Chicago_Gourmet", "birchwoodkitche", "Branch27", "MC3David", "SenoraBeeps", "handlebarchi", "CamillesDish", "Vincent_Chicago", "NandoMilanoTrat", "BaumeBrixChi", "TuscanyTaylor", "thingsnoticed", "EatTravelRock", "onethousandfps", "UPchicago", "readjack", "sethbradley", "dailydoseofdina", "eatatunion", "StephAndTheGoat", "Rick_Bayless", "sriskind", "thepublican2008", "MJSHChicago", "chrisjbukowski", "MastrosChicago", "ChooseChicago", "mattlindner"]
  #r = le.get_results(["restaurant"], "chicago", 0, experts)
  #r = le.get_results(["beer"], "houston", 0)
  
  """
  r = le.get_results(["ruby"], "austin", 0)
  rs = le.enrich_results(r)
  le.calculate_entropy(rs["e"], True)
  """
  td = TestData()
  td.get_data(os.path.expanduser("~/workspace/LocalExperts/data/goldset"))
  #td.entropy_evaluation()
  td.entropy_evaluation_all()

def main_precision():
  prec = Precision()
  prec.evaluate_precision_at_k(k=20)

if __name__ == "__main__":
  #main_entropy()
  main_precision()