'''
Created on Jun 21, 2013
@author: vandana
Module does entropy related analysis to understand if entropy is a good
feature for ranking model
'''
from bs4 import BeautifulSoup
from math import log
from pymongo import Connection
from src.ole.model.evaluate import get_filename, get_result_from_csv_file
from src.utilities import color
import cjson
import csv
import httplib2
import json
from math import floor
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import os
import re
import urllib

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
        #experts.remove(i)
        #not removing record if number of tweets = 0 (hopefully the user is irrelevant)
        i["entropy"] = None

    if plot:
      self.plot_entropy_vs_expertscore(experts, query, location)
    return experts
  
  def calculate_entropy_experts_list(self, results, category):
    category_results = results[category]
    for cr in category_results:
      query_terms = re.findall(r'"[\w\s]+"|\w+', cr["q"])
      localexperts_specific = [{"u": e} for e in cr["localbiz_es"]]
      self.get_all_tweets(query_terms, localexperts_specific)
      #self.calculate_entropy(localexperts_specific, True, cr["q"], cr["l"])
      self.calculate_entropy(localexperts_specific, False)
      cr["localbiz_es"] = localexperts_specific
      
      localexperts_generic = [{"u": e} for e in cr["people_es"]]
      self.get_all_tweets(query_terms, localexperts_generic)
      #self.calculate_entropy(localexperts_generic, True, cr["q"], cr["l"])
      self.calculate_entropy(localexperts_generic, False)
      cr["people_es"] = localexperts_generic
  
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
      if len(qset[i]["localbiz_es"]) > 0:
        ax = fig.add_subplot(len(qset),2,2*i+1)
        Y = [item["entropy"] for item in qset[i]["localbiz_es"]]
        K = [item["u"] for item in qset[i]["localbiz_es"]]
        ax.plot(Y, 'r', marker='o')
        ax.set_xticks(range(len(Y)))
        ax.set_ylabel("Topic-Location Entropy")
        ax.set_xticklabels(K, rotation=30, ha="right", fontsize=8)
        ax.set_title(qset[i]["q"]+"-"+qset[i]["l"]+" (local businesses)")
      
      if len(qset[i]["people_es"]) > 0:
        ax1 = fig.add_subplot(len(qset),2,2*i+2)
        Y = [item["entropy"] for item in qset[i]["people_es"]]
        K = [item["u"] for item in qset[i]["people_es"]]
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
      result_obj["localbiz_es"] = re.split(",\s+", cols[3].text)
      result_obj["people_es"] = re.split(",\s+", cols[4].text)
      if len(cols) > 6:
        result_obj["s"] = cols[6].text
      results.append(result_obj)
    return results


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

"""
Experiment to plot entropies across locations for various queries
"""  
class Experiment:
  DISTS = [0, 10, 20, 50, 100]
  def __init__(self, queries_file, locations_file):
    f = open(queries_file, "r")
    self.queries = [x.strip() for x in f.readlines()]
    f.close()
    f = open(locations_file, "r")
    self.locations = [x.strip() for x in f.readlines()]
    f.close()

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
        #for k in Experiment.DISTS:
        #fname = get_filename("ole", tname, lname, k)
        fname = get_filename("ole", tname, lname)
        if fname in all_fnames:
          ind = all_fnames.index(fname)
          self.queries_data.append({"q": i, "l":j})
          self.results["ole"].append(get_result_from_csv_file(all_files[ind],
                                                              True))
  def plot_entropy_data(self, output_dir=None):
    #colors_list = color.get_colors_list()
    colors_list = ["r", "g", "b", "purple", "#7FFF00", "#8B0000", "black"]
    num_queries = len(self.queries_data)
    title = "Scatter plot"
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    to_plot = {}
    for i in range(num_queries):
      query = self.queries_data[i]
      result = self.results["ole"][i]
      X1, Y1, Y2, labels = [], [], [], []
      #X2, Y2 = [], []
      for j in range(len(result["labels"])):
        if result["labels"][j] == "1" and result["entropies"][j]:
          X1.append(j+1)
          Y1.append(float(result["entropies"][j]))
          #Y2.append(floor(float(result["entropies"][j]))+0.5) #some expt
          """if result["expert_type"][j] == "1":
            X1.append(j+1)
            Y1.append(float(result["entropies"][j]))
          else:
            X2.append(j+1)
            Y2.append(float(result["entropies"][j]))"""
          labels.append(query["l"])
      #data used for entropy heatmap
      if output_dir:
        write_to_heatmap_datafile(output_dir, query["q"], query["l"],
                                sorted(Y1, reverse=True))
      
      if query["q"] not in to_plot:
        to_plot[query["q"]] = {"X": [], "Y": [], "labels":[]}
        """to_plot[query["q"]] = {"X1": [], "Y1": [],
                               "X2": [], "Y2": [],
                               "labels":[]}"""
      to_plot[query["q"]]["X"].extend(X1)
      to_plot[query["q"]]["Y"].extend(Y1)
      to_plot[query["q"]]["labels"].extend(labels)
      """to_plot[query["q"]]["X1"].extend(X1)
      to_plot[query["q"]]["Y1"].extend(Y1)
      to_plot[query["q"]]["X2"].extend(X2)
      to_plot[query["q"]]["Y2"].extend(Y2)"""
    ci = 0
    for topic in to_plot:
      X = to_plot[topic]["X"]
      Y = to_plot[topic]["Y"]
      ax.scatter(X, Y, s=70, c=colors_list[ci], marker="o", label=topic)
      """X1, Y1 = to_plot[topic]["X1"], to_plot[topic]["Y1"]
      X2, Y2 = to_plot[topic]["X2"], to_plot[topic]["Y2"]
      ax.scatter(X1, Y1, s=40, c=colors_list[ci], marker="o", label=topic)
      ax.scatter(X2, Y2, s=50, c=colors_list[ci], marker="^", label=topic)"""
      ci += 1
      """
      for label, x, y in zip(to_plot[topic]["labels"], X, Y):
        if y > 2:
          ax.annotate(label, xy=(x,y), xytext=(-10,-10),
                    textcoords= "offset points", ha="right", va="bottom",
                    bbox=dict(boxstyle="round,pad=0.5", fc="yellow", alpha=0.5),
                    arrowprops=dict(arrowstyle="->", connectionstyle="arc3, rad=0"))
      """
    ax.set_ylabel("Expert Entropy")
    handles, labels = ax.get_legend_handles_labels()
    ax.set_ylim(bottom=-0.1)
    ax.legend(handles, labels)
    plt.title(title)
    plt.show()
  
  def plot_entropy_hist(self):
    num_queries = len(self.queries_data)
    to_plot = {}
    for i in range(num_queries):
      query = self.queries_data[i]
      result = self.results["ole"][i]
      X1, Y1, labels = [], [], []
      for j in range(len(result["labels"])):
        if result["labels"][j] == "1" and result["entropies"][j]:
          X1.append(j+1)
          Y1.append(float(result["entropies"][j]))
          labels.append(query["l"])
      if query["q"] not in to_plot:
        to_plot[query["q"]] = {"X": [], "Y": [], "labels":[]}
      to_plot[query["q"]]["X"].extend(X1)
      to_plot[query["q"]]["Y"].extend(Y1)
      to_plot[query["q"]]["labels"].extend(labels)
    for topic in to_plot:
      title = "Entropy Distribution - " + topic
      fig = plt.figure()
      ax = fig.add_subplot(1,1,1)
      Y = to_plot[topic]["Y"]
      f = open(topic+".csv", "w")
      f.write("Expert Entropy\n")
      f.writelines([str(x)+"\n" for x in Y])
      f.close()
      ax.hist(Y, 20, normed=True, facecolor="green", alpha=0.75)
      ax.set_ylabel("Percentage")
      plt.title(title)
      plt.show()

def write_to_heatmap_datafile(basedir, q, l, values):
  outfile = basedir + q + "_entropy.csv"
  row = [l]
  row.extend(values)
  f = open(outfile, "a")
  csvwriter = csv.writer(f)
  csvwriter.writerow(row)
  f.close()

def main_experiment():
  experiment_base = "/home/vandana/workspace/LocalExperts/data/goldset/analysis/entropy/"
  input_files_dir = "/home/vandana/workspace/LocalExperts/data/goldset/ole/topics/"
  dle = Experiment(experiment_base+"queries2.txt",
                   experiment_base+"locations.txt")
  dle.process_data(input_files_dir)
  dle.plot_entropy_data(experiment_base)
  #dle.plot_entropy_hist()

if __name__ == "__main__":
  #main_entropy()
  main_experiment()