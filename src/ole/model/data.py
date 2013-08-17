'''
Created on Jun 21, 2013
@author: vandana
Module helps generate data and create gold set for better evaluation of ranking
model as well as OLE
'''
import cjson
import csv
import httplib2
import os
import time
import urllib
from bs4 import BeautifulSoup
#from src.ole.model.entropy import LocationEntropy
from entropy import LocationEntropy

class Groundtruth:
  def __init__(self, query_topics, query_locations, epsilons=[0]):
    self.query_topics = query_topics
    self.query_locations = query_locations
    self.epsilons = epsilons
    self.PREVIOUS_QUERY = ""
  
  def results_ole(self, outfolder, num_results=20, entropy=False):
    f0 = open(outfolder+"queries.txt", "w")
    csvwriter0 = csv.writer(f0)
    for c in self.query_topics:
      dir_name = outfolder+c+"/"
      if not os.path.exists(dir_name):
        os.makedirs(dir_name)
      for t in self.query_topics[c]:
        for l in self.query_locations:
          tname = "".join(t.split())
          lname = "".join(l.split())
          for e in self.epsilons:
            #regular ole search
            fname = dir_name+tname+"_"+lname+"_"+str(e)+".csv"
            print fname
            if os.path.exists(fname):
              continue
            result = search_ole(t, l, e, num_results)
            if self.PREVIOUS_QUERY == result["q"]["real_query"]:
              continue
            self.PREVIOUS_QUERY = result["q"]["real_query"]
            if entropy:
              result = add_entropy_to_result(result)
            if len(result["e"]) > 5:
              #write the processed query parameters to the queries.txt
              locs = " ".join([x+"("+str(result["q"]["locations"][x]["d"])+")" 
                           for x in result["q"]["locations"]])
              f0_str = [t, l, result["q"]["text_query"], locs, str(e)]
              csvwriter0.writerow(f0_str)
              f = open(fname, "w")
              Groundtruth.write_result_to_file(result, f, entropy)
              f.close()
              time.sleep(5)
      break
    f0.close()
  
  def results_ole_localonly(self, outfolder, num_results=20):
    f0 = open(outfolder+"queries.txt", "w")
    csvwriter0 = csv.writer(f0)
    for l in self.query_locations:
      if l != "san francisco":
        continue
      lname = "".join(l.split())
      for e in self.epsilons:
        fname = outfolder+lname+"_"+str(e)+".csv"
        print fname
        if os.path.exists(fname):
          continue
        result = search_ole(None, l, e, num_results)
        if self.PREVIOUS_QUERY == result["q"]["real_query"]:
          continue
        self.PREVIOUS_QUERY = result["q"]["real_query"]
        if len(result["e"]) > 5:
          #write the processed query parameters to the queries.txt
          locs = " ".join([x+"("+str(result["q"]["locations"][x]["d"])+")" 
                       for x in result["q"]["locations"]])
          f0_str = [l, locs, str(e)]
          csvwriter0.writerow(f0_str)
          f = open(fname, "w")
          Groundtruth.write_result_to_file(result, f)
          f.close()
          time.sleep(5)
    f0.close()
  
  def results_ole_topiconly(self, outfolder, num_results=20):
    f0 = open(outfolder+"queries.txt", "w")
    csvwriter0 = csv.writer(f0)
    for c in self.query_topics:
      dir_name = outfolder+c+"/"
      if not os.path.exists(dir_name):
        os.makedirs(dir_name)
      for t in self.query_topics[c]: 
        tname = "".join(t.split())
        fname = dir_name+tname+".csv"
        print fname
        if os.path.exists(fname):
          continue
        result = search_ole(t, None)
        if len(result["e"]) > 5:
          #write the processed query parameters to the queries.txt
          f0_str = [t]
          csvwriter0.writerow(f0_str)
          f = open(fname, "w")
          Groundtruth.write_result_to_file(result, f)
          f.close()
          time.sleep(5)
    f0.close()
  
  @staticmethod
  def write_result_to_file(result, outfilehandler, entropy=False):
    csvwriter = csv.writer(outfilehandler)
    f_str = ["handle", "name", "description", "home",
             "h_wt", "lsts", "des_wt", "score"]
    if entropy:
      f_str.append("entropy")
    f_str.extend(["is_expert", "local_global_012"])
    csvwriter.writerow(f_str)
    for i in result["e"]:
      name = i["p"]["name"].encode("ascii", "ignore")
      des = i["p"]["des"].encode("ascii", "ignore")
      uname = i["u"].encode("ascii", "ignore")
      home_loc = i["p"]["hl"].encode("ascii", "ignore")
      result_fields = [uname, name,
                       des, home_loc,
                       i["d"]["h"], i["d"]["lsts"],
                       i["d"]["term_des_count"], i["d"]["s"]]
      if entropy:
        result_fields.append(i["entropy"])
      result_fields.extend(["", ""])
      csvwriter.writerow(result_fields)
  
  @staticmethod
  def write_cognos_result_to_file(result, outfilehandler):
    csvwriter = csv.writer(outfilehandler)
    f_str = ["handle", "name", "description"]
    f_str.extend(["is_expert", "local_global_012"])
    csvwriter.writerow(f_str)
    for i in result:
      name = i["name"].encode("ascii", "ignore")
      des = ""
      if i["des"]:
        des = i["des"].encode("ascii", "ignore")
      uname = i["sn"].encode("ascii", "ignore")
      result_fields = [uname, name,
                       des]
      result_fields.extend(["", ""])
      csvwriter.writerow(result_fields)
  
  def results_cognos(self, outfolder, num_results=20):
    f0 = open(outfolder+"queries.txt", "w")
    csvwriter0 = csv.writer(f0)
    for c in self.query_topics:
      dir_name = outfolder+c+"/"
      if not os.path.exists(dir_name):
        os.makedirs(dir_name)
      for t in self.query_topics[c]: 
        tname = "".join(t.split())
        fname = dir_name+tname+".csv"
        print fname
        if os.path.exists(fname):
          continue
        result = search_cognos(t)
        if len(result) > 5:
          #write the processed query parameters to the queries.txt
          f0_str = [t]
          csvwriter0.writerow(f0_str)
          f = open(fname, "w")
          Groundtruth.write_cognos_result_to_file(result, f)
          f.close()
          time.sleep(5)
    f0.close()

class Constants:
  OLE_SEARCH_URL = "http://vostro.cs.tamu.edu:8080/textsearch/"
  OLE_LOCAL_SEARCH = "http://vostro.cs.tamu.edu:8080/search/"
  OLE_TOPIC_SEARCH = "http://vostro.cs.tamu.edu:8080/topicsearch/"
  COGNOS_SEARCH_URL = "http://vostro.cs.tamu.edu:8080/cognos/"

def search_ole(topic, location, epsilon=0, num_results=20,
               alpha_model=None, ranking_model=None):
  url = ""
  if not location:
    url = create_request_topic(Constants.OLE_TOPIC_SEARCH, topic, num_results)
  elif not topic:
    url = create_request_local(Constants.OLE_LOCAL_SEARCH, location,
                               epsilon, num_results)
  else:
    url = create_request(Constants.OLE_SEARCH_URL, topic, location, epsilon,
                       num_results, alpha_model, ranking_model)
  http = httplib2.Http()
  response, content = http.request(url, 'GET')
  if response['status'] == '200':
    content = cjson.decode(content)
    return content
  else:
    print "search call failed. internal server error"
  return None

def search_cognos(topic):
  results = []
  cognos_url = "http://twitter-app.mpi-sws.org/whom-to-follow/users.php?q="+topic
  #cognos_url = 'http://twitter-app.mpi-sws.org/whom-to-follow/users.php?q=beer+houston'
  http = httplib2.Http();
  response, content = http.request(cognos_url, "GET")
  if response["status"] == "200":
    soup = BeautifulSoup(content)
    search_results = soup.find(id="results")
    lis = search_results.find_all("li")
    for li in lis:
      result = {}
      result["sn"] = li.find("a", class_="username").string
      result["name"] = li.find_all("b")[1].string 
      result["des"] = li.find("div", class_="bio").string
      results.append(result)
  else:
    print "search call failed. internal server error"
  return results

def create_request(search_url, topic, location, epsilon, num_results,
                   alpha_model=None, ranking_model=None):
  url = search_url + urllib.quote_plus("q=" + topic + \
                                       "&l=" + location + \
                                       "&e=" + str(epsilon) + \
                                       "&n=" + str(num_results) + \
                                       "&p=yes&wr=yes&rg=no")
  return url

def create_request_topic(search_url, topic, num_results):
  url = search_url + urllib.quote_plus("q=" + topic + \
                                       "&n=" + str(num_results) + \
                                       "&p=yes&wr=yes&rg=no")
  return url

def create_request_local(search_url, location, epsilon, num_results):
  url = search_url + urllib.quote_plus("q=" + location + \
                                       "&e=" + str(epsilon) + \
                                       "&n=" + str(num_results) + \
                                       "&p=yes&wr=yes&rg=no")
  return url

def add_entropy_to_result(result):
  le = LocationEntropy(Constants.OLE_SEARCH_URL)
  le.enrich_results(result)
  result["e"] = le.calculate_entropy(result["e"])
  return result
  
def get_query_topics_from_csv(query_topics_csv):
  f = open(query_topics_csv, "r")
  topics = f.readline().strip().split(",")
  lines = [x.strip().split(",") for x in f.readlines()]
  f.close()
  query_topics = {}
  for i in range(len(topics)):
    query_topics[topics[i]] = list(set([j[i] for j in lines]) - set([""]))
  print query_topics
  return query_topics

def get_query_locations_from_csv(query_locations_csv):
  f = open(query_locations_csv, "r")
  query_locations = [x.strip() for x in f.readlines()]
  f.close()
  return query_locations

def main():
  topics_csv = os.path.expanduser("~/workspace/LocalExperts/data/goldset/input/queries_r.csv")
  locations_csv = os.path.expanduser("~/workspace/LocalExperts/data/goldset/input/locations_r.csv")
  topics = get_query_topics_from_csv(topics_csv)
  locations = get_query_locations_from_csv(locations_csv)
  epsilons = [0, 10, 20, 50, 100]
  outfolder = os.path.expanduser("~/workspace/LocalExperts/data/goldset/")
  gt = Groundtruth(topics, locations, epsilons)
  #gt.results_ole(outfolder+"ole/topics/", 20, True)
  gt.results_ole_localonly(outfolder+"local/", 20)
  #gt.results_ole_topiconly(outfolder+"topic/", 20)
  #gt.results_cognos(outfolder+"topic_lists/")

if __name__ == "__main__":
  main()
