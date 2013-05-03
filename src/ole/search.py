# coding: utf-8
'''
Created on Oct 11, 2012

@author: vandana
Ole search interface

There is nothing in a caterpillar that tells you it’s going to be a butterfly.
R. Buckminster Fuller
'''

import sys
sys.path.append('/home/vandana/workspace/LocalExperts/src/')
from bottle import run, Bottle
from lucene import Version, QueryParser, IndexSearcher
from lucene import SimpleFSDirectory, StandardAnalyzer, File, WhitespaceAnalyzer
from online import Expert, OnlineUser, UserProfiles
from settings import location_index_store_dir, usermap_index_store_dir, user_index_store_dir
from utilities import geo
#from src.utilities import geo
from pygeocoder import Geocoder, GeocoderError
from pymongo import Connection
import cjson
import lucene
import re
import uuid
import json
import httplib2
from bs4 import BeautifulSoup

if __name__ == "__main__":
  location_searcher_ = None
  usermap_searcher_ = None
  user_searcher_ = None
  LI = geo.LocationInfo()
  DEBUG = True
  #all the locations in our dataset
  f = open("top200locations.txt", "r")
  ALL_LOCATIONS = [x.strip() for x in f.readlines()]
  f.close()
  app = Bottle()


"""
Local Experts (for a location, across topics)
"""
@app.route('/search/<query>')
def search(query=None):
  global location_searcher_

  if DEBUG:
    print "Raw Query: ", query

  analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
  parsed_query = process_query(query, "l")
  if parsed_query is None:
    return ""
  
  #create query for lucene
  real_query = ""
  locations = {}
  if "epsilon" in parsed_query:
    locations = get_nearby_locations(parsed_query["location"], parsed_query["epsilon"])
  else:
    locations = get_nearby_locations(parsed_query["location"], 0)	
  parsed_query["locations"] = locations

  if len(locations.keys()) == 1:
    real_query = real_query + parsed_query["location"]
  else:
    """
    locations_query = []
    for l in locations:
      locations_query.append(l + "^" + str(locations[l]["dwt"]))
    real_query = real_query + " OR ".join(locations_query)
    """
    real_query = real_query + " OR ".join([l for l in locations])

  if DEBUG:
    print "Lucene Query: ", real_query

  parsed_query["real_query"] = real_query

  #parse query using lucene parser and get docs
  p_query = QueryParser(Version.LUCENE_CURRENT, "loc", analyzer).parse(real_query)
  scoreDocs = location_searcher_.search(p_query, 500).scoreDocs
  print "%s total matching documents." % len(scoreDocs)

  #rank results
  experts = []
  rankedDocs = rankDocs(parsed_query, location_searcher_, scoreDocs)

  for i in rankedDocs:
    if parsed_query["profile"] == "yes":
      experts.append({"u": i["user"], "d": i["details"], "p": i["profile"], "t": i["tweets"]})
    else:
      experts.append({"u": i["user"], "d": i["details"]})

  response = {"sid": str(uuid.uuid1()), "es": experts}

  if parsed_query["user_study"] == "yes":
    #write session to db
    session = {"l": parsed_query["location"],
              "_id": response["sid"], "ur": {}}
    conn = Connection("wheezy.cs.tamu.edu", 27017)
    db = conn["ole_evaluation"]
    db["user_response"].insert(session)

  return cjson.encode(response)


"""
Local Experts for a location given a set of topics
"""
@app.route('/textsearch/<query>')
def textsearch(query=None):
  global usermap_searcher_

  if DEBUG:
    print "Raw Query: ", query

  analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
  parsed_query = process_query(query)
  if parsed_query is None:
    return ""

  #create query for lucene
  real_query = ""
  locations = {}
  if "epsilon" in parsed_query:
    locations = get_nearby_locations(parsed_query["location"], parsed_query["epsilon"])
  else:
    locations = get_nearby_locations(parsed_query["location"], 0)	
  parsed_query["locations"] = locations

  text_query = parsed_query["text_query"]
  if len(locations.keys()) == 1:
    real_query = text_query + " AND " + parsed_query["location"]
  else:
    lc = 0
    for l in locations:
      """
      rq = "(" + text_query + " AND " + "\"" + l + "\"" + ")^" + \
            str(locations[l]["dwt"])
      """
      rq = "(" + "(" + text_query + ")" + " AND " + "\"" + l + "\"" + ")"
      if lc == 0:
        real_query = rq
      else:
        real_query = real_query + " OR " + rq
      lc += 1
  parsed_query["real_query"] = real_query

  if DEBUG:
    print "Real Query: ", real_query
  #parse query using lucene parser and get docs
  p_query = QueryParser(Version.LUCENE_CURRENT, "text", analyzer).parse(real_query)
  scoreDocs = usermap_searcher_.search(p_query, 500).scoreDocs
  print "%s total matching documents." % len(scoreDocs)
 
  #rank results
  experts = []
  rankedDocs = rankDocs(parsed_query, usermap_searcher_, scoreDocs)
    
  for i in rankedDocs:
    if parsed_query["profile"] == "yes":
      experts.append({"u": i["user"], "d": i["details"], "p": i["profile"], "t": i["tweets"]})
      #experts.append({"u": i["user"], "d": i["details"], "t": i["tweets"]})
    else:
      experts.append({"u": i["user"], "d": i["details"]})

  response = {"sid": str(uuid.uuid1()), "es": experts}

  if parsed_query["user_study"] == "yes":
    #write session to db
    session = {"q": text_query, "l": parsed_query["location"],
              "_id": response["sid"], "ur": {}}
    conn = Connection("wheezy.cs.tamu.edu", 27017)
    db = conn["ole_evaluation"]
    db["user_response"].insert(session)
  #print response
  return cjson.encode(response)


@app.route('/submiteval/<evaluation>')
def submiteval(evaluation=None):
  evaluation = cjson.decode(evaluation)
  sid = evaluation["sid"]
  conn = Connection("wheezy.cs.tamu.edu", 27017)
  db = conn["ole_evaluation"]
  ur = {}
  for i in evaluation:
    if "rel" in i or 'compare' in i:
      #result_no = re.sub(r"rel", "", i)
      parts = i.split("_")
      #result_no = i
      result_no = parts[0]
      ur[result_no] = evaluation[i]
      ur["id"] = parts[1]
  print "sid: ", sid
  print ur
  db["user_response"].update({"_id": sid}, {"$set": {"ur": ur}})
  #save response in db as per a session and calculate MAP estimate for the session
  #ur = user response. For each query result: relevant = 1, not relevant = 2, not sure = 3
  return "success"


@app.route('/hello')
def hello():
  return "hello"


@app.route('/usersearch/<query>')
def usersearch(query):
  global user_searcher_

  analyzer = WhitespaceAnalyzer(Version.LUCENE_CURRENT)
  if query is None:
    return {}

  #parse query using lucene parser and get docs
  p_query = QueryParser(Version.LUCENE_CURRENT, "user", analyzer).parse(query)
  print p_query
  scoreDocs = user_searcher_.search(p_query, 50).scoreDocs
  print "%s total matching documents." % len(scoreDocs)
  
  docs = []
  for scoreDoc in scoreDocs:
    doc = user_searcher_.doc(scoreDoc.doc)
    user = doc.get("user")
    profile = UserProfiles.get_user_profile_(user)
    p = {}
    if profile:
      if profile[user]["location"] != None:
        p["hl"] = profile[user]["location"]
        if "status" in profile[user]:
          p["sts"]["text"] = profile[user]["status"]["text"]
          p["sts"]["text"]["created_at"] = profile[user]["status"]["created_at"]
        p["foc"] = profile[user]["followers_count"]
        p["frc"] = profile[user]["friends_count"]
        p["sc"] = profile[user]["statuses_count"]
        p["name"] = profile[user]["name"]
        p["url"] = profile[user]["url"]
        p["des"] = profile[user]["description"]
        docs.append({"user": doc.get("user"), "locations": doc.get("locs"),
                "profile": p})
    else:
      docs.append({"user": doc.get("user"), "locations": doc.get("locs")})
  return cjson.encode(docs)


@app.route('/cognos/<query>')
def cognos(query):
  cognos_url = 'http://twitter-app.mpi-sws.org/whom-to-follow/users.php?'+query
  #cognos_url = 'http://twitter-app.mpi-sws.org/whom-to-follow/users.php?q=beer+houston'
  http = httplib2.Http();
  response, content = http.request(cognos_url, 'GET')
  if response['status'] == '200':
    soup = BeautifulSoup(content)
    search_results = soup.find(id='results')
    footer = soup.find('p', id='footer')
    footer['class'] = 'footer'
    footer['id'] = 'footer_c'
    search_results.center.replace_with('')
    #print str(search_results) + str(footer)
    return str(search_results) + str(footer)
  return ''


def rankDocs(query, searcher_, scoreDocs):
  locations = query["locations"]
  include_profile = query["profile"]
  rankedDocs = []
  users = []
  count = 0
  conn = Connection("wheezy.cs.tamu.edu", 27017)
  db = conn["local_expert_tweets"]
  sorted_locs = sorted(locations.iteritems(), key=lambda k:k[1]["dwt"], reverse=True)
  query_location = sorted_locs[0]
  for scoreDoc in scoreDocs:
    count += 1
    rankedDoc = {}
    doc = searcher_.doc(scoreDoc.doc)
    rankedDoc["user"] = doc.get("user").strip("@")
    rankedDoc["details"] = {}
    rankedDoc["profile"] = {}
    rankedDoc["details"]["ls"] = float(scoreDoc.score) #lucene score

    #number of tweets by location (query location + epsilon)
    num_tweets = cjson.decode(doc.get("num_tweets"))
    
    #total no. of tweets for the locations in query
    rankedDoc["details"]["tnm"] = 0
    for i in num_tweets:
      if i in locations:
        rankedDoc["details"]["tnm"] += num_tweets[i]

    tweets = {}
    def filter_tweets(query_terms, tweet):
      for i in query_terms:
        if i in tweet.lower():
          return True
      return False
    for j in sorted_locs:
      it = db["user_location_tweets"].find({"sn": rankedDoc["user"], "l": j[0]})
      for x in it:
        if query["query_type"] == "t":
          temp = [json.dumps(item) for item in x["t"] if filter_tweets(query["terms"], item)]
          if len(temp) > 0:
            tweets[j[0]] = temp
        else:
          tweets[j[0]] = [json.dumps(item) for item in x["t"]]
        break

    #drop documents which do not have any endorsing tweets
    if not tweets:
      #if DEBUG:
        #print "No endorsing tweets"
      continue
    else:
      rankedDoc["tweets"] = tweets
      users.append(rankedDoc["user"])
      rankedDocs.append(rankedDoc)

  if include_profile == "yes":
    rankedDocs = update_profile_information(users, rankedDocs, locations, query_location)

  rankedDocs = compute_doc_scores(rankedDocs, locations)
  #top 20
  #return rankedDocs[:20]
  #top 15
  return rankedDocs[:15]

def compute_doc_scores(docs, locations):
  for doc in docs:
    """
    if 'lot' not in doc['details']:
      doc['details']['s'] = get_score_basic(doc)
    else:
      doc['details']['s'] = get_score(doc)
    """
    if 'tweets' in doc:
      doc["details"]["lsts"] = compute_lsnts(doc["tweets"], locations)
      doc["details"]["s"] = get_score(doc)
    else:
      doc["details"]["ls"] = get_score_basic(doc)
  rankedDocs = sorted(docs, key=lambda k: k["details"]["s"], reverse=True)
  return rankedDocs

#learn the scoring function later
def get_score(doc):
  p = doc["details"]
  #return float(100*p['ls'] + 2000*p['nts'] + 0.001*p['lot'] + 20000*p['h'] + \
  """
  return float(100*p["ls"] + 0.001*p["lot"] + 20000*p["h"] + \
               0.001*p["foc"] + 0.1*p["frc"] + 0.01*p["sc"])
  """
  try:
    if "h" in p:
      return float(10*p["ls"] + 200*p["lsts"] + 100*p["h"])
    else:
      return float(10*p["ls"] + 200*p["lsts"])
  except:
    print p
    raise

def get_score_basic(doc):
  p = doc["details"]
  #return float(p['ls'] + p['nts'])
  return float(10*p["ls"])

def compute_lsnts(tweets_obj, locations):
  lsnm = 0.0 #location-specific num tweets score
  if locations != None:
    try:
      for i in locations:
        if i in tweets_obj:
          lsnm = lsnm + len(tweets_obj[i])*locations[i]["dwt"]
    except:
      #print i, " ... ", num_tweets_object, " ... ", locations
      pass
  return lsnm

"""
Get nearby locations, given a location and epsilon, from OLE's set of locations
"""
def get_nearby_locations(location, epsilon):
  locations = {location:{"d": 0, "dwt": 1.0, "lat": None, "lng": None}}
  in_db_locations = LI.in_db_locations
  (slat, slng) = LI.get(location, True)
  if slat is not None and slng is not None:
    locations[location]["lat"] = slat
    locations[location]["lng"] = slng
    if epsilon > 0:
      for i in ALL_LOCATIONS:
        if i in in_db_locations and i != location:
          wdl = get_weighted_distance_location(slat, slng,
                                               in_db_locations[i]["lat"],
                                               in_db_locations[i]["lng"],
                                               epsilon)
          if wdl:
            locations[i] = wdl
  return locations

def get_weighted_distance_location(slat, slng, dlat, dlng, epsilon=0):
  l = {}
  d = geo.haversine_dist(slat, slng, dlat, dlng)
  if epsilon > 0:
    if d <= epsilon:
    #dmin = 100KM, alpha = 2 in the monotonic distance weight rreducing formula
      dweight = float((100.0/(d + 100.0))**2.0)
      l = {"d": d, "dwt": dweight, "lat": dlat, "lng": dlng}
  else:
    dweight = float((100.0/(d + 100.0))**2.0)
    l = {"d": d, "dwt": dweight, "lat": dlat, "lng": dlng}
  return l

"""
processes the query to collect all parameters passed
"""
def process_query(query, query_type="t"):
  if query == None:
    return None
  query_obj = {}
  query_obj["query_type"] = query_type
  query = query.replace("+", " ")
  query_parts = query.split("&")
  for i in query_parts:
    query_part = i.split("=")
    if (query_part[0] == "q"):
      if query_type == "l":
        locs = re.split(r"\s*,\s+", query_part[1].lower())
        query_obj["location"] = locs[0]
      else:
        query_obj["terms"] = re.findall(r'"[\w\s]+"|\w+', query_part[1])
        #converting to lower case
        query_obj["terms"] = [x.lower() for x in query_obj["terms"]]
        if DEBUG:
          print "Terms: ", query_obj["terms"]
        """
        if len(query_obj["terms"]) > 1:
          text_query = "(" + " OR ".join(query_obj["terms"]) + ")"
        else:
          text_query = "+" + " OR ".join(query_obj["terms"])
        """
        text_query = " OR ".join(query_obj["terms"])
        query_obj["text_query"] = text_query
    elif (query_part[0] == "l"):
      locs = re.split(r"\s*,\s+", query_part[1].lower())
      query_obj["location"] = locs[0]
    elif (query_part[0] == "e"):
      query_obj["epsilon"] = float(query_part[1])
    elif (query_part[0] == "us"):
      query_obj["user_study"] = query_part[1]
    elif (query_part[0] == "p"):
      query_obj["profile"] = query_part[1]
  return query_obj

"""
populate the search result with users profile information
"""
def update_profile_information(users, docs, locations, query_location):
  unique_users = set()
  f = open("no_profiles.txt", "a+")
  no_profile_accounts = [x.strip() for x in f.readlines()]
  profiles = UserProfiles.get_user_profile_info(users)
  #profiles = OnlineUser.get_location_time_info(users)
  for rd in docs[:]:
    #foursquare removed as we can consider it as a spam account
    #introduced this logic because sometimes the user write the wrong username while referring to a user
    #we get the profile from twitter but need to make sure that the username is corrected
    if rd["user"] not in profiles:
      found = False
      for i in profiles.keys():
        #the above logic might introduce duplicates so we remove those
        if rd["user"] == i.lower():
          rd["user"] = i
          found = True
      if not found:
        if rd["user"] not in no_profile_accounts:
          f.write(rd["user"]+"\n")
        #we could augment the tweets but leaving that for now in case the user already exists
        #but not doing that thinking not many people should be making mistakes so we won't miss
        #out on many endorsements.
        docs.remove(rd)
        continue
    if rd["user"] == "foursquare" or rd["user"] in unique_users:
      docs.remove(rd)
      continue
    unique_users.add(rd["user"])
    """
    #Not dealing with last online time, because of the real time twitter call
    #slowdown. Some profiles might have last online time but we don't want to
    #make it part of ranking yet. Just propose that it can be done.

    if 'last_online' in profiles[rd['user']]:
      rd['details']['lot'] = float(profiles[rd['user']]['last_online'])
      rd['details']['t'] = profiles[rd['user']]['status']['created_at']#last online time
    else:
      rd['details']['lot'] = float(0) #make it something else
    """
    
    rd["details"]["h"] = 0
    rd["profile"] = {}
    if profiles[rd["user"]]["location"] != None:
      rd["profile"]["hl"] = cjson.encode(profiles[rd["user"]]["location"])
      loc = profiles[rd["user"]]["location"].lower()
      for l in locations:
        if l in loc:
          rd["details"]["h"] = locations[l]["dwt"]
          break
      if rd["details"]["h"] == 0:
        coords = None
        if "location_coords" in profiles[rd["user"]]:
          coords = profiles[rd["user"]]["location_coords"]
        else:
          coords = LI.get(loc, True)
        if coords[0] is not None and coords[1] is not None:
          wdl = get_weighted_distance_location(query_location[1]["lat"],
                                               query_location[1]["lng"],
                                               coords[0],
                                               coords[1])
          if wdl:
            rd["details"]["h"] = wdl["dwt"]

    if "status" in profiles[rd["user"]]:
      rd["profile"]["sts"] = {}
      if "geo" in profiles[rd["user"]]["status"] and profiles[rd["user"]]["status"]["geo"] != None:
        geo_field = profiles[rd["user"]]["status"]["geo"]
        try:
          geo_results = Geocoder.reverse_geocode(float(geo_field["coordinates"][0]),
                                               float(geo_field["coordinates"][1]))
          if geo_results:
            rd["profile"]["cl"] = str(geo_results[0])
            rd["profile"]["sts"]["geo"] = str(geo_results[0])
        except GeocoderError as e:
          print str(e)
      rd["profile"]["sts"]["text"] = json.dumps(profiles[rd["user"]]["status"]["text"])
      rd["profile"]["sts"]["created_at"] = json.dumps(profiles[rd["user"]]["status"]["created_at"])
    rd["profile"]["foc"] = profiles[rd["user"]]["followers_count"]
    rd["profile"]["frc"] = profiles[rd["user"]]["friends_count"]
    rd["profile"]["sc"] = profiles[rd["user"]]["statuses_count"]
    rd["profile"]["name"] = profiles[rd["user"]]["name"]
    rd["profile"]["url"] = json.dumps(profiles[rd["user"]]["url"])
    rd["profile"]["des"] = json.dumps(profiles[rd["user"]]["description"])
    #if "profile_image_url" in profiles[rd["user"]]:
      #rd["profile"]["pic"] = json.dumps(profiles[rd["user"]]["profile_image_url"])
    #else:
    url = UserProfiles.get_profile_image_url(rd["user"])
    if url:
      rd["profile"]["pic"] = url
  f.close()
  return docs
 
"""
def run(searcher, analyzer):
  while True:
    print 
    print "Hit enter with no input to quit."
    command = raw_input("Query:")
    if command == '':
      #searcher.close()
      return

    print "Searching for:", command
    query = QueryParser(Version.LUCENE_CURRENT, "w", analyzer).parse(command)
    scoreDocs = searcher.search(query, 50).scoreDocs
    print "%s total matching documents." % len(scoreDocs)
    
    for scoreDoc in scoreDocs:
      doc = searcher.doc(scoreDoc.doc)
      
      for a in cjson.decode(doc.get("ats")):
        expert = Expert('u', a)
        OnlineUser.get_location_time(expert)
      for a in cjson.decode(doc.get("h")):
        expert = Expert('h', a)
        OnlineUser.get_location_time(expert)
      print 'tx:', doc.get('tx')
      print
"""

if __name__ == "__main__":
  
  env=lucene.initVM()
  
  location_index_dir = SimpleFSDirectory(File(location_index_store_dir))
  usermap_index_dir = SimpleFSDirectory(File(usermap_index_store_dir))
  user_index_dir = SimpleFSDirectory(File(user_index_store_dir))
  
  # For now I just use the StandardAnalyzer, but you can change this
  #analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
  
  location_searcher_ = IndexSearcher(location_index_dir)
  usermap_searcher_ = IndexSearcher(usermap_index_dir)
  user_searcher_ = IndexSearcher(user_index_dir)
  
  # start up the terminal query Interface
  #run(searcher, analyzer)
  
  #run(app, host='localhost', port='8080')
  run(app, host='vostro.cs.tamu.edu', port='8080')
