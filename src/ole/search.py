# coding: utf-8
'''
Created on Oct 11, 2012

@author: vandana
Ole search interface

There is nothing in a caterpillar that tells you it’s going to be a butterfly.
R. Buckminster Fuller
'''

from bottle import run, Bottle
from lucene import Version, QueryParser, IndexSearcher
from lucene import SimpleFSDirectory, StandardAnalyzer, File
from online import Expert, OnlineUser
from settings import location_index_store_dir, usermap_index_store_dir
from src.utilities import geo
from pygeocoder import Geocoder
import pymongo
import cjson
import lucene
import re
import operator

if __name__ == "__main__":
  location_searcher_ = None
  usermap_searcher_ = None
  #all the locations in our dataset
  f = open("top200locations.txt", "r")
  all_locations = [x.strip() for x in f.readlines()]
  f.close()
  app = Bottle()

"""
@app.route('/search/<query>')
def search(query=None):
  global searcher_
  # For now I just use the StandardAnalyzer, but you can change this
  analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
  if query == None:
    return []
  else:
    p_query = QueryParser(Version.LUCENE_CURRENT, "w", analyzer).parse(query)
    scoreDocs = searcher_.search(p_query, 50).scoreDocs
    print "%s total matching documents." % len(scoreDocs)
    
    experts = []
    for scoreDoc in scoreDocs:
      doc = searcher_.doc(scoreDoc.doc)
      for a in cjson.decode(doc.get("ats")):
        expert = Expert('u', a)
        resp = OnlineUser.get_location_time(expert)
        if resp:
          experts.append({'n': a, 't': '@', 'cl': resp['cl'], 'ca': resp['ca'],
                          'tx': doc.get('tx'), 'w': doc.get('w')})
      for a in cjson.decode(doc.get("h")):
        expert = Expert('h', a)
        resp = OnlineUser.get_location_time(expert)
        if resp:
          experts.append({'n': a, 't': '#', 'cl': resp['cl'], 'ca': resp['ca'],
                          'tx': doc.get('tx'), 'w': doc.get('w')})
      #print 'tx:', doc.get('tx')
    #print experts
    return cjson.encode(experts)
"""
@app.route('/search/<query>')
def search(query=None):
  global location_searcher_
  # For now I just use the StandardAnalyzer, but you can change this
  analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
  if query == None:
    return []
  else:
    real_query = ""
    location = ""
    epsilon = 0
    locations = {}
    query = query.replace('+', ' ')
    query_parts = query.split("&")
    for i in query_parts:
      query_part = i.split("=")
      if (query_part[0] == "q"):
        location = query_part[1]
        locations = {location:{"d": 0, "dwt": 1}}
      elif (query_part[0] == "e"):
        epsilon = float(query_part[1])
    if(epsilon > 0):
      conn = pymongo.Connection("localhost", 27017)
      db = conn["local_experts"]
      in_db_locations = {}
      it = db["location_info"].find()
      for i in it:
        in_db_locations[i["_id"]] = i
      results = Geocoder.geocode(location)
      if results:
        (slat, slng) = results[0].coordinates
        for i in all_locations:
          if i in in_db_locations:
            d = geo.haversine_dist(slat, slng,
                                       in_db_locations[i]["lat"],
                                       in_db_locations[i]["lng"])
            if d <= epsilon and i != location:
              #dmin = 100KM, alpha = 2 in the monotonic distance weight reducing formula
              dweight = float((100.0/(d + 100.0))**2.0)
              locations[i] = {"d": d, "dwt": dweight}
        locations_query = []
        for l in locations:
          locations_query.append(l + "^" + str(locations[l]["dwt"]))
        real_query = real_query + " OR ".join(locations_query)
      else:
        real_query = real_query + location
    else:
      real_query = real_query + location
    p_query = QueryParser(Version.LUCENE_CURRENT, "loc", analyzer).parse(real_query)
    scoreDocs = location_searcher_.search(p_query, 1000).scoreDocs
    print "%s total matching documents." % len(scoreDocs)
    
    experts = []
    rankedDocs = rankDocs(query, location_searcher_, scoreDocs, locations)
    for i in rankedDocs:
      #doc= i['doc'] # not using doc for now
      details = i['details']
      experts.append({'u': i['user'], 'd':details})
    #print cjson.encode(experts)
    return cjson.encode(experts)

#TODO: both functions can be merged (based on some query parameter)

@app.route('/textsearch/<query>')
def textsearch(query=None):
  global usermap_searcher_
  # For now I just use the StandardAnalyzer, but you can change this
  analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
  if query == None:
    return []
  else:
    real_query = ""
    location = ""
    epsilon = 0
    locations = {}
    query = query.replace('+', ' ')
    query_parts = query.split("&")
    for i in query_parts:
      query_part = i.split("=")
      if (query_part[0] == "q"):
        terms = re.split(r",\s*", query_part[1])
        real_query = real_query + "(" + " OR ".join(terms) + ")"
      elif (query_part[0] == "l"):
        location = query_part[1]
        locations = {location:{"d": 0, "dwt": 1}}
      elif (query_part[0] == "e"):
        epsilon = float(query_part[1])
    if(epsilon > 0):
      conn = pymongo.Connection("localhost", 27017)
      db = conn["local_experts"]
      in_db_locations = {}
      it = db["location_info"].find()
      for i in it:
        in_db_locations[i["_id"]] = i
      results = Geocoder.geocode(location)
      if results:
        (slat, slng) = results[0].coordinates
        for i in all_locations:
          if i in in_db_locations:
            d = geo.haversine_dist(slat, slng,
                                       in_db_locations[i]["lat"],
                                       in_db_locations[i]["lng"])
            if d <= epsilon and i != location:
              #dmin = 100KM, alpha = 2 in the monotonic distance weight reducing formula
              dweight = float((100.0/(d + 100.0))**2.0)
              locations[i] = {"d": d, "dwt": dweight}
        locations_query = []
        for l in locations:
          locations_query.append(l + "^" + str(locations[l]["dwt"]))
        real_query = real_query + " AND (" + " OR ".join(locations_query) + ")"
      else:
        real_query = real_query + " AND " + location
    else:
      real_query = real_query + " AND " + location
    p_query = QueryParser(Version.LUCENE_CURRENT, "text", analyzer).parse(real_query)
    scoreDocs = usermap_searcher_.search(p_query, 500).scoreDocs
    print "%s total matching documents." % len(scoreDocs)
    
    experts = []
    #sort locations by distance - not required until we have the actual tweets
    #locations = sorted(locations.iteritems(), key=operator.itemgetter(1)['dwt'])
    rankedDocs = rankDocs(query, usermap_searcher_, scoreDocs, locations)
    for i in rankedDocs:
      #doc= i['doc'] # not using doc for now
      details = i['details']
      experts.append({'u': i['user'], 'd':details})
    return cjson.encode(experts)

@app.route('/hello')
def hello():
  return "hello"

def rankDocs(query, searcher_, scoreDocs, locations=None):
  rankedDocs = []
  users = []
  count = 0
  for scoreDoc in scoreDocs:
    count += 1
    rankedDoc = {}
    doc = searcher_.doc(scoreDoc.doc)
    rankedDoc['doc'] = doc
    rankedDoc['user'] = doc.get('user').strip('@')
    rankedDoc['details'] = {}
    rankedDoc['details']['ls'] = float(scoreDoc.score) #lucene score
    #its an object now, so we need to find the exact count/score due of num_tweets
    num_tweets = cjson.decode(doc.get('num_tweets'))
    rankedDoc['details']['nts'] = compute_lsnts(num_tweets, locations)
    #contact profile db or online query to find if this is user's home location.
    users.append(rankedDoc['user'])
    rankedDocs.append(rankedDoc)
  
  """
  profiles = OnlineUser.get_location_time_info(users)
  num_location_profiles = 0.0
  num_home_location_matches = 0.0
  for rd in rankedDocs:
    if rd['user'] not in profiles:
      continue
    if 'last_online' in profiles[rd['user']]:
      rd['details']['lot'] = float(profiles[rd['user']]['last_online'])
      rd['details']['t'] = profiles[rd['user']]['status']['created_at']#last online time
    else:
      rd['details']['lot'] = float(0) #make it something else
    rd['details']['h'] = 0
    locations = []
    if profiles[rd['user']]['location'] != None:
      rd['details']['hl'] = profiles[rd['user']]['location']
      locations = profiles[rd['user']]['location'].lower().split(',')
      num_location_profiles += 1
    for l in locations:
      if l in query:
        rd['details']['h'] = 1
        num_home_location_matches += 1
        break
    if 'status' in profiles[rd['user']] and 'geo' in profiles[rd['user']]['status']:
      rd['details']['cl'] = profiles[rd['user']]['status']['geo']
    rd['details']['foc'] = profiles[rd['user']]['followers_count']
    rd['details']['frc'] = profiles[rd['user']]['friends_count']
    rd['details']['sc'] = profiles[rd['user']]['statuses_count']
  print "% of home location matches: ",
  #print float((num_home_location_matches/num_location_profiles)*100)
  """
  
  rankedDocs = compute_doc_scores(rankedDocs)
  #sending top 20 for now. to check how does the result look like
  #print rankedDocs[:20]
  return rankedDocs[:50]

def compute_doc_scores(docs):
  for doc in docs:
    if 'lot' not in doc['details']:
      doc['details']['s'] = get_score_basic(doc)
    else:
      doc['details']['s'] = get_score(doc)
  rankedDocs = sorted(docs, key=lambda k: k['details']['s'], reverse=True)
  return rankedDocs

#learn the scoring function later
def get_score(doc):
  p = doc['details']
  return float(100*p['ls'] + 2000*p['nts'] + 0.001*p['lot'] + 20000*p['h'] + \
               0.001*p['foc'] + 0.1*p['frc'] + 0.01*p['sc'])

def get_score_basic(doc):
  p = doc['details']
  return float(p['ls'] + 20*p['nts'])
  #return float(p['ls'])

def compute_lsnts(num_tweets_object, locations):
  lsnm = 0.0 #location-specific num tweets score
  if locations != None:
    try:
      for i in locations:
        lsnm = lsnm + num_tweets_object[i]*locations[i]['dwt']
    except:
      print i, " ... ", num_tweets_object, " ... ", locations
      pass
  return lsnm

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
  
  # For now I just use the StandardAnalyzer, but you can change this
  #analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
  
  location_searcher_ = IndexSearcher(location_index_dir)
  usermap_searcher_ = IndexSearcher(usermap_index_dir)
  
  # start up the terminal query Interface
  #run(searcher, analyzer)
  
  run(app, host='localhost', port='8080')