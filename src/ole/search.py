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
from lucene import SimpleFSDirectory, StandardAnalyzer, File
from online import Expert, OnlineUser, UserProfiles
from settings import location_index_store_dir, usermap_index_store_dir
from utilities import geo
#from src.utilities import geo
from pygeocoder import Geocoder
from pymongo import Connection
import pymongo
import cjson
import lucene
import re
import operator
import uuid


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
    location, user_study = "", "no"
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
      elif (query_part[0] == "us"):
        user_study = query_part[1]
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
    rankedDocs = rankDocs(query, location_searcher_, scoreDocs,
                          locations, user_study)
    for i in rankedDocs:
      #doc= i['doc'] # not using doc for now
      details = i['details']
      if user_study:
        experts.append({'u': i['user'], 'd':details, 'p':i['profile']})
      else:
        experts.append({'u': i['user'], 'd':details})
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
    real_query, text_query = "", ""
    location, user_study = "", "no"
    epsilon = 0
    locations = {}
    query = query.replace('+', ' ')
    query_parts = query.split("&")
    for i in query_parts:
      query_part = i.split("=")
      if (query_part[0] == "q"):
        terms = re.split(r",\s*", query_part[1])
        #real_query = real_query + "(" + " OR ".join(terms) + ")"
        text_query = "(" + " OR ".join(terms) + ")"
      elif (query_part[0] == "l"):
        location = query_part[1]
        locations = {location:{"d": 0, "dwt": 1}}
      elif (query_part[0] == "e"):
        epsilon = float(query_part[1])
      elif (query_part[0] == "us"):
        user_study = query_part[1]
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
        """
        locations_query = []
        for l in locations:
          locations_query.append("\""+l+"\"" + "^" + str(locations[l]["dwt"]))
        real_query = real_query + " AND (" + " OR ".join(locations_query) + ")"
        """
        lc = 0
        for l in locations:
          rq = "(" + text_query + " AND " + "\"" + l + "\"" + ")^" + \
                str(locations[l]["dwt"])
          if lc == 0:
            real_query = rq 
          else:
            real_query = real_query + " OR " + rq
          lc += 1
      else:
        #real_query = real_query + " AND " + location
        real_query = text_query + " AND " + location
    else:
      #real_query = real_query + " AND " + location
      real_query = text_query + " AND " + location
    p_query = QueryParser(Version.LUCENE_CURRENT, "text", analyzer).parse(real_query)
    scoreDocs = usermap_searcher_.search(p_query, 500).scoreDocs
    print "%s total matching documents." % len(scoreDocs)
    
    experts = []
    rankedDocs = rankDocs(query, usermap_searcher_, scoreDocs,
                          locations, user_study)
    #sort locations by distance - not required until we have the actual tweets
    
    sorted_locs = sorted(locations.iteritems(), key=lambda k:k[1]['dwt'], reverse=True)
    conn = Connection("wheezy.cs.tamu.edu", 27017)
    db = conn['local_expert_tweets']
    for i in rankedDocs:
      #doc= i['doc'] # not using doc for now
      details = i['details']
      if user_study:
        tweets = []
        for j in sorted_locs:
          it = db['user_location_tweets'].find({'sn': i['user'], 'l': j[0]})
          for x in it:
            tweets.extend(x['t'])
            break
          if len(tweets) > 2:
            break
        experts.append({'u': i['user'], 'd':details, 'p':i['profile'], 't': tweets})
      else:
        experts.append({'u': i['user'], 'd':details})
    
    response = {'sid': str(uuid.uuid1()), 'es': experts}
    if user_study:
      #write session to db
      session = {'q': text_query, 'l': location, '_id': response['sid'], 'ur': {}}
      db = conn['ole_evaluation']
      db['user_response'].insert(session)
		#return cjson.encode(experts)
    return cjson.encode(response)

@app.route('/submiteval/<evaluation>')
def submiteval(evaluation=None):
  evaluation = cjson.decode(evaluation)
  sid = evaluation['sid']
  conn = Connection("wheezy.cs.tamu.edu", 27017)
  db = conn['ole_evaluation']
  ur = {}
  for i in evaluation:
    if 'rel' in i:
      result_no = re.sub(r'rel', '', i)
      ur[result_no] = evaluation[i]
  db['user_response'].update({'_id': sid}, {'$set': {'ur': ur}})
  #save response in db as per a session and calculate MAP estimate for the session
  #ur = user response. For each query result: relevant = 1, not relevant = 2, not sure = 3
  return "success"

@app.route('/hello')
def hello():
  return "hello"

def rankDocs(query, searcher_, scoreDocs, locations=[], user_study="no"):
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
    rankedDoc['profile'] = {}
    rankedDoc['details']['ls'] = float(scoreDoc.score) #lucene score

    #its an object now, so we need to find the exact count/score due of 
    #num_tweets, understand how to use this. Its getting lost in the lucene score as of now.
    num_tweets = cjson.decode(doc.get('num_tweets'))
    #rankedDoc['details']['nts'] = compute_lsnts(num_tweets, locations)
    rankedDoc['details']['nm'] = 0
    for i in num_tweets:
      if i in locations:
        rankedDoc['details']['nm'] += num_tweets[i]
    
    #contact profile db or online query to find if this is user's home location.
    users.append(rankedDoc['user'])
    rankedDocs.append(rankedDoc)

  if user_study == "yes":
    f = open("no_profiles.txt", 'a+')
    no_profile_accounts = [x.strip() for x in f.readlines()]
    profiles = UserProfiles.get_user_profile_info(users) 
    #profiles = OnlineUser.get_location_time_info(users)
    for rd in rankedDocs:
      #shall we remove docs without profiles?
      if rd['user'] not in profiles:
        if rd['user'] not in no_profile_accounts:
          f.write(rd['user']+'\n')
        rankedDocs.remove(rd)
        continue
      """
      #Not dealing with last online time, because of the real time twitter call slowdown. Some profiles might have last online time but we don't want to make it part of ranking yet. Just propose that it can be done.

      if 'last_online' in profiles[rd['user']]:
        rd['details']['lot'] = float(profiles[rd['user']]['last_online'])
        rd['details']['t'] = profiles[rd['user']]['status']['created_at']#last online time
      else:
        rd['details']['lot'] = float(0) #make it something else
			"""
      rd['details']['h'] = 0
      if profiles[rd['user']]['location'] != None:
        rd['profile']['hl'] = profiles[rd['user']]['location']
        loc = profiles[rd['user']]['location'].lower()
        for l in locations:
          if l in loc:
            rd['details']['h'] = 1
            break
      if 'status' in profiles[rd['user']]:
        rd['profile']['sts'] = {}
        if 'geo' in profiles[rd['user']]['status']:
          rd['profile']['sts']['geo'] = profiles[rd['user']]['status']['geo']
          rd['profile']['cl'] = profiles[rd['user']]['status']['geo']
        rd['profile']['sts']['text'] = profiles[rd['user']]['status']['text']
        rd['profile']['sts']['created_at'] = profiles[rd['user']]['status']['created_at']
      rd['profile']['foc'] = profiles[rd['user']]['followers_count']
      rd['profile']['frc'] = profiles[rd['user']]['friends_count']
      rd['profile']['sc'] = profiles[rd['user']]['statuses_count']
      rd['profile']['name'] = profiles[rd['user']]['name']
      rd['profile']['url'] = profiles[rd['user']]['url']
      rd['profile']['des'] = profiles[rd['user']]['description']
    f.close()
  
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
  #return float(100*p['ls'] + 2000*p['nts'] + 0.001*p['lot'] + 20000*p['h'] + \
  return float(100*p['ls'] + 0.001*p['lot'] + 20000*p['h'] + \
               0.001*p['foc'] + 0.1*p['frc'] + 0.01*p['sc'])

def get_score_basic(doc):
  p = doc['details']
  #return float(p['ls'] + p['nts'])
  return float(p['ls'])

def compute_lsnts(num_tweets_object, locations):
  lsnm = 0.0 #location-specific num tweets score
  if locations != None:
    try:
      for i in locations:
        if i in num_tweets_object:
          lsnm = lsnm + num_tweets_object[i]*locations[i]['dwt']
    except:
      #print i, " ... ", num_tweets_object, " ... ", locations
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
  
  #run(app, host='localhost', port='8080')
  run(app, host='vostro.cs.tamu.edu', port='8080')
