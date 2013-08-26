'''
Created on Aug 3, 2013
@author: vandana
Some miscellaneous functions and trying few things
'''
from pymongo import Connection
from nltk.corpus import stopwords
import csv
import nltk
import operator
import os
import pickle
import re

def armstrong_tweets(tweets_file):
  loc_map = {}
  tweets = []
  locs = ["houston", "paris", "sydney", "texas", "iowa", "orlando",
          "hawaii", "london", "chicago", "panama", "florida", "austin",
          "france", "boston", "seattle", "italy", "dallas", "buffalo"]
  if not os.path.exists(tweets_file):
    conn = Connection("wheezy.cs.tamu.edu", 27017)
    db = conn["local_expert_tweets"]
    it = db["user_location_tweets"].find({"sn": "lancearmstrong"})
    for i in it:
      if i["l"] not in locs:
        continue
      if i["l"] in loc_map:
        loc_map[i["l"]].extend(i["t"])
      else:
        loc_map[i["l"]] = i["t"]
    f = open(tweets_file, "w")
    pickle.dump(loc_map, f)
    f.close()
  else:
    f = open(tweets_file, "r")
    loc_map = pickle.load(f)
    f.close()
  for i in loc_map:
    tweets.extend(loc_map[i])
  tweets_dict = create_dict(tweets, locs=locs)
  user_loc_map = {}
  num_tweets = 0
  for i in loc_map:
    tweet_ids = [num_tweets+j for j in range(len(loc_map[i]))]
    num_tweets += len(loc_map[i])
    user_loc_map[i] = {"t": tweet_ids}
  f = open("lance_lstsm.csv", "w")
  csvwriter = csv.writer(f)
  row = [""]
  row.extend(locs)
  csvwriter.writerow(row)
  for i in tweets_dict:
    row = [i]
    for j in locs:
      tweet_ids = user_loc_map[j]
      lsts = 0
      for (x,y) in tweets_dict[i]:
        if x in tweet_ids["t"]:
          lsts += y
      row.append(lsts)
    csvwriter.writerow(row)
  f.close()

def tebow_tweets():
  conn = Connection("wheezy.cs.tamu.edu", 27017)
  db = conn["local_expert_tweets"]
  it = db["user_location_tweets"].find({"sn": "TimTebow"})
  loc_profile = {}
  for i in it:
    loc_profile[i["l"]] = len(i["t"])
  sorted_loc_profile = sorted(loc_profile.iteritems(),
                              key=operator.itemgetter(1),
                              reverse=True)
  print sorted_loc_profile[:25]

def create_dict(texts, stopwords=True, locs=None):
  text = " ".join(texts)
  text = re.sub(r"(@\w+\s?)|(@\s+)", "", text)
  text = re.sub(r"http:.*\s+", "", text)
  text = text.encode("ascii", "ignore")
  text = re.sub(r"[;:\)\(\?\'\"!,.@#\-+*/\\0-9]", " ", text)
  text = " ".join(text.split())
  if stopwords:
    text = filter_stopwords(text, locs)
  words = nltk.word_tokenize(text)
  texts_dict = {}
  for i in words:
    texts_dict[i] = []
  for i in range(len(texts)):
    for j in texts_dict:
      if j in texts[i].lower():
        texts_dict[j].append((i, texts[i].lower().count(j)))
  return texts_dict

def filter_stopwords(tweet, locs=None):
  words = nltk.word_tokenize(tweet)
  final = set()
  for w in words:
    if w in stopwords.words("english") or len(w) <= 2:
      continue
    if locs and w.lower() in locs:
      continue
    final.add(w.lower())
  return ' '.join(final)

def main():
  #armstrong_tweets("armstrong_tweets_map.txt")
  tebow_tweets()

if __name__ == "__main__":
  main()
    
