#!/usr/bin/env python
# encoding: utf-8
'''
Created on Oct 3, 2012

@author: vandana
Gets local tweets
'''

from library.mrjobwrapper import ModifiedMRJob
import cjson
import nltk
import re

class LocalTweets(ModifiedMRJob):
  DEFAULT_INPUT_PROTOCOL = 'raw_value'
  AT_RE = re.compile(r"@\w+")

  def configure_options(self):
    super(LocalTweets, self).configure_options()
    #self.add_file_option('--locations', default='locations.txt')
    self.add_file_option('--locations', default='toplocations.txt')
  
  def __init__(self, *args, **kwargs):
    super(LocalTweets, self).__init__(*args, **kwargs)
    self.locs = set([x.strip().lower() for x in open(self.options.locations, 'r').readlines()])

  def mapper(self, key, line):
    data = cjson.decode(line)
    tx = data['tx'].lower()
    loc_words = []
    for i in self.locs:
      try:
        if i in tx:
          loc_words.append(i)
      except:
        continue
    """
    words = self.tokenize(tx)
    for word in words:
      if word in self.locs:
        loc_words.append(word)
    """
    if len(loc_words) > 0:
      at_mentions = self.AT_RE.findall(data['tx'])
      if len(at_mentions) > 0:
        data['ats'] = at_mentions
        data['w'] = loc_words
        yield key, data
      

  def tokenize(self, text):
    pattern = r'''(\w+, [A-Z][A-Z])|([A-Z]\.)+|\w+(-\w+)*'''
    tokens_r = nltk.regexp_tokenize(text, pattern)
    return tokens_r

class UserMentionTweets(ModifiedMRJob):
  DEFAULT_INPUT_PROTOCOL = 'raw_value'
  
  def __init__(self, *args, **kwargs):
    super(UserMentionTweets, self).__init__(*args, **kwargs)
  
  def mapper(self, key, line):
    data = cjson.decode(line)
    ats = data['ats']
    #locs = data['top_locs']
    locs = data['ner_locs']
    for i in ats:
      for j in locs:
        if 'lang' in data:
          tweetmention = {'name': j, 'tweet': {'tx': data['tx'], 't': data['t'],
			'id': data['id'], 'lg': data['lang']}}
        else:
          tweetmention = {'name': j, 'tweet': {'tx': data['tx'], 't': data['t'],
			'id': data['id']}}
        yield i, tweetmention
    """
    for i in data['h']:
      for j in locs:
        if 'lang' in data:
          hashmention = {'name': j, 'tweet': {'tx': data['tx'], 't': data['t'],
																							'id': data['id'], 'lg': data['lang']}}
        else:
          hashmention = {'name': j, 'tweet': {'tx': data['tx'], 't': data['t'],
																							'id': data['id']}}
        yield i, hashmention
    """
  
  def reducer(self, key, values):
    mentions = {}
    mentions['user'] = key
    mentions['locations'] = []
    added = False
    for value in values:
      lname = value['name']
      for j in mentions['locations']:
        if(j['name'] == lname):
          j['tweets'].append(value['tweet'])
          added = True
          break
      if not added:
        mentions['locations'].append({'name': lname, 'tweets': [value['tweet']]})
      added = False
    yield key, mentions

class FilterTweets(ModifiedMRJob):
  DEFAULT_INPUT_PROTOCOL = 'raw_value'
  
  def configure_options(self):
    super(FilterTweets, self).configure_options()
    #self.add_file_option('--locations', default='locations.txt')
    self.add_file_option('--locations', default='top200locations.txt')
  
  def __init__(self, *args, **kwargs):
    super(FilterTweets, self).__init__(*args, **kwargs)
    self.locs = set([x.strip().lower() for x in open(self.options.locations, 'r').readlines()])
  
  def mapper(self, key, line):
    data = cjson.decode(line)
    top_locs = []
    for l in data['w']:
      if l in self.locs:
        top_locs.append(l)
    if len(top_locs) > 0:
      data['top_locs'] = top_locs
      yield key, data
    
if __name__ == '__main__':
  #LocalTweets.run()
  UserMentionTweets.run()
  #FilterTweets.run()
    
