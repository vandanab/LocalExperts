'''
Created on Oct 12, 2012

@author: vandana
Searches twitter to get real time and location info of identified experts.
'''

import cjson
import httplib2
import urllib
from datetime import datetime
import time

class OnlineUser:
  USER_SEARCH_URL = 'http://search.twitter.com/search.json?rpp=2&' + \
      'q={0}&page=1&include_entities=true&result_type=mixed'
  
  USERS_SEARCH_URL = 'https://api.twitter.com/1/users/lookup.json?' + \
      'screen_name={0}&include_entities=true'
  
  @staticmethod
  def get_location_time_info(users):
    http = httplib2.Http()
    users = list(set(users))
    l = len(users)/100
    r = len(users)%100
    if r > 0:
      l += 1
    p = 0
    profiles = {}
    for i in range(l):
      q = ','.join([x.strip('@') for x in users[p:(i+1)*100]])
      url = OnlineUser.USERS_SEARCH_URL.format(urllib.quote_plus(q))
      response, content = http.request(url, 'GET')
      if response['status'] == '200':
        content = cjson.decode(content)
        for profile in content:
          profiles[profile['screen_name']] = OnlineUser.get_short_profile(profile)
      else:
        print 'REQUEST FAILED: ', url
        print response
        if response['status'] == 400: #ratelimit exceeded
          break
      p = (i+1)*100
    return profiles
        
  @staticmethod
  def get_short_profile(profile):
    short_profile = {
                     'followers_count': profile['followers_count'],
                     'location': profile['location'],
                     'statuses_count': profile['statuses_count'],
                     'description': profile['description'],
                     'friends_count': profile['friends_count'],
                     'screen_name': profile['screen_name'],
                     'favourites_count': profile['favourites_count'],
                     'name': profile['name'],
                     'url': profile['url'],
                     'created_at': profile['created_at'],
                     'listed_count': profile['listed_count'],
                     'id': profile['id'],
                     'time_zone': profile['time_zone'],
                     'created_at': profile['created_at'],
                     'lang': profile['lang']
                     }
    if 'status' in profile:
      short_profile['status'] = {'text':profile['status']['text'],
                                 'created_at': profile['status']['created_at'],
                                 'in_reply_to_screen_name': profile['status']['in_reply_to_screen_name'],
                                 'entities': {'urls': [x['expanded_url'] for x in profile['status']['entities']['urls']], 'hashtags': profile['status']['entities']['hashtags'], 'user_mentions': [x['screen_name'] for x in profile['status']['entities']['user_mentions']]},
                                 'retweet_count': profile['status']['retweet_count']
                                 }
      last_online = time.strptime(profile['status']['created_at'],
                                  "%a %b %d %H:%M:%S +0000 %Y")
      short_profile['last_online'] = time.mktime(last_online) 
      if 'place' in profile['status']:
        short_profile['status']['place'] = profile['status']['place']
      if 'coordinates' in profile['status']:
        short_profile['status']['coordinates'] = profile['status']['coordinates']
      if 'geo' in profile['status'] :
        short_profile['status']['geo'] = profile['status']['geo']
    return short_profile
      
    
  @staticmethod
  def get_location_time(expert):
    """
    q = ''
    if expert.ishash:
      q = '#' + expert.v
    else:
      q = 'from:' + expert.v.strip('@')
    """
    q = 'from:' + expert.strip('@')
    searchurl = OnlineUser.USER_SEARCH_URL.format(urllib.quote_plus(q))
    
    http = httplib2.Http()
    response, content = http.request(searchurl, 'GET')
    if response['status'] == '200':
      content = cjson.decode(content)
      results = content['results']
      online_resp = {}
      if len(results) > 0:
        #print expert.v,
        online_resp['cl'] = results[0]['geo']
        online_resp['ca'] = results[0]['created_at']
        if results[0]['geo'] != None:
          #print " current_location: ", results[0]['geo'],
          online_resp['cl'] = results[0]['geo']
        if results[0]['created_at'] != None:
          #print " recent tweet at: ", results[0]['created_at'],
          online_resp['ca'] = results[0]['created_at']
        #print
        return online_resp

class Expert:
  def __init__(self, etype, value):
    if etype == 'h':
      self.ishash = True
    else:
      self.ishash = False
    self.v = value