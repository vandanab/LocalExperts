'''
Created on Nov 16, 2012

@author: vandana
Gets user_profiles from twitter user lookup api.
'''
import cjson
import httplib2
import sys
import time
import urllib
from utilities.twitter_request_session import RequestSession
from pymongo import Connection

class UserProfile:
  USER_LOOKUP_URL = 'http://api.twitter.com/1/users/lookup.json?' + \
      'screen_name={0}&include_entities=true'
  HTTP = httplib2.Http()
  DB = 'local_experts'
  DB_CONN = Connection('localhost', 27017)
  
  @staticmethod
  def crawl(ids_file, output_file):
    f = open(ids_file, 'r')
    user_ids = [x.strip() for x in f.readlines()]
    f.close()
    f = open(output_file, 'w')
    c = 0
    num_userids = len(user_ids)
    request_ids = []
    rs = RequestSession()
    rs.start_session()
    for i in user_ids:
      request_ids.append(i)
      c += 1
      num_userids -= 1
      if c == 100 or num_userids == 0:
        rs.monitor_requests()
        response = UserProfile.lookup_user(request_ids)
        if response == None:
          print "error occurred. request cannot be completed."
          return
        print "processing response: ", rs.num_requests
        for profile in response:
          f.write(cjson.encode(profile)+'\n')
        request_ids = []
        c = 0
  
  @staticmethod
  def lookup_user(request_ids_list):
    str_ids_list = ','.join(request_ids_list)
    request_url = UserProfile.USER_LOOKUP_URL.format(urllib.quote_plus(str_ids_list))
    while True:
      response, content = UserProfile.HTTP.request(request_url, 'GET')
      try:
        if response['status'] == '200':
          #print content
          profile_list = []
          data = cjson.decode(content)
          for profile in data:
            profile_list.append(UserProfile.get_short_profile(profile))
          #return cjson.decode(content)
          return profile_list
        elif response['status'] == '400':
          print "request monitoring not working..."
          print "response: ", response
          time.sleep(60*60) #sleep for 60 mins before continuing
        elif response['status'] == '502':
          time.sleep(120) #sleep for 2 mins server is busy
        else:
          print "error occurred. no response"
          print "response: ", response
          print "response content: ", content
          return []
      except:
        return
  
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
      if 'place' in profile['status']:
        short_profile['status']['place'] = profile['status']['place']
      if 'coordinates' in profile['status']:
        short_profile['status']['coordinates'] = profile['status']['coordinates']
      if 'geo' in profile['status'] :
        short_profile['status']['geo'] = profile['status']['geo']
    return short_profile
  
  @staticmethod
  def update_profiles(user_mention_map):
    db = UserProfile.DB_CONN[UserProfile.DB]
    f = open(user_mention_map, 'r')
    lines = f.readlines()
    f.close()
    f = open('profile_not_present.txt', 'w')
    for l in lines:
      data = cjson.decode(l)
      locations = [x['name'] for x in data['locations']]
      updated = False
      it = db['user_profiles'].find({'_id': data['user'].strip('@')})
      for profile_in_db in it:
        home_mention = 0
        for loc in locations:
          if 'location' in profile_in_db and profile_in_db['location'] and loc in profile_in_db['location'].lower():
            print profile_in_db['location']
            home_mention = 1
        """
        db['user_profiles'].update({'_id': profile_in_db['_id']},
                                   {'$set':{'locations': data['locations'],
                                            'home_mention': home_mention}})
        """
        somedict = {'locations': data['locations'], 'home_mention': home_mention}
        #print somedict
        updated = True
        break
      if not updated:
        f.write(cjson.encode(data)+'\n')
    f.close()
  
def crawl():
  if len(sys.argv) < 2:
    UserProfile.crawl('i_files/user_ids.txt', 'user_profiles.txt')
  elif len(sys.argv) == 2:
    UserProfile.crawl(sys.argv[1], 'user_profiles.txt')
  else:
    UserProfile.crawl(sys.argv[1], sys.argv[2])
  
def update_with_location_mention():
  UserProfile.update_profiles('../data/results/local_tweets/user_location_map_lang_topcities')
      
def main():
  #crawl()
  update_with_location_mention()

if __name__ == "__main__":
  sys.exit(main())
    
