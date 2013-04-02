'''
Created on Nov 18, 2012

@author: vandana
Tracks no. of twitter requests made per hour by the crawler to respect
twitter rate limits.
'''
import time

class RequestSession:
  DEFAULT_TIME_INTERVAL = 60*60 # 1 hr in seconds
  DEFAULT_NUM_REQUESTS_ALLOWED = 140
  def __init__(self, num_requests_allowed=None, time_interval=None):
    self.num_requests = 0
    self.time = 0
    if num_requests_allowed:
      self.num_requests_allowed = num_requests_allowed
    else:
      self.num_requests_allowed = self.DEFAULT_NUM_REQUESTS_ALLOWED
    
    if time_interval:
      self.time_interval = time_interval
    else:
      self.time_interval = self.DEFAULT_TIME_INTERVAL
  
  def start_session(self):
    self.start_time = time.time()
  
  def monitor_requests(self):
    if self.num_requests == self.num_requests_allowed:
      curr_time = time.time()
      if(curr_time - self.start_time) <= self.time_interval:
        print "curr_time: %s. sleeping for an hr, before starting to re-crawl" % curr_time
        time.sleep(60*60)
      self.num_requests = 0
      self.start_time = time.time()
    else:
      self.num_requests += 1
