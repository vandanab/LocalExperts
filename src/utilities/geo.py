'''
Created on Feb 14, 2013
@author: vandana
geo utilities
'''
import cjson
import httplib2
import urllib
from math import radians, cos, sin, atan2, sqrt

def decode(location):
  http = httplib2.Http()
  url = 'http://sarge.csdl.tamu.edu:4296/fulltext/fulltextsearch?q={0}&format=json'.format(urllib.quote_plus(location))
  response, content = http.request(url, 'GET')
  latlong = None
  if response['status'] == '200':
    content = cjson.decode(content)
    if content['responseHeader']['status'] == 0:
      latlong = {}
      docs = content['response']['docs']
      for i in docs:
        if location in i['name'].lower():
          latlong['lat'] = i['lat']
          latlong['lng'] = i['lng']
          latlong['placetype'] = i['placetype']
          latlong['name'] = i['name']
          latlong['fqn'] = i['fully_qualified_name']
          latlong['country_code'] = i['country_code']
          break
  return latlong

def decode_locations(infile):
  f = open(infile, 'r')
  lines = [x.strip() for x in f.readlines()]
  f.close()
  locations_info = {}
  for l in lines:
    info = decode(l)
    if info != {}:
      locations_info[l] = info
    else:
      print l
  return locations_info

def haversine_dist(lat1, lng1, lat2, lng2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    R = 6371 # mean radius of earth in kms
    # convert decimal degrees to radians 
    lng1, lat1, lng2, lat2 = map(radians, [lng1, lat1, lng2, lat2])
    # haversine formula 
    dlon = lng2 - lng1 
    dlat = lat2 - lat1 
    a = (sin(dlat/2))**2 + cos(lat1) * cos(lat2) * (sin(dlon/2))**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    km = R * c
    return km

if __name__ == '__main__':
  print decode('buffalo')