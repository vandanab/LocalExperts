'''
Created on Feb 14, 2013
@author: vandana
geo utilities
'''
import cjson
import httplib2
import sys
import urllib
from math import radians, cos, sin, atan2, sqrt
from pygeocoder import Geocoder, GeocoderError
from pymongo import Connection

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

"""
Get coordinates using geocoder
"""
def get_coordinates_gc(location):
  try:
    results = Geocoder.geocode(location)
    if results:
      return results[0].coordinates
  except GeocoderError as e:
    print 'Geo Error: ', str(e)
  except:
    print sys.exc_info()[0]
  return None

class LocationInfo:
  def __init__(self):
    self.db_conn = Connection('localhost', 27017)
    self.location_db = self.db_conn['local_experts']
    self.collection = 'location_info'
    self.in_db_locations = {}
    it = self.location_db[self.collection].find()
    for i in it:
      self.in_db_locations[i["_id"]] = i
  
  def get(self, location, try_geocoder=False):
    location = location.lower()
    for i in self.in_db_locations:
      if i in location:
        return (self.in_db_locations[i]['lat'], self.in_db_locations[i]['lng'])
    if try_geocoder:
      results = get_coordinates_gc(location)
      if results:
        lat, lng = results[0], results[1]
        self.add(location, {'name': location, 'lat': lat, 'lng': lng})
        return (lat, lng)
    return (None, None)
  
  def add(self, name, location_info):
    location_info['_id'] = name
    self.location_db[self.collection].insert(location_info)
    self.in_db_locations[name] = location_info

if __name__ == '__main__':
  print decode('buffalo')
