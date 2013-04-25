# coding: utf-8
'''
Created on Feb 23, 2013
@author: vandana
Location coverage request handler
'''
from bottle import run, Bottle
from settings import geo_distrib_file
from pymongo import Connection
import cjson

if __name__ == "__main__":
  lp = None
  app = Bottle()

@app.route('/coverage/<location>')
def coverage(location=None):
  response = {}
  if location != None:
    location = location.replace('+', ' ')
    location = location.lower().strip()
    conn = Connection('localhost', 27017)
    db = conn['local_experts']
    ls = db['location_info'].find({'_id': location})
    for it in ls:
      response['name'] = it['name']
      response['latlng'] = [it['lat'], it['lng']]
      break
    f = open(geo_distrib_file, 'r')
    for l in f:
      data = cjson.decode(l)
      if data['name'] == location:
        mentions = []
        for i in data['mentions_from']:
          point = {}
          point['lat'] = i[0]
          point['lng'] = i[1]
          point['count'] = 1
          mentions.append(point)
        response['mentions_from'] = mentions
        break
    f.close()
  return response
    
if __name__ == "__main__":
  run(app, host='localhost', port='8081')
