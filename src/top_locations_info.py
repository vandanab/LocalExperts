'''
Created on Feb 14, 2013
Move top locations geo info to db
@author: vandana
'''
from pymongo import Connection
from utilities.geo import decode_locations

def to_db(infile, db_name):
  conn = Connection('localhost', 27017)
  db = conn[db_name]
  locations_info = decode_locations(infile)
  for i in locations_info:
    locations_info[i]['_id'] = i
    db['location_info'].insert(locations_info[i])
  conn.close()

if __name__ == '__main__':
  to_db('toplocations.txt', 'local_experts')