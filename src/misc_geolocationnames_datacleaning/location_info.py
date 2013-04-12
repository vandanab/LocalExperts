'''
Created on Mar 29, 2013
@author: vandana
Updating correct location info in the database (the latlong information for the
locations set, top200locations.txt
'''
from pymongo import Connection
from pygeocoder import Geocoder, GeocoderError
import math
import time

def update(infile, db_name, coll_name):
  f = open(infile, 'r')
  locations = [x.strip() for x in f.readlines()]
  f.close()
  conn = Connection("localhost", 27017)
  db = conn[db_name]
  g = Geocoder()
  count = 0
  for l in locations:
		try:
			results = g.geocode(l)
			count += 1
			print count
			if results:
				(lat, lng) = results[0].coordinates
				fqn = results.formatted_address
				it = db[coll_name].find({'_id': l})
				is_present = False
				for i in it:
					is_present = True
					if math.fabs(i['lat'] - lat) >= 1 or math.fabs(i['lng'] - lng) >= 1:
						print l
						print "lat: ", i['lat'], " lng: ", i['lng'], " ", i['fqn']
						print "geocoder: lat: ", lat, " lng: ", lng
						print "Choose geocoder value (y/n): "
						ch = raw_input()
						if ch == "y":
							db[coll_name].update({'_id': l},
                                   {'$set': {'lat': lat, 'lng': lng, 'fqn': fqn}})
				if not is_present:
					db[coll_name].insert({'_id': l, 'lat': lat, 'lng': lng, 'fqn': fqn})
			time.sleep(20)
		except GeocoderError as e:
			print e
			#raise

def main():
  update("top200locations.txt", "local_experts", "location_info")

if __name__ == "__main__":
  main()
