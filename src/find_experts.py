"""
From user_mention_map it finds those users and their tweets are associated with the top 1000 cities
as given in toplocations.txt. We call them potential_location_experts with reduced scope.
Also we are considering 3 or more tweets per location (might have to remove this restriction)
Also it takes the user profiles and user ids file and writes them to the local mongo db
database: local_experts, table: user_profiles.
"""
import cjson
from pymongo import Connection

def find(infile, outfile):
	lfile = open('topcities.txt', 'r')
	locations = [x.strip() for x in lfile.readlines()]
	f = open(infile, 'r')
	fo = open(outfile, 'w')
	for l in f:
		data = cjson.decode(l)
		data_to_write = {'user': data['user'], 'locations': []}
		for i in data['locations']:
			if len(i['tweets']) >= 3 and i['name'] in locations:
				data_to_write['locations'].append(i)
		if len(data_to_write['locations']) > 0:
			fo.write(cjson.encode(data_to_write)+'\n')
	f.close()
	fo.close()

def filter_profiles_todb(user_ids_file, user_profiles_file):
	conn = Connection('localhost', 27017)
	db = conn['local_experts']
	f = open(user_ids_file, 'r')
	ids = set(f.readlines())
	f1 = open(user_profiles_file, 'r')
	for l in f1:
		data = cjson.decode(l)
		if data['screen_name'] in ids:
			data['_id'] = data['screen_name']
			db['user_profiles'].insert(data)
	f1.close()
	f.close()

def main():
	#find('../data/results/local_tweets/user_location_map', 'potential_location_experts_red_scope.txt')
	filter_profiles_todb('i_files/user_ids_r_scope.txt', 'user_profiles.txt')

if __name__ == '__main__':
	main()
