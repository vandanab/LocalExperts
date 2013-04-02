"""
get user ids for crawling user_profiles
"""

import cjson
from pymongo import Connection
from settings import f_mentions

class UserIds:
	CONN = Connection('localhost', 27017)
	DB_NAME = 'local_experts'
	
	def get(self, user_location_map_file, users_crawled, users_tocrawl):
		f = open(user_location_map_file, 'r')
		f1 = open(users_crawled, 'w')
		f2 = open(users_tocrawl, 'w')
		db = UserIds.CONN[UserIds.DB_NAME]
		for l in f:
			try:
				data = cjson.decode(l)
				u = data['user'].strip('@')
				q_result = db['user_profiles'].find({'_id': u})
				is_present = False
				for _ in q_result:
					f1.write(u+'\n')
					is_present = True
				if not is_present:
					f2.write(u+'\n')
			except:
				pass
		f.close()
		f1.close()
		f2.close()

def main():
	uids = UserIds()
	uids.get(f_mentions, 'ids_crawled.txt', 'ids_tocrawl.txt')

if __name__ == '__main__':
	main()