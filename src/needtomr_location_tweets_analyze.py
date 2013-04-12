'''
various methods to help analyze the location tweets
first we get the tweets from the user_mention_map.
'''
import cjson
import re
import nltk
import subprocess
import operator
#from sklearn.feature_extraction.text import Vectorizer, CountVectorizer
#from sklearn.feature_extraction.text import TfidfVectorizer
from pymongo import Connection
from sklearn.cluster import KMeans
from settings import f_local_tweets, f_local_tweets_filtered, f_tweet_texts, local_clusters_folder, lda, spock_local_base_dir, f_geo_distrib
from utilities import geo
from nltk.corpus import stopwords
from math import log
import matplotlib.pyplot as plt

class LocationTweetsAnalysis:
	"""
	works with the tweets from the location specific tweets collected, 
	and produces a filtered tweets file for analysis.
	"""
	@staticmethod
	def get_tweets(tweets_file, filtered_tweets_outfile, topcities=None):
		topcities_list = []
		tweets_unique = set()
		if topcities != None:
			f2 = open(topcities, 'r')
			topcities_list = [x.lower().strip() for x in f2.readlines()]
			f2.close()
		f1 = open(filtered_tweets_outfile, 'w')
		f = open(tweets_file, 'r')
		for l in f:
			data = cjson.decode(l)
			if len(data['ats']) > 0 and len(data['w']) > 0:
				if data['id'] in tweets_unique:
					continue
				# do topcities based filtering
				locations = []
				for i in data['w']:
					t = i.lower()
					if t in topcities_list:
						locations.append(t)
				if len(locations) > 0:
					data['top_locs'] = locations
					tweets_unique.add(data['id'])
					f1.write(cjson.encode(data) + '\n')
		f.close()
		f1.close()
	
	@staticmethod
	def get_wherecan_tweets(tweets_file, outfile):
		f = open(tweets_file, 'r')
		tweets = []
		for l in f:
			data = cjson.decode(l)
			if 'where can' in data['tx'].lower():
				tweets.append(data)
		f.close()
		f = open(outfile, 'w')
		for t in tweets:
			f.write(cjson.encode(t) + '\n')
		f.close()

	@staticmethod
	def get_tweettexts(tweets_file, outfile, filters=[], lang=None):
		f = open(tweets_file, 'r')
		f1 = open(outfile, 'w')
		f2 = open(outfile+'.full', 'w')
		for l in f:
			data = cjson.decode(l)
			if lang == None or (lang != None and 'lang' in data and data['lang'] == lang):
				data['pro_tx'] = get_processed_tweet(data['tx'], data['top_locs'], filters)
				if data['pro_tx'] != '':
					f1.write(data['pro_tx']+'\n')
					f2.write(cjson.encode(data)+'\n')
		f.close()
		f1.close()
		f2.close()
	
	@staticmethod
	def get_tweettexts_en(tweets_file, outfile):
		LocationTweetsAnalysis.get_tweettexts(tweets_file, outfile, 'en')

	@staticmethod	
	def get_tweettexts_en_from_processed_file(tweets_file, outfile):
		f = open(tweets_file, 'r')
		f1 = open(outfile, 'w')
		f2 = open(outfile+'.full', 'w')
		for l in f:
			data = cjson.decode(l)
			if 'lang' in data and data['lang'] == 'en' and data['pro_tx'] != '':
				better_pro_tx = remove_junk_chars(data['pro_tx'])
				if better_pro_tx != '':
					f1.write(better_pro_tx+'\n')
					f2.write(l)
		f2.close()
		f1.close()
		f.close()

	@staticmethod
	def get_geo_distrib(tweets_file, outfile_geo_distrib, lang=None):
		f = open(tweets_file, 'r')
		geo_dict = {}
		for l in f:
			data = cjson.decode(l)
			if lang == None or (lang != None and 'lang' in data and data['lang'] == lang):
				for i in data['top_locs']:
					if i in geo_dict:
						geo_dict[i]['tweets'].append(data['tx'])
						geo_dict[i]['n_tweets'] += 1
					else:
						obj = geo.decode(i)
						obj['tweets'] = [data['tx']]
						obj['n_tweets'] = 1
						geo_dict[i] = obj
		f.close()
		f = open(outfile_geo_distrib, 'w')
		f.write(cjson.encode(geo_dict))
		f.close()
	
	@staticmethod
	def plot_geo_distrib(geo_distrib_file):
		f = open(geo_distrib_file, 'r')
		geo_dict = cjson.decode(f.readline())
		f.close()
		f1 = open('top20.txt', 'w')
		f2 = open('last20.txt', 'w')
		geo_dict_r = []
		for i in geo_dict:
			geo_dict_r.append((i, geo_dict[i]['n_tweets']))
		geo_dict_sorted = sorted(geo_dict_r, key=operator.itemgetter(1), reverse=True)
		l = len(geo_dict_sorted)
		x = [0] * l
		y = [0] * l
		lim = range(l)
		for i in lim:
			if i >= 0:
				#x[i] = log(i)
				x[i] = i
				(p,q) = geo_dict_sorted[i]
				y[i] = log(q)
				#y[i] = q
				if i < 21:
					f1.write(p+'\t'+str(q)+'\n')
				if i > (l - 20):
					f2.write(p+'\t'+str(q)+'\n')
		f1.close()
		f2.close()
		plt.plot(x, y, color='green', linestyle='solid')
		plt.xlabel('location (by rank)')
		plt.ylabel('no. of tweets')
		plt.show()
	
	@staticmethod
	def plot_geo_distrib1(geo_distrib_file):
		f = open(geo_distrib_file, 'r')
		f1 = open('top40.txt', 'w')
		f2 = open('last40.txt', 'w')
		lines = f.readlines();
		f.close()
		geo_dict_r = []
		for i in lines:
			i = cjson.decode(i)
			geo_dict_r.append((i['name'], i['n_tweets']))
		geo_dict_sorted = sorted(geo_dict_r, key=operator.itemgetter(1), reverse=True)
		l = len(geo_dict_sorted)
		x = [0] * l
		y = [0] * l
		lim = range(l)
		for i in lim:
			x[i] = i
			(p,q) = geo_dict_sorted[i]
			#y[i] = log(q)
			y[i] = q
			if i < 41:
				f1.write(p+'\t'+str(q)+'\n')
			if i > (l - 40):
				f2.write(p+'\t'+str(q)+'\n')
		f.close()
		f1.close()
		f2.close()
		plt.plot(x, y, color='green', linestyle='solid')
		plt.xlabel('location (by rank)')
		plt.ylabel('no. of tweets')
		plt.show()
	
	"""
	@staticmethod
	def find_category_clusters_km(infile, outfolder, k=10):
		f = open(infile, 'r')
		f1 = open(infile+'.full', 'r')
		lines = f.readlines()
		objs = f1.readlines()
		print len(lines),"-", len(objs)
		f.close()
		f1.close()
		#vectorizer = Vectorizer(max_df=0.5, max_features=100000)
		vectorizer = TfidfVectorizer(max_df=0.5, max_features=1000000, stop_words='english')
		X = vectorizer.fit_transform(lines)
		print X.shape
		km = KMeans(n_clusters=k, init='random', max_iter=100, n_init=1, verbose=1)
		km.fit(X)
		for i in range(len(lines)):
			cl_no = km.labels_[i]
			f = open(outfolder + 'cluster' + str(cl_no), 'a')
			#f.write(lines[i])
			f.write(objs[i])
			f.close()
	"""
	@staticmethod
	def find_category_clusters_lda(d, infile, objfile, cluster_outfolder, k=20, twords=50):
		f = open(d+infile, 'r')
		lines = f.readlines()
		f.close()
		f = open(d+infile, 'w')
		f.write(str(len(lines)) + '\n')
		f.writelines(lines)
		f.close()
		subprocess.call([lda, '-est', '-ntopics', str(k), '-twords', str(twords), '-savestep', str(5000), '-dfile', d+infile, '-niters', str(10000)])
		f = open(d+'model-final.theta', 'r')
		f1 = open(d+objfile, 'r')
		for l in f:
			probs = l.split()
			obj = f1.readline()
			probs = [float(x) for x in probs]
			topic = -1
			maxprob = -1
			for i in range(len(probs)):
				if probs[i] > maxprob:
					maxprob = probs[i]
					topic = i
			f2 = open(cluster_outfolder + 'cluster' + str(topic), 'a')
			f2.write(obj)
			f2.close()
		f1.close()
		f.close()
	
	@staticmethod
	def train_for_classification(traindata):
		pass

"""
strip the non-english and other useless characters from the tweets
"""
def get_processed_tweet(tweet, locations, filters=[], stopwords=True):
	text = tweet.lower()
	for n in locations:
		text = text.replace(n, '')
	for i in filters:
		text = text.replace(i, '')
	text = re.sub(r'(@\w+\s?)|(@\s+)', '', text)
	text = re.sub(r'http:[\\/.a-z0-9]+\s?', '', text)
	text = text.encode('ascii', 'ignore')
	text = re.sub(r'[;:\)\(\?\'\"!,.@#-+*/\\]', ' ', text)
	text = ' '.join(text.split())
	if stopwords:
		text = filter_stopwords(text)
	return text.strip()

def remove_junk_chars(text):
	text = re.sub(r'[\^=~\-+_\[\]{}]', ' ', text)
	text = re.split('\s+', text)
	text = ' '.join(text)
	return text.strip()

def filter_stopwords(tweet):
	words = nltk.word_tokenize(tweet)
	final = []
	for w in words:
		if w in stopwords.words('english'):
			continue
		final.append(w)
	return ' '.join(final)

"""
don't need to get tweets from user_mention_map
"""
"""    
def get_tweets(user_location_map_file, outfile):
  f = open(user_location_map_file, 'r')
  f1 = open(outfile, 'w')
  c = 0
  tweets = set()
  for l in f:
    data = cjson.decode(l)
    for location in data['locations']:
      for twt in location['tweets']:
        #tweets.add(twt['tx']+' '+location['name'])
        #tweets.add(twt['tx'].replace(location['name'], ""))
        if '@' in twt['tx']:
          #tweets.add(twt['tx'])
          locs = [lc['name'].lower() for lc in data['locations']]
          t = get_processed_tweet(twt['tx'], locs)
          tweets.add(t)
  f.close()
  for i in set(tweets):
    f1.write(cjson.encode(i)+'\n')
  f1.close()
"""

"""
check whether the locations associated with a user because of mentions matches with the location in their profiles.
"""
def user_location_mapping_verify(user_location_mentions):
	conn = Connection('localhost', 27017)
	db = conn['local_experts']
	f = open(user_location_mentions, 'r')
	users = f.readlines()
	cmatch = count = 0.0
	for u in users:
		data = cjson.decode(u)
		locations = [j['name'] for j in data['locations']]
		# print locations
		it = db['user_profiles'].find({'_id': data['user'].strip('@')})
		# print data['user'],
		for i in it:
			if profile_location_matches(i, locations):
				cmatch += 1
		count += 1
		"""
		if count == 100:
			break
		"""
	print float(cmatch / count)
	f.close()

def profile_location_matches(profile, locations):
	for i in locations:
		l = i.lower()
		if (profile['location'] != None and l in profile['location'].lower()) or ('status' in profile and profile['status'] != None and l in cjson.encode(profile['status']).lower()):
			print profile	
			return True
	return False

def main():
	# user_location_mapping_verify('i_files/potential_location_experts_red_scope.txt')

	#tweets analysis directly from tweets file
	#first level filter - topcities
	#LocationTweetsAnalysis.get_tweets(f_local_tweets, f_local_tweets_filtered, 'toplocations.txt')
	
	#second level filter and clustering
	#LocationTweetsAnalysis.get_tweettexts(f_local_tweets_filtered, f_tweet_texts)
	#LocationTweetsAnalysis.find_category_clusters_km(f_tweet_texts, local_clusters_folder, 50)
	#LocationTweetsAnalysis.get_tweettexts_en_from_processed_file(f_tweet_texts+'.full.0', f_tweet_texts)
	LocationTweetsAnalysis.find_category_clusters_lda(spock_local_base_dir%'local_tweets', 'tweets_text', 'tweets_text.full', local_clusters_folder+'v-final/', k=50)

	#where can analysis
	#LocationTweetsAnalysis.get_wherecan_tweets(f_local_tweets_filtered, 'wherecan.txt')
	#LocationTweetsAnalysis.get_tweettexts('wherecan.txt', 'temp/wherecan_processtext.txt', ['where can'])
	#LocationTweetsAnalysis.find_category_clusters_km('temp/wherecan_processtext.txt', local_clusters_folder+'k_wherecan/', 50)
	#LocationTweetsAnalysis.find_category_clusters_lda('temp/', 'wherecan_processtext.txt', 'wherecan_processtext.txt.full', local_clusters_folder+'wherecan/', k=50)
	
	#trying classification
	#LocationTweetsAnalysis.train_for_classification()
	#LocationTweetsAnalysis.classify_tweets()
	
	#geo analysis
	#LocationTweetsAnalysis.get_geo_distrib(f_local_tweets_filtered, 'geo_distrib.txt')
	#LocationTweetsAnalysis.plot_geo_distrib('geo_distrib.txt')
	
	#LocationTweetsAnalysis.plot_geo_distrib1('geo_distrib1.txt')
	

if __name__ == '__main__':
	main()
