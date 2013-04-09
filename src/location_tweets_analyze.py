'''
various methods to help analyze the location tweets
first we get the tweets from the user_mention_map.
'''
import cjson
import re
import nltk
from nltk.corpus import stopwords
from library.mrjobwrapper import ModifiedMRJob

class TweetsGeoAnalysis(ModifiedMRJob):
	DEFAULT_INPUT_PROTOCOL = 'raw_value'
	
	def __init__(self, *args, **kwargs):
		super(TweetsGeoAnalysis, self).__init__(*args, **kwargs)
	
	"""
	mapper for geo distribution (just counts)
	"""
	"""
	def mapper(self, key, line):
		data = cjson.decode(line)
		locations = data['top_locs']
		for l in locations:
			yield l, data['tx']
	"""
	
	"""
	mapper for geo distribution - geo information
	"""
	def mapper(self, key, line):
		data = cjson.decode(line)
		locations = data['top_locs']
		for l in locations:
			if 'geo' in data:
				yield l, data['geo']
	
	
	"""
	reducer for geo distribution (just counts)
	"""
	"""
	def reducer(self, key, values):
		geo_mention_tweets = {}
		geo_mention_tweets['name'] = key
		count = 0
		for _ in values:
			count += 1
		geo_mention_tweets['n_tweets'] = count
		yield key, geo_mention_tweets
	"""
	
	"""
	reducer for geo distribution - geo information
	"""
	def reducer(self, key, values):
		location_geo_following = {}
		location_geo_following['name'] = key
		geo_tags_from = set();
		for i in values:
			geo_tags_from.add((i[0], i[1]))
		location_geo_following['mentions_from'] = list(geo_tags_from)
		yield key, location_geo_following

class TweetTexts(ModifiedMRJob):
	DEFAULT_INPUT_PROTOCOL = 'raw_value'
	
	def __init__(self, *args, **kwargs):
		super(TweetTexts, self).__init__(*args, **kwargs)
	
	#for processing tweet texts
	def mapper(self, key, line):
		data = cjson.decode(line)
		data['pro_tx'] = self.get_processed_tweet(data['tx'], data['top_locs'])
		yield key, data
	
	#for creating a separate file of tweet texts
	"""
	def mapper(self, key, line):
		data = cjson.decode(line)
		yield key, data['pro_tx']
	"""
	
	def get_processed_tweet(self, tweet, locations, filters=[], stopwords=True):
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
			text = self.filter_stopwords(text)
		return text.strip()
	
	def filter_stopwords(self, tweet):
		words = nltk.word_tokenize(tweet)
		final = []
		for w in words:
			if w in stopwords.words('english'):
				continue
			final.append(w)
		return ' '.join(final)

"""
class TopicClusters(ModifiedMRJob):
	DEFAULT_INPUT_PROTOCOL = 'raw_value'
	
	def __init__(self, *args, **kwargs):
		super(TopicClusters, self).__init__(*args, **kwargs)
	
	def configure_options(self):
		super(TopicClusters, self).configure_options()
		self.add_file_option('--locations', default='top200locations.txt')
	
	#for processing tweet texts
	def mapper(self, key, line):
		data = cjson.decode(line)
		for i in data['top_locs']:
			if i in self.locations:
				yield i, 1
	
	def reducer(self, key, values):
		location_geo_following = {}
		location_geo_following['name'] = key
		geo_tags_from = set();
		for i in values:
			geo_tags_from.add((i[0], i[1]))
		location_geo_following['mentions_from'] = list(geo_tags_from)
		yield key, location_geo_following
"""

if __name__ == '__main__':
	#TweetsGeoAnalysis.run()
	TweetTexts.run()