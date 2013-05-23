'''
Created on Oct 7, 2012

@author: vandana
Contains the settings used by LocalTweets project 
'''
import os

#base dirs
hdfs_input_folder = 'hdfs:///user/kykamath/geo/hashtags/%s/'
hdfs_input_folder1 = 'hdfs:///user/vbachani/data/results/%s/'
localfs_input_folder = '/home/vandana/infolab/research/data/%s/' 

#localfs_output_base_dir = '/home/vandana/infolab/research/data/results/%s/'
localfs_output_base_dir = os.path.expanduser('~/workspace/LocalExperts/data/results/%s/')
chevron_output_base_dir = '/mnt/chevron/vbachani/data/results/%s/'
spock_local_base_dir = os.path.expanduser('~/LocalExperts/data/results/%s/')

#module specific
#map-reduce related settings
input_folder = hdfs_input_folder % 'world'
local_tweets_input_folder = localfs_input_folder % 'local_tweets'

#input when script for user_mention_map is run locally
#f_local_tweets = localfs_output_base_dir % 'local_q_tweets' + 'tweets'

#input when script for user_mention_map is run using chevron data
#f_local_tweets = chevron_output_base_dir % 'local_tweets' + 'tweets'

#input when script for user_mention_map run on spock locally
f_local_tweets = spock_local_base_dir % 'local_tweets' + 'tweets_'

#input when user_mention_map run from data in dfs
#f_local_tweets = hdfs_input_folder1 % 'local_tweets' + 'tweets'

#input when user_mention_map is run on dfs data with language field also marked in tweets
#f_local_tweets = spock_local_base_dir % 'local_tweets' + 'tweets_modified'
#f_local_tweets = localfs_output_base_dir % 'local_tweets' + 'tweets_modified'
#f_local_tweets = localfs_output_base_dir % 'local_tweets' + 'tweets_'

#f_mentions = chevron_output_base_dir % 'local_tweets' + 'user_location_map'
#f_mentions = localfs_output_base_dir % 'local_tweets' + 'user_location_map'
#f_mentions = spock_local_base_dir % 'local_tweets' + 'user_location_map'
#f_mentions = spock_local_base_dir % 'local_tweets' + 'user_location_map_langinfo'
f_mentions = spock_local_base_dir % 'local_tweets' + 'user_location_map_ner'

#outfile for filtered tweets to aid in clustering to understand the topics
#f_tweet_texts = spock_local_base_dir % 'local_tweets' + 'tweets_text'
#f_tweet_texts = localfs_output_base_dir % 'local_tweets' + 'tweets_text'
f_tweet_texts = localfs_output_base_dir % 'local_tweets' + 'tagged_tweet_texts'
#f_local_tweets_filtered = spock_local_base_dir % 'local_tweets' + 'tweets_for_analysis'
#f_local_tweets_filtered = localfs_output_base_dir % 'local_tweets' + 'tweets_for_analysis'
#f_local_tweets_filtered = localfs_output_base_dir % 'local_tweets' + 'tweets_for_analysis_filtered'
#f_local_tweets_filtered = localfs_output_base_dir % 'local_tweets' + '02-13-2013/tweets_for_analysis'
#f_local_tweets_filtered = hdfs_input_folder1 % 'local_tweets' + 'tweets_for_analysis_modified'
#f_local_tweets_filtered = hdfs_input_folder1 % 'local_tweets' + 'tweets_for_analysis'
f_local_tweets_filtered = hdfs_input_folder1 % 'local_tweets' + 'tweets_for_analysis_filtered_ner'


f_geo_distrib = localfs_output_base_dir % 'local_tweets' + 'geo_distrib.txt'
#f_geo_distrib = spock_local_base_dir % 'local_tweets' + 'geo_distrib.txt'

local_clusters_folder = os.path.expanduser('~/workspace/LocalExperts/data/clusters/')

f_local_qa_tweets = chevron_output_base_dir % 'local_q_tweets' + 'tweets'

#miscellaneous
lda = os.path.expanduser('~/GibbsLDA++-0.2/src/lda')

#ole related settings"
index_store_dir = localfs_output_base_dir % 'local_tweets' + "index"
fs_local_tweets = localfs_output_base_dir % 'local_tweets'
