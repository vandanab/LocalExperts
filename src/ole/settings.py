'''
Created on Feb 19, 2013
@author: vandana
OLE settings
'''
import os

basedir = os.path.expanduser('~/workspace/LocalExperts/data/results/%s/')
location_index_store_dir = basedir % 'local_tweets/ole' + 'location_index/'
usermap_index_store_dir = basedir % 'local_tweets/ole' + 'usermap_index/'
dir_user_location_map = basedir % 'local_tweets/ole' + 'input/'
geo_distrib_file = basedir % 'local_tweets/' + 'geo_geo_distrib.txt'

