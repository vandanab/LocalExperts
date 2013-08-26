'''
@author: vandana
Map reduce for tweets analysis
'''

from library.mrjobwrapper import runMRJob
from settings import f_local_tweets, f_geo_distrib, f_tweet_texts, f_local_tweets_filtered, f_ulm
from location_tweets_analyze import TweetsGeoAnalysis, TweetTexts, \
                                    UsersByMentions, LocationsByMentions, LocationUserPairs#, TopicClusters
import os

class TweetsAnalysisMRJobRunner(object):
  @staticmethod
  def geo_analysis(input_files):
    mr_class = TweetsGeoAnalysis
    output_file = f_geo_distrib
    runMRJob(mr_class,
             output_file,
             input_files,
             mrJobClassParams = {'job_id': 'as'},
             # uncomment when running on local
             #args = [],
             jobconf={'mapred.reduce.tasks':300, 'mapred.task.timeout': 86400000}
    )
  
  @staticmethod
  def tweet_texts(input_files):
    mr_class = TweetTexts
    output_file = f_tweet_texts
    runMRJob(mr_class,
             output_file,
             input_files,
             mrJobClassParams = {'job_id': 'as'},
             # uncomment when running on local
             #args = [],
             jobconf={'mapred.reduce.tasks':300, 'mapred.task.timeout': 86400000}
    )
  
  @staticmethod
  def users_by_mentions(input_files):
    mr_class = UsersByMentions
    output_file = os.path.expanduser('~/LocalExperts/data/results/%s/') % 'local_tweets' + 'usersbymentions'
    runMRJob(mr_class,
             output_file,
             input_files,
             mrJobClassParams = {'job_id': 'as'},
             # uncomment when running on local
             #args = [],
             jobconf={'mapred.reduce.tasks':300, 'mapred.task.timeout': 86400000}
    )
  
  @staticmethod
  def locations_by_mentions(input_files):
    mr_class = LocationsByMentions
    output_file = os.path.expanduser('~/LocalExperts/data/results/%s/') % 'local_tweets' + 'locationuserpairs'
    runMRJob(mr_class,
             output_file,
             input_files,
             mrJobClassParams = {'job_id': 'as'},
             # uncomment when running on local
             #args = [],
             jobconf={'mapred.reduce.tasks':300, 'mapred.task.timeout': 86400000}
    )
  
  @staticmethod
  def location_user_pairs(input_files):
    mr_class = LocationUserPairs
    output_file = os.path.expanduser('~/LocalExperts/data/results/%s/') % 'local_tweets' + 'locationsbymentions'
    runMRJob(mr_class,
             output_file,
             input_files,
             mrJobClassParams = {'job_id': 'as'},
             # uncomment when running on local
             #args = [],
             jobconf={'mapred.reduce.tasks':300, 'mapred.task.timeout': 86400000}
    )
  
  """
  @staticmethod
  def topic_clusters_geo_division(input_files):
    mr_class = TopicClusters
    #TODO: update output_file
    output_file = ""
    runMRJob(mr_class,
             output_file,
             input_files,
             mrJobClassParams = {'job_id': 'as'},
             # uncomment when running on local
             args = [],
             jobconf={'mapred.reduce.tasks':300, 'mapred.task.timeout': 86400000}
    )
    """
    
  @staticmethod
  def run():
    input_files = []
    #input_files.append(f_local_tweets)
    #input_files.append(f_local_tweets_filtered)
    #TweetsAnalysisMRJobRunner.geo_analysis(input_files)
    
    #input_files.append(f_local_tweets_filtered)
    #TweetsAnalysisMRJobRunner.tweet_texts(input_files)
    
    #TODO: get input files from cluster folder
    #TweetsAnalysisMRJobRunner.topic_clusters_geo_division(input_files)
    
    input_files.append(f_ulm)
    #TweetsAnalysisMRJobRunner.users_by_mentions(input_files)
    TweetsAnalysisMRJobRunner.locations_by_mentions(input_files)
    #TweetsAnalysisMRJobRunner.location_user_pairs(input_files)

if __name__ == '__main__':
  TweetsAnalysisMRJobRunner.run()
