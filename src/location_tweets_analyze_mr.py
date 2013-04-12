'''
@author: vandana
Map reduce for tweets analysis
'''

from library.mrjobwrapper import runMRJob
from settings import f_local_tweets, f_geo_distrib, f_tweet_texts, f_local_tweets_filtered
from location_tweets_analyze import TweetsGeoAnalysis, TweetTexts#, TopicClusters

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
    input_files.append(f_local_tweets)
    TweetsAnalysisMRJobRunner.geo_analysis(input_files)
    
    #input_files.append(f_local_tweets_filtered)
    #TweetsAnalysisMRJobRunner.tweet_texts(input_files)
    
    #TODO: get input files from cluster folder
    #TweetsAnalysisMRJobRunner.topic_clusters_geo_division(input_files)

if __name__ == '__main__':
  TweetsAnalysisMRJobRunner.run()
