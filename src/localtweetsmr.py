'''
Created on Oct 7, 2012

@author: vandana
Map reduce to get tweets containing location
'''
from datetime import datetime
from library.mrjobwrapper import runMRJob
from settings import f_local_tweets, input_folder, f_mentions, f_local_tweets_filtered
from utilities import fs
from localtweets import LocalTweets, UserMentionTweets, FilterTweets

class LocalTweetsMRJobRunner(object):
  @staticmethod
  def local_tweets(input_files_start_time, input_files_end_time):
    mr_class = LocalTweets
    output_file = f_local_tweets
    runMRJob(mr_class,
             output_file,
             # uncomment when running on local
             #fs.get_local_input_files(local_tweets_input_folder),
						 fs.get_dated_input_files(input_files_start_time,
                                      input_files_end_time,
                                      input_folder),
						 mrJobClassParams = {'job_id': 'as'},
             # uncomment when running on local
             #args = [],
             jobconf={'mapred.reduce.tasks':300, 'mapred.task.timeout': 86400000}
    )
    
  @staticmethod
  def user_mention_map(input_files):
    mr_class = UserMentionTweets
    output_file = f_mentions
    runMRJob(mr_class,
             output_file,
             input_files,
             mrJobClassParams = {'job_id': 'as'},
             # uncomment when running on local
             #args = [],
             jobconf={'mapred.reduce.tasks':500, 'mapred.task.timeout': 86400000}
    )
  
  @staticmethod
  def filter_tweets(input_files):
    mr_class = FilterTweets
    output_file = f_local_tweets_filtered
    runMRJob(mr_class,
             output_file,
             input_files,
             mrJobClassParams = {'job_id': 'as'},
             # uncomment when running on local
             #args = [],
             jobconf={'mapred.reduce.tasks':500, 'mapred.task.timeout': 86400000}
    )
    
  @staticmethod
  def run():
    """
    input_files_start_time, input_files_end_time = \
                            datetime(2012, 1, 1), datetime(2012, 10, 31)
    LocalTweetsMRJobRunner.local_tweets(input_files_start_time,
                                        input_files_end_time)
    """
    input_files = []
    #input_files.append(f_local_tweets)
    input_files.append(f_local_tweets_filtered)
    #LocalTweetsMRJobRunner.filter_tweets(input_files)
    LocalTweetsMRJobRunner.user_mention_map(input_files)

if __name__ == '__main__':
  LocalTweetsMRJobRunner.run()
