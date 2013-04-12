'''
Created on Oct 9, 2012

@author: vandana
It heuristically determines the location specific qa tweets from the location
tweets.
Uses the pattern "where can" for now.
'''
from settings import f_local_tweets, f_local_qa_tweets
import re

class LocationQATweets:
    patterns = ['.*where can\s+.*']
    
    @staticmethod
    def get_qa_tweets(inputfile, outputfile):
        re_expr = "|".join(LocationQATweets.patterns)
        re_c = re.compile(re_expr)
        f1 = open(outputfile, 'w')
        f = open(inputfile, 'r')
        for l in f:
            if re_c.match(l) != None:
                f1.write(l)
        f.close()
        f1.close()
    
    @staticmethod
    def percent_tweets_local(inputfile):
        f = open(inputfile, 'r')
        re_expr = r'.*@\w+.*|.*#\w+.*'
        re_c = re.compile(re_expr)
        total_twts = c_local_qa_at_hash_twts = 0.0
        for l in f:
            total_twts += 1
            if re_c.match(l):
                c_local_qa_at_hash_twts += 1
        f.close()
        print "Pecentage qa tweets with ats and hash:" + \
                str((c_local_qa_at_hash_twts/total_twts)*100)
     
if __name__ == "__main__":
    #LocationQATweets.get_qa_tweets(f_local_tweets, f_local_qa_tweets)
    LocationQATweets.percent_tweets_local(f_local_qa_tweets)
