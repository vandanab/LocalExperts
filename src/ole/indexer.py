'''
Created on Oct 11, 2012

@author: vandana
The indexer for the Ole search engine.
'''

from lucene import SimpleFSDirectory, File, Document, Field, \
    StandardAnalyzer, IndexWriter, Version
from settings import location_index_store_dir, usermap_index_store_dir, dir_user_location_map
import cjson
import lucene
import os
import time

#import threading, signal

# no threading for now
#class Indexer(threading.Thread):
class Indexer(object):
  # set some initial values for the class,
  # the root directory to start indexing and pass in a writer instance
  def __init__(self, root, writer, directoryToWalk):
    #threading.Thread.__init__(self)
    self.root = root
    self.writer = writer
    self.directory = directoryToWalk

  def run(self):
    #env.attachCurrentThread()
    # begin the index
    self.indexDocs()
    # sleep for a bit
    time.sleep(3)
    """
    for dirname, dirnames, filenames in os.walk(self.directory):
      
      for subdirname in dirnames:

        # the first directory to index
        self.root = os.path.join(dirname, subdirname)
        print "Adding the folder: ", self.root

        # call the indexer
        print "in run"
    """ 

  # start indexing beginning at the root directory
  def indexDocs(self):
    for root, _, filenames in os.walk(self.directory):
      for filename in filenames:
        try:
          path = os.path.join(root, filename)
          print path
          f = open(path, 'r')
          # every line in the file is a tweet document to be indexed
          for line in f:
            tweet = cjson.decode(line)
            doc = Document()
            doc.add(Field("tx", tweet['tx'], Field.Store.YES,
                          Field.Index.NOT_ANALYZED))
            doc.add(Field("h", cjson.encode(tweet['h']), Field.Store.YES,
                          Field.Index.NOT_ANALYZED))
            doc.add(Field("t", cjson.encode(tweet['t']), Field.Store.YES,
                          Field.Index.NOT_ANALYZED))
            doc.add(Field("ats", cjson.encode(tweet['ats']), Field.Store.YES,
                          Field.Index.NOT_ANALYZED))
            """
            doc.add(Field('w', tweet['w'], Field.Store.NO, Field.Index.ANALYZED))
            """
            
            doc.add(Field("w", " ".join(tweet['w']), Field.Store.NO,
                    Field.Index.ANALYZED))
            
            doc.add(Field("user", cjson.encode(tweet['user']), Field.Store.YES,
                          Field.Index.NOT_ANALYZED))
            self.writer.addDocument(doc)
          f.close()
        except Exception, e:
          print "Failed in indexDocs:", e
        #reader = writer.getReader()
        #addedDoc = reader.document(0)
        #print addedDoc
        #time.sleep(20)

    # optimize for fast search and commit the changes
    self.writer.optimize()
    self.writer.commit()
  
class LocationIndexer(Indexer):
  def __init__(self, root, writer, directoryToWalk):
    super(LocationIndexer, self).__init__(root, writer, directoryToWalk)
  
  def indexDocs(self):
    #from user_location_map
    for root, _, filenames in os.walk(self.directory):
      for filename in filenames:
        try:
          path = os.path.join(root, filename)
          print path
          f = open(path, 'r')
          # every line in the file is a user_location_map entry
          for line in f:
            data = cjson.decode(line)
            doc = Document()
            locations = [x['name'] for x in data['locations']]
            num_tweets = {}
            for i in data['locations']:
              num_tweets[i['name']] = len(i['tweets'])
            doc.add(Field("loc", " ".join(locations),
                          Field.Store.YES,
                          Field.Index.ANALYZED))
            doc.add(Field("num_tweets", cjson.encode(num_tweets),
                          Field.Store.YES,
                          Field.Index.NO))
            doc.add(Field("user", data['user'], Field.Store.YES,
                          Field.Index.NO))
            self.writer.addDocument(doc)
          f.close()
        except Exception, e:
          print "Failed in indexLocations:", e
        #reader = writer.getReader()
        #addedDoc = reader.document(0)
        #print addedDoc
        #time.sleep(20)

    # optimize for fast search and commit the changes
    self.writer.optimize()
    self.writer.commit()
  
class UserMapIndexer(Indexer):
  def __init__(self, root, writer, directoryToWalk):
    super(UserMapIndexer, self).__init__(root, writer, directoryToWalk)

  def indexDocs(self):
    for root, _, filenames in os.walk(self.directory):
      for filename in filenames:
        try:
          path = os.path.join(root, filename)
          print path
          f = open(path, 'r')
          # every line in the file is a user_location_map document to be indexed
          for line in f:
            data = cjson.decode(line)
            doc = Document()
            locations = [x['name'] for x in data['locations']]
            num_tweets = {}
            tweets = {}
            tw_texts = []
            for i in data['locations']:
              tweets[i['name']] = [x['tx'] for x in i['tweets']]
              tw_texts.extend(tweets[i['name']])
              num_tweets[i['name']] = len(i['tweets'])
            doc.add(Field("loc", " ".join(locations),
                        Field.Store.NO,
                        Field.Index.ANALYZED))
            doc.add(Field("text", " ".join(tw_texts), Field.Store.NO,
                        Field.Index.ANALYZED))
            """
            Instead of storing into lucene we need to write the tweets to a db.
            We do that here. Thats our supplementary index.
            doc.add(Field("tweets", cjson.encode(tweets), Field.Store.YES,
                        Field.Index.NO))
            """
            doc.add(Field("num_tweets", cjson.encode(num_tweets),
                        Field.Store.YES,
                        Field.Index.NO))
            doc.add(Field("user", data['user'], Field.Store.YES,
                        Field.Index.NO))
            self.writer.addDocument(doc)
          f.close()
        except Exception, e:
          print "Failed in indexDocs:", e
        #reader = writer.getReader()
        #addedDoc = reader.document(0)
        #print addedDoc
        #time.sleep(20)

    # optimize for fast search and commit the changes
    self.writer.optimize()
    self.writer.commit()

"""
  #Alternate implementation above which might solve the LSTS problem.
  def indexDocs(self):
    for root, _, filenames in os.walk(self.directory):
      for filename in filenames:
        try:
          path = os.path.join(root, filename)
          print path
          f = open(path, 'r')
          # every line in the file is a user_location_map document to be indexed
          for line in f:
            data = cjson.decode(line)
            doc = Document()
            
            #maybe indexed as part of the tweet texts
            doc.add(Field("locs", " ".join([n['name'] for n in data['locations']]),
                          Field.Store.NO,
                          Field.Index.ANALYZED))
            
            tweets = []
            c = 0
            for l in data['locations']:
              c += len(l['tweets'])
              tw_texts = " ".join([x['tx'] for x in l['tweets']])
              tweets.append(tw_texts)
            doc.add(Field("tweets", " ".join(tweets), Field.Store.NO,
                    Field.Index.ANALYZED))
            
            doc.add(Field("num_tweets", str(c), Field.Store.YES,
                    Field.Index.NO))
            doc.add(Field("num_locs", str(len(data['locations'])),
                    Field.Store.YES,
                    Field.Index.NO))
            doc.add(Field("user", data['user'], Field.Store.YES,
                    Field.Index.NO))
            self.writer.addDocument(doc)
          f.close()
        except Exception, e:
          print "Failed in indexDocs:", e
        #reader = writer.getReader()
        #addedDoc = reader.document(0)
        #print addedDoc
        #time.sleep(20)

    # optimize for fast search and commit the changes
    self.writer.optimize()
    self.writer.commit()
"""

# before we close we always want to close the writer to prevent
# corruption to the index
def quit_gracefully(*args):
  writer.close()
  print "Cleaning up and terminating"
  exit(0)

def createIndexDir(index_store_dir):
  if not os.path.exists(index_store_dir):
    os.mkdir(index_store_dir)
  return SimpleFSDirectory(File(index_store_dir))

if __name__ == '__main__':
  """ old ole --->
  # always declare the signal handler first
  #signal.signal(signal.SIGINT, quit_gracefully)
  
  #TODO: make it multi-threaded later
  
  fd = '/home/vandana/infolab/research/%s/'
  index_store_dir = fd % 'ole/local_tweets' + 'index'
  fs_local_tweets = fd % 'data/results/local_q_tweets'
  
  STORE_DIR = index_store_dir

  env=lucene.initVM()
  print 'Using Directory: ', STORE_DIR

  notExist = 0
        
  # both the main program and the background indexer will share the
  # same directory and analyzer
  if not os.path.exists(STORE_DIR):
    os.mkdir(STORE_DIR)
    notExist = 1

  directory = SimpleFSDirectory(File(STORE_DIR))

  # what directory we want to index
  directoryToWalk = fs_local_tweets

  # For now I just use the StandardAnalyzer, but you can change this
  analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)

  # we will need a writer
  writer = IndexWriter(directory, analyzer, True,
                       IndexWriter.MaxFieldLength.LIMITED)
  writer.setMaxFieldLength(1048576)

  #if notExist == 1:
  #  writer.close()

  # and start the indexer
  # note the indexer thread is set to daemon causing it to terminate on a SIGINT
  indexer = Indexer(STORE_DIR, writer, directoryToWalk)
  indexer.run()
  # running as foreground process
  #indexer.setDaemon(True)
  #indexer.start()
  #print 'Starting Indexer in background...'

  # If return from Searcher, then call the signal handler to clean up the indexer cleanly
  quit_gracefully()
  """
  
  # what directory we want to index
  directoryToWalk = dir_user_location_map
  
  env=lucene.initVM()
  # For now I just use the StandardAnalyzer, but you can change this
  analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
  """
  #location indexer
  LOCATION_INDEX_STORE_DIR = location_index_store_dir
  location_index_dir = createIndexDir(LOCATION_INDEX_STORE_DIR)
  # we will need a writer
  writer = IndexWriter(location_index_dir, analyzer, True,
                       IndexWriter.MaxFieldLength.LIMITED)
  writer.setMaxFieldLength(1048576)
  # and start the indexer
  location_indexer = LocationIndexer(LOCATION_INDEX_STORE_DIR, writer, directoryToWalk)
  location_indexer.run()
  writer.close()
  """
  #user_location_map indexer
  USERMAP_INDEX_STORE_DIR = usermap_index_store_dir
  usermap_index_dir = createIndexDir(USERMAP_INDEX_STORE_DIR)
  # we will need a writer
  writer = IndexWriter(usermap_index_dir, analyzer, True,
                       IndexWriter.MaxFieldLength.LIMITED)
  writer.setMaxFieldLength(1048576)
  # and start the indexer
  usermap_indexer = UserMapIndexer(USERMAP_INDEX_STORE_DIR, writer, directoryToWalk)
  usermap_indexer.run()
  writer.close()
  
  print "Cleaning up and terminating"
  exit(0)