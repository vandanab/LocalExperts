'''
Created on Mar 28, 2013
@author: vandana
We have found topic clusters in the local experts data. This module presents
different ways of representing the clusters and different visualizations.
'''

from settings import local_clusters_folder, local_tweets_input_folder
import matplotlib.pyplot as plt
import cjson
import operator
import os
import pickle

#Visualize LDA generated topics
class Topics:
  def __init__(self, model_folder, clusters_folder, num_topics, cluster_labels, 
              output_folder=None):
    self.num_topics = num_topics
    self.cluster_labels = cluster_labels
    self.model_location = model_folder
    self.clusters_location = clusters_folder
    if output_folder:
      self.geo_topic_matrices = output_folder + "geo_topic_matrices"
      self.topic_cluster_sizes = output_folder + "topic_cluster_sizes"
    else:
      self.geo_topic_matrices = local_tweets_input_folder + "geo_topic_matrices"
      self.topic_cluster_sizes = local_tweets_input_folder + "topic_cluster_sizes"
    
  
  """
  generates two matrices (tranpose of each other)
  which contain the number of tweets about topic in a particular location
  """
  def generate_geo_cluster_matrix(self, locations_file):
    f = open(locations_file, "r")
    locations = [x.strip() for x in f.readlines()]
    f.close()
    clusters_geo_matrix = [] #num_topics X num_geo_locations
    #num_geo_locations X num_topics
    geo_clusters_matrix = self.initialize_geo_vector(locations, True)  
    for (root, _, files) in os.walk(self.clusters_location):
      for f in files:
        cluster_row = self.initialize_geo_vector(locations)
        cluster_no = int(f.replace("cluster", ""))
        cfile = open(root+"/"+f, "r")
        for line in cfile:
          data = cjson.decode(line)
          for i in data["top_locs"]:
            if i in locations:
              cluster_row[i] += 1
              geo_clusters_matrix[i][cluster_no] += 1
        cfile.close()
        clusters_geo_matrix.append(cluster_row)
    f = open(self.geo_topic_matrices, "w")
    pickle.dump({"cgm": clusters_geo_matrix, "gcm": geo_clusters_matrix}, f)
    f.close()
  
  """
  generates the fancy plots for which location is famous for which topic
  """
  def topics_distribution(self, clusters_to_represent=[]):
    f = open(self.geo_topic_matrices, "r")
    matrix = pickle.load(f)
    f.close()
    cgm = matrix["cgm"]
    for i in range(self.num_topics):
      if len(clusters_to_represent) > 0 and i not in clusters_to_represent:
        continue
      geo_distrib = cgm[i]
      X = geo_distrib.keys()
      Y = geo_distrib.values()
      #plt.plot(Y, color="#3DA63F", linestyle="solid")
      #plt.plot(Y, color="#3C7E80", linestyle="solid")
      plt.plot(Y, color="#FF6136", linestyle="solid")
      #plt.fill_between(range(len(X)), Y, facecolor="#43F746", interpolate=True)
      #plt.fill_between(range(len(X)), Y, facecolor="#76E1E3", interpolate=True)
      plt.fill_between(range(len(X)), Y, facecolor="#FFFF36", interpolate=True)
      labelled_points = 0
      for j in range(len(X)):
        if Y[j] >= 1000:
          ry, r1, r2, r3, r4, r5, r6, r7 = 20, -20, -30, -40, -50, -60, -70, 30
          ry = r1 if j%8 == 1 else (r2 if j%8 == 2 else (r3 if j%8 == 3 else (r4 if j%8 == 4 else (r5 if j%8 == 5 else (r6 if j%8 == 6 else (r7 if j%8 == 7 else ry))))))
          plt.annotate(X[j], xy=(j, 0), xycoords="data",
                       xytext=(0, ry), textcoords="offset points",
											 size=9,
											 #bbox=dict(boxstyle="round", fc=(1.0, 0.7, 0.7), ec=(1., .5, .5)),
											 bbox=dict(boxstyle="round", fc=(1., 0.7, 1.), ec=(0.5, 0., 0.5)),
                       #arrowprops=dict(arrowstyle="wedge,tail_width=0.4", fc=(1.0, 0.7, 0.7), ec=(1., .5, .5), patchA=None, relpos=(0.2, 0.8), connectionstyle="arc3,rad=-0.1"))
                       arrowprops=dict(arrowstyle="wedge,tail_width=0.4", fc=(1., .7, 1.), ec=(.5, 0., .5), patchA=None, relpos=(0.2, 0.8), connectionstyle="arc3,rad=-0.1"))
          labelled_points += 1
      plt.ylabel("size of cluster")
      plt.xlabel("locations")
      plt.title(self.cluster_labels[i])
      plt.show()
  
  """
  generates the topic distribution of top topics for specified locations (pie)
  """
  def geo_cluster_pie(self, output_folder, clusters_to_represent=[], locations_to_represent=[]):
    f = open(self.geo_topic_matrices, "r")
    matrix = pickle.load(f)
    f.close()
    gcm = matrix["gcm"]
    pie_files = {}
    for i in gcm:
      if len(locations_to_represent) > 0 and i not in locations_to_represent:
        continue
      clusters_distrib = gcm[i]
      top5, top5_clabels = [], []
      if len(clusters_to_represent) > 0:
        indexes = range(len(clusters_distrib))
        clusters_distrib = [clusters_distrib[item] \
                            for item in indexes if item in clusters_to_represent]
        clabels = [self.cluster_labels[item] \
                  for item in indexes if item in clusters_to_represent]
        clusters_distrib_tuples = [(x, clusters_distrib[x]) for x in range(len(clusters_distrib))]
        sorted_clusters = sorted(clusters_distrib_tuples, key=operator.itemgetter(1), reverse=True)
        top5 = sorted_clusters[:5]
        top5_clabels = [clabels[x[0]] for x in top5]
      if len(top5) > 0:
        plt.pie([x[1] for x in top5], explode=None, labels=top5_clabels,
              colors=("#8a56e2","#cf56e2","#e256ae","#e25668",
							        "#e28956"),
              autopct=None, pctdistance=0.6, shadow=False,
              labeldistance=1.1)
        plt.legend(top5_clabels)
      else:
        plt.pie(clusters_distrib, explode=None, labels=clabels,
              colors=("#8a56e2","#cf56e2","#e256ae","#e25668",
                      "#e28956","#e2cf56","#aee256","#68e256",
                      "#56e289","#56e2cf","#56aee2","#5668e2",
                      "#E60F39","#F2768E","#2F20AC","#6154D5",
                      "#950521", "#8076D5"
                      ),
              autopct=None, pctdistance=0.6, shadow=False,
              labeldistance=1.1)
        plt.legend(clabels)
      plt.title(i)
      plt.show()
    pass
  
  def initialize_geo_vector(self, locations, geo=False):
    result = {}
    for i in locations:
      if geo:
        result[i] = [0] * self.num_topics
      else:
        result[i] = 0
    return result
  
  def topic_cluster_distribution(self):
    def file_len(fname):
      with open(fname) as f:
          for i, _ in enumerate(f):
              pass
      return i + 1
    Y = []
    for root, _, files in os.walk(self.clusters_location):
      for f in files:
        fname = root + '/' + f
        Y.append(file_len(fname))
    print Y
    print len(Y)
    f = open(self.topic_cluster_sizes, "w")
    pickle.dump(Y, f)
    f.close()
    
  def plot_topic_cluster_sizes(self):
    f = open(self.topic_cluster_sizes, "r")
    Y_all = pickle.load(f)
    f.close()
    #filter not sure and useless clusters
    labels, Y = [], []
    for i in range(self.num_topics):
      if "not sure" in self.cluster_labels[i] or "useless" in self.cluster_labels[i]:
        continue
      else:
        Y.append(Y_all[i])
        labels.append(self.cluster_labels[i])
    fig = plt.figure()
    ax = fig.add_subplot(111)
    X = range(len(Y))
    ax.bar(X, Y, 0.35, color='g')
    ax.set_ylabel("Num tweets per cluster")
    ax.set_title("Distribution of tweets across topics in dataset")
    ax.set_xticks(X)
    ax.set_xticklabels(labels)
    fig.autofmt_xdate()
    plt.show()

GEO_LOCATIONS_FILE = "misc_geolocationnames_datacleaning/top200locations.txt"
def main():
  """
  # files have been removed/moved
  f_clabels = local_clusters_folder + 'cluster_labels.txt'
  f_clusters = local_clusters_folder + 'v-semifinal/'
  f_model = local_tweets_input_folder + 'last-model/'
  """
  f_clabels = local_clusters_folder + "50_clusters_new_data/cluster_labels.txt"
  f_clusters = local_clusters_folder + "50_clusters_new_data/files/"
  f_output = local_clusters_folder + "50_clusters_new_data/temp/"
  f_model = local_tweets_input_folder + "lda_models/model_50_clusters_new_data/"
  cluster_labels = [x.strip() for x in open(f_clabels, "r").readlines()]
  ts = Topics(f_model, f_clusters, 50, cluster_labels, f_output)
  ts.generate_geo_cluster_matrix(GEO_LOCATIONS_FILE)
  """
  clusters_to_represent = [1, 2, 4, 5, 7, 12, 13, 14, 15, 16,
                           17, 19, 20, 21, 23, 24, 28, 29]
  locations_to_represent = ['houston', 'london',
                     'austin', 'sunnyvale', 'mumbai']
  ts.topics_distribution(clusters_to_represent)
  #ts.geo_cluster_pie(f_clusters+'images/', clusters_to_represent, locations_to_represent)
  """
  
"""
Plotting topic cluster distribution
"""
def main2():
  f_clabels = local_clusters_folder + "50_clusters_old_data/cluster_labels.txt"
  f_clusters = local_clusters_folder + "50_clusters_old_data/files/"
  f_output = local_clusters_folder + "50_clusters_old_data/temp/"
  f_model = local_tweets_input_folder + "lda_models/model_50_clusters_old_data/"
  cluster_labels = [x.strip() for x in open(f_clabels, "r").readlines()]
  print len(cluster_labels)
  ts = Topics(f_model, f_clusters, 50, cluster_labels, f_output)
  #ts.topic_cluster_distribution()
  ts.plot_topic_cluster_sizes()

if __name__ == "__main__":
  #main()
  main2()

"""
              colors=("#E60F39", "#AC334B", "#950521", "#F2486A", "#FBD44B",
                      "#F2768E", "#FBDF7A", "#F7C410", "#B99D37", "#A07E05", 
                      "#2F20AC", "#3B3281", "#150B6F", "#6154D5", "#8076D5"),
              """
