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
  def __init__(self, model_folder, clusters_folder, num_topics, cluster_labels):
    self.num_topics = num_topics
    self.cluster_labels = cluster_labels
    self.model_location = model_folder
    self.clusters_location = clusters_folder
    self.geo_topic_matrices = "geo_topic_matrices"
  
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
  
  def topics_distribution(self):
    f = open(self.geo_topic_matrices, "r")
    matrix = pickle.load(f)
    f.close()
    cgm = matrix["cgm"]
    for i in range(self.num_topics):
      geo_distrib = cgm[i]
      #geo_distrib = sorted(geo_distrib, key=operator.itemgetter(1))
      X = geo_distrib.keys()
      Y = geo_distrib.values()
      plt.plot(Y, color="green", linestyle="solid")
      for j in range(len(X)):
        if Y[j] > 2000:
          plt.annotate(X[j], xy=(j, 0),
                       xytext=(j+1, Y[j]+5),
                       arrowprops=dict(width=0.2, facecolor='black'))
      plt.ylabel("size of cluster")
      plt.xlabel("locations")
      plt.title(self.cluster_labels[i])
      plt.show()
  
  def geo_cluster_pie(self, output_folder, clusters_to_represent=None):
    f = open(self.geo_topic_matrices, "r")
    matrix = pickle.load(f)
    f.close()
    gcm = matrix["gcm"]
    pie_locations = ['houston', 'london',
                     'austin', 'sunnyvale', 'mumbai']
    pie_files = {}
    for i in gcm:
      if i not in pie_locations:
        continue
      clusters_distrib = gcm[i]
      if clusters_to_represent:
        indexes = range(len(clusters_distrib))
        clusters_distrib = [clusters_distrib[item] \
                            for item in indexes if item in clusters_to_represent]
        clabels = [self.cluster_labels[item] \
                  for item in indexes if item in clusters_to_represent]
      plt.pie(clusters_distrib, explode=None, labels=clabels,
              colors=("#8a56e2","#cf56e2","#e256ae","#e25668",
                      "#e28956","#e2cf56","#aee256","#68e256",
                      "#56e289","#56e2cf","#56aee2","#5668e2",
                      "#E60F39","#F2768E","#2F20AC","#6154D5",
                      "#950521", "#8076D5"
                      ),
              autopct=None, pctdistance=0.6, shadow=False,
              labeldistance=1.1)
      #plt.legend(clabels)
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

def main():
  f_clabels = local_clusters_folder + 'cluster_labels.txt'
  f_clusters = local_clusters_folder + 'v-semifinal/'
  f_model = local_tweets_input_folder + 'last-model/'
  cluster_labels = [x.strip() for x in open(f_clabels, "r").readlines()]
  ts = Topics(f_model, f_clusters, 30, cluster_labels)
  ts.generate_geo_cluster_matrix("misc_geolocationnames_datacleaning/top200locations.txt")
  #ts.topics_distribution()
  clusters_to_represent = [1, 2, 4, 5, 7, 12, 13, 14, 15, 16,
                           17, 19, 20, 21, 23, 24, 28, 29]
  #ts.geo_cluster_pie(f_clusters+'images/', clusters_to_represent)

if __name__ == "__main__":
  main()

"""
              colors=("#E60F39", "#AC334B", "#950521", "#F2486A", "#FBD44B",
                      "#F2768E", "#FBDF7A", "#F7C410", "#B99D37", "#A07E05", 
                      "#2F20AC", "#3B3281", "#150B6F", "#6154D5", "#8076D5"),
              """