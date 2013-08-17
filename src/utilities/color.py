'''
Created on Jul 10, 2013
@author: vandana
colors list to be used in plots graphs
'''
import os

COLORS_FILE = os.path.expanduser("~/workspace/LocalExperts/src/utilities/colors.txt")
def get_colors_list(colors_list_file=COLORS_FILE):
  f = open(colors_list_file, "r")
  colors_list = [x.strip() for x in f.readlines()] 
  f.close()
  return colors_list

if __name__ == "__main__":
  print get_colors_list()