'''
Created on Feb 8, 2013

@author: vandana
'''
import cjson
from category_finder import get_categories

def main(infile, outfile):
  f = open(infile, 'r')
  lines = f.readlines()
  f.close()
  f = open(outfile, 'w')
  for l in lines:
    data = cjson.decode(l)
    cat_input = raw_input(data['tx'] + ': ')
    characteristic_words = cat_input.split(', ')
    categories = get_categories(characteristic_words)
    print categories
    classes = raw_input('Choose from above list: ')
    data['classes'] = classes.split(', ')
    f.write(cjson.encode(data)+'\n')
  f.close()

if __name__ == '__main__':
  main('wherecan.txt', 'wherecan_labelled.txt')