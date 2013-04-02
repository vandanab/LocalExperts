'''
Created on Feb 7, 2013

@author: vandana
'''

import urllib
import httplib2
import cjson

def find(text):
  http = httplib2.Http()
  url = 'https://www.googleapis.com/freebase/v1/search?query={0}&indent=true'.format(urllib.quote_plus(text))
  (response, content) = http.request(url, 'GET')
  categories = []
  if response['status'] == '200':
    content = cjson.decode(content)
    if content['status'] == '200 OK':
      result = content['result']
      for i in result:
        if 'notable' in i:
          categories.append(i['notable']['id'])
  return categories

def get_categories(words):
  categories = []
  for i in words:
    categories.append(find(i))
  return categories

if __name__ == '__main__':
  print find('nokia')
  #find('tickets')
