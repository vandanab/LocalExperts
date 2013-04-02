'''
Created on Oct 7, 2012

@author: vandana
Provides utility functions for text processing
'''

import nltk

def tokenize(text):
    pattern = r'''(?x)([A-Z]\.)+| \w+(-\w+)*| \$?\d+(\.\d+)?%?| \.\.\.| [][.,;"'?():-_`]'''
    tokens_r = nltk.regexp_tokenize(text, pattern)
    return tokens_r