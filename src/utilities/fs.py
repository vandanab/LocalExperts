'''
Created on Oct 7, 2012

@author: vandana
Contains filesystem related utilities
'''

from dateutil.relativedelta import relativedelta
import os

def get_dated_input_files(startTime, endTime, input_folder):
    current = startTime
    while current <= endTime:
        input_file = input_folder + '%s_%s' % (current.year, current.month)
        print input_file
        yield input_file
        current += relativedelta(months=1)

def get_local_input_files(input_folder):
    for (root, _, files) in os.walk(input_folder):
        for f in files:
            yield root + f