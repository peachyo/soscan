__author__ = 'chloe'

import urllib2
import pymongo
import json
import re
import pandas as pd
import time
import sys

# connect to mongo
connection = pymongo.Connection("mongodb://localhost", safe=True)
print(connection.database_names())

# get a handle to the reddit database
db=connection['optionstore']
collection = db['option_chains']
print(collection.count())
print(db.collection_names())

fname = 'rest.csv'
my_data = pd.read_csv(fname, squeeze='true')

prefix = 'http://www.google.com/finance/option_chain?q='
suffix = '&output=json'

'''
urlstr = prefix + 'MSFT'+ suffix
print urlstr
option_data = urllib2.urlopen(urlstr)
s = option_data.read()
json_string = re.sub(r'([a-zA-Z_]+):', r'"\1":', s)
print json_string
#value = json.loads(json_string)
#collection.insert(value)

'''
t0=time.clock()
count=0
for i, row in enumerate(my_data):
    urlstr = prefix + row + suffix
    print urlstr
    try:
        option_data = urllib2.urlopen(urlstr)
        s = option_data.read()
        json_string = re.sub(r'([a-zA-Z_]+):', r'"\1":', s)
        print json_string
        value = json.loads(json_string)
        collection.insert(value)
        count+=1
    except:
        print "Error:", sys.exc_info()[0]
        print row, "oops no option data"
        my_data=my_data.drop(i)

my_data.to_csv('option_list.csv', index=False)

print time.clock()-t0, "seconds process time"
print 'Total optionable symbols: [{}]'.format(count)
