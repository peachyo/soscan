__author__ = 'chloe'

import urllib2
import pymongo
import json
import re
import pandas as pd
import time
import sys
from time import strftime

def getDbCollection():
# connect to mongo
    connection = pymongo.Connection("mongodb://localhost", safe=True)
    print(connection.database_names())

# get a handle to the reddit database
    db=connection['optionstore']
    collection = db['option_chains']
    print(collection.count())
    print(db.collection_names())
    return collection

'''
prefix = 'http://www.google.com/finance/option_chain?q='
suffix = '&output=json'

urlstr = prefix + 'MSFT'+ suffix
print urlstr
option_data = urllib2.urlopen(urlstr)
s = option_data.read()
json_string = re.sub(r'([a-zA-Z_]+):', r'"\1":', s)
print json_string
#value = json.loads(json_string)
#collection.insert(value)

'''

def getJson(urlstr):
    option_data = urllib2.urlopen(urlstr)
    s = option_data.read()
    json_string = re.sub(r'([a-zA-Z_]+):', r'"\1":', s)
    #print json_string
    value = json.loads(json_string)
    return value

def insertAllJson(symbol, collection):
    prefix = 'http://www.google.com/finance/option_chain?q='
    suffix = '&output=json'
    urlstr=prefix+symbol+suffix
    frontMon = getJson(urlstr)

    for date in frontMon["expirations"]:
        exp_str='&expd='+`date['d']`+'&expm='+`date['m']`+'&expy='+`date['y']`
        #print date['d'], date['m'], date['y']
        urlstr_exp=prefix+symbol+exp_str+suffix

        data = getJson(urlstr_exp)
        data['symbol']=symbol
        data['date']=strftime("%Y-%m-%d")
        collection.insert(data)

def run():
    t0=time.clock()
    my_data = pd.read_csv('symbols.csv', squeeze=True)

    count=0
    collection=getDbCollection()

    for i, row in enumerate(my_data):

        try:
            insertAllJson(row,collection)
            count+=1
            print count, row
        except:
            print "Error:", sys.exc_info()[0]
            print row, "oops no option data"
            my_data=my_data.drop(i)

    my_data.to_csv('option_list.csv', index=False)

    print time.clock()-t0, "seconds process time"
    print 'Total optionable symbols: [{}]'.format(count)

if __name__ == '__main__':
    run()


