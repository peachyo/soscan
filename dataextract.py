__author__ = 'chloe'

import urllib2
import pymongo
import json
import re
import pandas as pd
import sys
import time
from datetime import timedelta, datetime
from volcal import getIV
from implvol import findImplVol

def getDate(delta=0):
    date=datetime.today()
    if date!= 0:
        date=date+timedelta(delta)
    return date

def getDbCollection():
# connect to mongo
    connection = pymongo.Connection("mongodb://localhost", safe=True)
    print(connection.database_names())

# get a handle to the database
    db=connection['optionstore']
    collection = db['optionchains']
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
    req = urllib2.Request(urlstr, headers={ 'User-Agent': 'Mozilla/5.0' })
    s = urllib2.urlopen(req).read()
    json_string = re.sub(r'([a-zA-Z_]+):', r'"\1":', s)
    #print json_string
    value = json.loads(json_string)
    return value

def insertAllJson(symbol, collection, date):
    prefix = 'http://www.google.com/finance/option_chain?q='
    suffix = '&output=json'
    urlstr=prefix+symbol+suffix
    frontMon = getJson(urlstr)
    #print frontMon

    datestr=date.strftime("%b %d, %Y")
    dayofweek=date.strftime("%A")

    fy = frontMon['expiry']['y']
    fm = frontMon['expiry']['m']
    fd = frontMon['expiry']['d']

    #print fy, fm, fd
    #print frontMon['expirations']
    #print frontMon['expiry']

    #remove the 'expiry' field
    del frontMon['expiry']

    #add 'symbol' field
    frontMon['symbol']=symbol

    #add 'date' field
    frontMon['date']=date
    frontMon['dayofweek']=dayofweek

    for dt in frontMon["expirations"]:
        #skip front month
        if dt['d']==fd and dt['m']==fm and dt['y']==fy:
            continue
        #print dt['d'], dt['m'], dt['y']

        exp_str='&expd='+`dt['d']`+'&expm='+`dt['m']`+'&expy='+`dt['y']`
        #print date['d'], date['m'], date['y']
        urlstr_exp=prefix+symbol+exp_str+suffix

        data = getJson(urlstr_exp)

        try:
        #append call and put data to the front month json
            frontMon['puts']=frontMon['puts']+data['puts']
            frontMon['calls']=frontMon['calls']+data['calls']

        #some leaps don't have calls or puts data
        except KeyError:
            pass
        #do not send request too frequent to avoid being blocked by google
        #time.sleep(1)

    #delete the expirations
    del frontMon["expirations"]

    #remove options with open interest=0 or volumn=0 or bid=0 or price=0 for puts
    frontMon['puts']=[item for item in frontMon['puts'] \
        if item['oi']!='0' and item['vol']!='-' and \
            item['p']!='-' and item['b']!='-']

    #remove name, cid, e, cs, cp fields for puts
    for element in frontMon['puts']:
        if 'name' in element:
            del element['name']
        if 'cid' in element:
            del element['cid']
        if 'e' in element:
            del element['e']
        if 'cs' in element:
            del element['cs']
        if 'cp' in element:
            del element['cp']

    #remove options with open interest=0 or volumn=0 or bid=0 or price=0 for calls
    frontMon['calls']=[item for item in frontMon['calls'] \
        if item['oi']!='0' and item['vol']!='-' and \
            item['p']!='-' and item['b']!='-']

    S=float(frontMon['underlying_price'])

    #remove name, cid, e, cs, cp fields for calls
    for element in frontMon['calls']:
        if 'name' in element:
            del element['name']
        if 'cid' in element:
            del element['cid']
        if 'e' in element:
            del element['e']
        if 'cs' in element:
            del element['cs']
        if 'cp' in element:
            del element['cp']

        #calculate implied volatility for calls
        d=datetime.strptime(element['expiry'],"%b %d, %Y")
        daysdelta=datetime.strptime(element['expiry'],"%b %d, %Y")-date
        tau=daysdelta.days
        element['dtoexp']=tau
        K=float(element['strike'])
        C1=float(element['a'])
        C2=float(element['b'])
        #iv1=getIV(C1,S,K,tau,0.001)
        #iv2=getIV(C2,S,K,tau,0.001)
        print C1,S,K,tau
        iv1=findImplVol(C1,'c',S,K,tau/365.0,0.001)
        iv2=findImplVol(C2,'c',S,K,tau/365.0,0.001)
        element['iv']=(iv1+iv2)/2.0
        #print iv1, iv2
        print element

    #now insert one day's data for each symbol into database
    collection.insert(frontMon)

def run(datedelta=0):
    my_data = pd.read_csv('symbols.csv', squeeze=True)

    count=0
    collection=getDbCollection()

    date = getDate(datedelta)

    for i, row in enumerate(my_data):

        try:
            insertAllJson(row,collection, date)
            count+=1
            print count, row
        except:
            print "Error:", sys.exc_info()[0]
            print row, "oops no option data"

    print 'Total optionable symbols: [{}]'.format(count)

if __name__ == '__main__':
    datedelta=0
    if len(sys.argv) == 2:
        #sys.stderr.write('Usage: python dataextract.py [datedelta]')
        #sys.exit(1)
        datedelta = int(sys.argv[1])
    run(datedelta)


