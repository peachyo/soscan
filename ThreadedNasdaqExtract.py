#!/usr/bin/env python
import Queue
import threading
import time
import pandas as pd
import urllib2
import pymongo
import json
import sys
from datetime import timedelta, datetime
import jsontree
from bs4 import BeautifulSoup
import timeit


url_prefix="http://www.nasdaq.com/symbol/"
url_suffix="/option-chain?dateindex=-1"
url_page="&page="

queue = Queue.Queue()
connection = pymongo.Connection("mongodb://localhost", safe=True)
# get a handle to the database
db=connection['optionstore']
#db.optionchains.drop()
date=datetime.today()

class ThreadUrl(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            if queue.empty() == True:
                break

            symbol = self.queue.get()
            #print symbol

            #grabs urls of hosts and prints first 1024 bytes of page
            getOptionData(symbol)
            print symbol,"done"
            #signals to queue job is done
            self.queue.task_done()

def getOptionData(symbol):

    data = jsontree.jsontree()
    data.symbol = symbol

    data.puts = []
    data.calls= []
    data.date = time.strftime("%x")
    data.time = time.strftime("%X")
    #option_chain_url=url_prefix + symbol +url_suffix #+url_page+str(4)
    url = url_prefix + symbol +url_suffix
    #print url

    option_req = urllib2.Request(url, headers={ 'User-Agent': 'Mozilla/5.0' })
    option_html = urllib2.urlopen(option_req).read()
    soup = BeautifulSoup(option_html)

    price=soup.find("div", id="qwidget_lastsale")
    data.price=price.text[1:]
    #print data.price

    table=soup.find_all("table")[2]
    #print table
    rows = table.findAll('tr')
    if len(rows)<2:
        return
    for index, row in enumerate(table.findAll('tr', {'class': not 'groupheader'})[2:]):

        col = row.findAll('td')
        call = jsontree.jsontree()
        call.expiration=col[0].text
        call.last = col[1].text
        call.vol = col[5].text
        if call.vol =='0': continue
        call.oi=col[6].text
        if call.oi=='': continue
        call.strike=col[8].text
        data.calls.append(call)

        put = jsontree.jsontree()
        put.expiration=col[9].text
        put.last=col[10].text
        put.vol=col[14].text
        if put.vol =='0': continue
        put.oi=col[15].text
        if put.oi =='': continue
        put.strike=col[8].text
        data.puts.append(put)

    a= soup.find('a',{'class': 'pagerlink', 'id':'quotes_content_left_lb_LastPage'})

    if a:
        numpages=int(a['href'][-1])

        for i in range(numpages):
            if i==0:
                continue
                #start from page 2
            i+=1
            #print i

            option_chain_url=url_prefix + symbol +url_suffix +url_page+str(i)

            option_req = urllib2.Request(option_chain_url, headers={ 'User-Agent': 'Mozilla/5.0' })
            option_html = urllib2.urlopen(option_req).read()
            soup = BeautifulSoup(option_html)

            table=soup.find_all("table")[2]
            #print table
            rows = table.findAll('tr')
            if len(rows)<2:
                continue

            for index, row in enumerate(table.findAll('tr', {'class': not 'groupheader'})[2:]):
                #print row

                col = row.findAll('td')
                #print col
                call = jsontree.jsontree()
                call.expiration=col[0].text
                call.last = col[1].text
                call.bid = col[3].text
                call.ask = col[4].text
                call.vol = col[5].text
                if call.vol =='0': continue
                call.oi=col[6].text
                if call.oi=='': continue
                call.strike=col[8].text
                data.calls.append(call)

                put = jsontree.jsontree()
                put.expiration=col[9].text
                put.last=col[10].text
                put.bid=col[12].text
                put.ask=col[13].text
                put.vol=col[14].text
                if put.vol =='0': continue
                put.oi=col[15].text
                if put.oi =='': continue
                put.strike=col[8].text
                data.puts.append(put)

    #ser = jsontree.dumps(data)
    db.optionchains.insert(data)

def main():

    my_data = pd.read_csv('symbols.csv', squeeze=True)

    #spawn a pool of threads, and pass them queue instance
    for i in range(40):
        t = ThreadUrl(queue)
        t.setDaemon(True)
        t.start()

    #count=0
    #populate queue with data
    for i, row in enumerate(my_data):
        try:
            queue.put(row)
            #count+=1
            #print count, row
        except:
            print "Error:", sys.exc_info()[0]
            print row, "oops no option data"

    #wait on the queue until everything has been processed
    queue.join()

if __name__=="__main__":
    start = timeit.default_timer()

    main()

    print "Elapsed Time: %s" % (timeit.default_timer() - start)
