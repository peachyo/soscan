__author__ = 'chloe'

import urllib2
from pymongo import MongoClient
import csv

mongo_client = MongoClient()
db=mongo_client.optionstore

db.symbols.drop()

with open('symbols/amex.csv') as f:
    records = csv.DictReader(f)
    for record in records:
        record['Exchange']='AMEX'
        record['Country']='United States'
        db.symbols.insert(record)

with open('symbols/nasdaq.csv') as f:
    records = csv.DictReader(f)
    for record in records:
        record['Exchange']='NASDAQ'
        record['Country']='United States'
        db.symbols.insert(record)

with open('symbols/nyse.csv') as f:
    records = csv.DictReader(f)
    for record in records:
        record['Exchange']='NYSE'
        record['Country']='United States'
        db.symbols.insert(record)

#db.symbols.update({}, {"$set":{"Country":"US"}}, multi=True)

with open('symbols/africa.csv') as f:
    records = csv.DictReader(f)
    for record in records:
        db.symbols.update({"Symbol":record['Symbol']}, \
                          {"$set": {"Country":record['Country']}}, upsert=False)

with open('symbols/africa.csv') as f:
    records = csv.DictReader(f)
    for record in records:
        db.symbols.update({"Symbol":record['Symbol']}, \
                          {"$set": {"Country":record['Country']}}, upsert=False)

with open('symbols/asia.csv') as f:
    records = csv.DictReader(f)
    for record in records:
        db.symbols.update({"Symbol":record['Symbol']}, \
                          {"$set": {"Country":record['Country']}}, upsert=False)

with open('symbols/australiasouthpacific.csv') as f:
    records = csv.DictReader(f)
    for record in records:
        db.symbols.update({"Symbol":record['Symbol']}, \
                          {"$set": {"Country":record['Country']}}, upsert=False)

with open('symbols/centralamericacaribbean.csv') as f:
    records = csv.DictReader(f)
    for record in records:
        db.symbols.update({"Symbol":record['Symbol']}, \
                          {"$set": {"Country":record['Country']}}, upsert=False)

with open('symbols/southamerica.csv') as f:
    records = csv.DictReader(f)
    for record in records:
        db.symbols.update({"Symbol":record['Symbol']}, \
                          {"$set": {"Country":record['Country']}}, upsert=False)

with open('symbols/europe.csv') as f:
    records = csv.DictReader(f)
    for record in records:
        db.symbols.update({"Symbol":record['Symbol']}, \
                          {"$set": {"Country":record['Country']}}, upsert=False)

with open('symbols/canada.csv') as f:
    records = csv.DictReader(f)
    for record in records:
        db.symbols.update({"Symbol":record['Symbol']}, \
                          {"$set": {"Country":"Canada"}}, upsert=False)

with open('symbols/mexico.csv') as f:
    records = csv.DictReader(f)
    for record in records:
        db.symbols.update({"Symbol":record['Symbol']}, \
                          {"$set": {"Country":"Mexico"}}, upsert=False)

db.symbols.update({},{"$unset":{"LastSale":""}}, multi=True)
db.symbols.update({},{"$unset":{"MarketCap":""}}, multi=True)

with open('symbols/s&p500.csv', 'r') as f:
    reader = csv.reader(f)
    #next(reader) # Ignore first row
    for row in reader:
        symbol = row[0]
        print symbol
        db.symbols.find_and_modify(query={'Symbol':symbol}, update={'$addToSet':{"Index Membership":'S&P 500'}})

with open('symbols/nq100.csv', 'r') as f:
    reader = csv.reader(f)
    #next(reader) # Ignore first row
    for row in reader:
        symbol = row[0]
        print symbol
        db.symbols.find_and_modify(query={'Symbol':symbol}, update={'$addToSet':{"Index Membership":'NASDAQ 100'}})

with open('symbols/russell2000.csv', 'r') as f:
    reader = csv.reader(f)
    #next(reader) # Ignore first row
    for row in reader:
        symbol = row[0]
        print symbol
        db.symbols.find_and_modify(query={'Symbol':symbol}, update={'$addToSet':{"Index Membership":'Russell 2000'}})

with open('symbols/dow30.csv', 'r') as f:
    reader = csv.reader(f)
    #next(reader) # Ignore first row
    for row in reader:
        symbol = row[0]
        print symbol
        db.symbols.find_and_modify(query={'Symbol':symbol}, update={'$addToSet':{"Index Membership":'Dow Jones Industrial'}})


