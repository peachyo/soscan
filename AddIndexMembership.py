from pymongo import MongoClient
import csv

mongo_client = MongoClient()
db=mongo_client.optionstore

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



