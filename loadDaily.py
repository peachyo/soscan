import csv
import json
from time import strftime
import pymongo

def loadFile(file, collection):
    csvfile = open(file, 'r')
    fieldnames = ("symbol","date","open","high","low","close","volume")
    reader = csv.DictReader( csvfile, fieldnames)
    for row in reader:
        collection.insert(json.loads(json.dumps(row)))

if __name__=="__main__":
    date=str(int(strftime("%Y%m%d"))-1)
    amexfile = 'AMEX_'
    nysefile = 'NYSE_'
    nasdaqfile='NASDAQ_'
    filesuffix=".csv"
    fileprefix="/users/chloe/Downloads/"

    connection = pymongo.Connection("mongodb://localhost", safe=True)
    # get a handle to the database
    db=connection['optionstore']
    db.daily.drop()
    collection = db.daily

    loadFile(fileprefix+amexfile+date+filesuffix, collection)
    loadFile(fileprefix+nysefile+date+filesuffix, collection)
    loadFile(fileprefix+nasdaqfile+date+filesuffix, collection)
