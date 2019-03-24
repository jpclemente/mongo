from datetime import datetime

import xmltodict
from lxml import etree
from pymongo import MongoClient


def getDict(oDict, id):
    pass

def write(result):
    connection = MongoClient('localhost', 27017)
    db = connection.prueba
    col = db.publications
    [col.insert(item) for item in []]
    connection.close()


result = []
count = 0
articles = 0
inproceedings = 0
incollection = 0
limit = 100000
doc = etree.iterparse('../data/dblp.xml', html=True, events=["end"])

dict = {}
id = 0
for event, elem in doc:
    id += 1
    start = datetime.now()
    if elem.tag in ["article", "inproceedings", "incollection"]:
        count += 1
        if elem.tag == "article":
            articles += 1
        elif elem.tag == "inproceedings":
            inproceedings += 1
        else:
            incollection += 1
        print("total: " + str(count))
        print("articles: " + str(articles))
        print("inproceedings: " + str(inproceedings))
        print("incollection: " + str(incollection))
        print("result: " + str(result.__len__()))
        result.append(getDict(elem, id))
        if result.__len__() == limit:
            write(result)
            result = []
    else:
        continue
    elem.clear()


