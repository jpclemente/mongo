from datetime import datetime
from lxml import etree
from pymongo import MongoClient


def getDict(oDict, id):
    dictionary = {}
    dictionary['id'] = id
    dictionary['type'] = oDict.tag
    dictionary['date'] = oDict.findtext('mdate')
    dictionary['title'] = oDict.findtext('title')
    dictionary['authors'] = oDict.findtext('author')
    return dictionary

def write(result):
    connection = MongoClient('localhost', 27017)
    db = connection.practica_prueba
    db.publications.insert_many(result)
    print(db.publications.find())
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


