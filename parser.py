from lxml import etree
from pymongo import MongoClient


def get_dict(element, identifier):
    publication = {}
    publication['_id'] = identifier
    publication['type'] = element.tag
    publication['date'] = element.get('mdate')
    authors = []
    for child in element.getchildren():
        if child.tag == 'author':
            authors.append(child.text)
        elif child.tag == 'title':
            publication['title'] = child.text
    publication['author'] = authors
    return publication


def write(publications):
    connection = MongoClient('localhost', 27017)
    db = connection.practica_prueba
    db.publications.insert_many(publications)
    print(db.publications.find())
    connection.close()


result = []
count = 0
articles = 0
inproceedings = 0
incollection = 0
limit = 100000
publication_id = 0
doc = etree.iterparse(
    'data/dblp.xml',
    #tag = ["article", "inproceedings", "incollection"],
    events = ["end"],
    load_dtd=True,
    dtd_validation=True,
    encoding="ISO-8859-1")

for event, elem in doc:
    count += 1
    if elem.tag in ["article", "inproceedings", "incollection"]:
        publication_id += 1
        if elem.tag == "article":
            articles += 1
        elif elem.tag == "inproceedings":
            inproceedings += 1
        else:
            incollection += 1
        result.append(get_dict(elem, publication_id))
        print(get_dict(elem, publication_id))
        if result.__len__() == limit:
            print("articles: " + str(articles))
            print("inproceedings: " + str(inproceedings))
            print("incollection: " + str(incollection))
            print("total: " + str(count))
            print("result: " + str(result.__len__()))
            write(result)
            result = []
    else:
        continue
    elem.clear()
