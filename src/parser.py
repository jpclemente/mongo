from datetime import datetime

import xmltodict
from lxml import etree

result = []
count = 0
articles = 0
inproceedings = 0
incollection = 0
doc = etree.iterparse('../data/dblp.xml', html=True, events=["end"], tag=["article", "inproceedings", "incollection"])

for event, elem in doc:
    start = datetime.now()
    if elem.tag in ["article", "inproceedings", "incollection"]:
        count += 1
        if elem.tag == "article":
            articles += 1
        elif elem.tag == "inproceedings":
            inproceedings += 1
        else:
            incollection += 1
        oDict = xmltodict.parse(etree.tostring(elem), encoding="ISO-8859-1")
        print("total: " + str(count))
        print("articles: " + str(articles))
        print("inproceedings: " + str(inproceedings))
        print("incollection: " + str(incollection))
        print("result: " + str(result.__len__()))
        result.append(oDict)
    else:
        continue
    elem.clear()