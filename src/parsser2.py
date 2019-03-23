from datetime import datetime, timedelta

from lxml import etree

source = '../data/dblp.xml'

# get an iterable
context = etree.iterparse(source,
                    html=True,
                    events=("end",),
                    # tag=["article", "inproceedings", "incollection"],
                    encoding="ISO-8859-1")

articles = 0
inproceedings = 0
incollection = 0
count = 0
for event, elem in context:
    start = datetime.now()
    if event == "end" and elem.tag in ["article", "inproceedings", "incollection"]:
        count +=1
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
    else:
        continue
    elem.clear()
    time_diff = (datetime.now() - start).total_seconds() * 1000
    print(time_diff)