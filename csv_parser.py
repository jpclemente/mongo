from lxml import etree
import pandas as pd


author_nodes = {'name': []}
publication_nodes = {'type':[], 'date':[], 'title':[]}
aristas = pd.DataFrame()
count = 0
total = 0
limit = 100000
doc = etree.iterparse(
    'data/dblp.xml',
    tag = ["article", "inproceedings", "incollection"],
    events = ["end"],
    load_dtd=True,
    dtd_validation=True,
    encoding="ISO-8859-1")

for event, elem in doc:
    count += 1
    total += 1
    publication_nodes['type'].append(elem.tag)
    publication_nodes['date'].append(elem.get('mdate'))
    for child in elem.getchildren():
        if child.tag == 'author':
            author_nodes['name'].append(child.text)
        elif child.tag == 'title':
            publication_nodes['title'].append(child.text)

    if count >= limit:
        print("total: " + str(total))
        author_nodes['name'] = list(set(author_nodes['name']))
        print("authors: " + str(author_nodes['name'].__len__()))
        print("publications: " + str(publication_nodes['title'].__len__()))
        count = 0
    elem.clear()
print("writting: " + str(author_nodes['name'].__len__()) + " authors")
pd.DataFrame(author_nodes).to_csv("data/author_nodes.csv")
author_nodes = {}
print("writting: " + str(publication_nodes['title'].__len__()) + " publications")
pd.DataFrame(publication_nodes).to_csv("data/publication_nodes.csv")
publication_nodes = {}

'''
neo4j_home$ bin/neo4j-admin import --nodes "import/movies_header.csv,import/movies.csv" \
--nodes "import/actors_header.csv import/actors.csv" \
--relationships "import/roles_header.csv,import/roles.csv" 
'''