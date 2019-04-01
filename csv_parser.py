from lxml import etree
import pandas as pd


publication_nodes = {'publication_id': [], 'type':[], 'date':[], 'title':[]}
relationships = {'name':[], 'publication_id': []}
count = 0
total = 0
publication_id = 0
author_id = 0
limit = 500000
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
    publication_id +=1
    publication_nodes['publication_id'] = publication_id
    publication_nodes['type'].append(elem.tag)
    publication_nodes['date'].append(elem.get('mdate'))
    for child in elem.getchildren():
        if child.tag == 'author':
            relationships['name'].append(child.text)
            relationships['publication_id'].append(publication_id)
        elif child.tag == 'title':
            publication_nodes['title'].append(child.text)

    # if count >= limit:
    #     print("total: " + str(total))
    #     print("publications: " + str(publication_nodes['title'].__len__()))
    #     print("relationships: " + str(relationships['name'].__len__()))
    #     count = 0
    elem.clear()
print("writting: " + str(publication_nodes['title'].__len__()) + " publications")
pd.DataFrame(publication_nodes).to_csv("data/publication_nodes.csv")
publication_nodes = {}

df_relationships = pd.DataFrame(relationships)
relationships = {}
df_authors = pd.DataFrame(
    {
        'name': list(set(df_relationships['name'])),
        'author_id': [x for x in range(1,list(set(df_relationships['name'])).__len__() + 1)]
    }
)
df_relationships = pd.merge(df_relationships, df_authors, on=['name', 'name'])[["author_id", "publication_id"]]

print("writting: " + str(df_authors['name'].__len__()) + " authors")
df_authors.to_csv("data/author_nodes.csv")
df_authors = []
print("writting: " + str(df_relationships['publication_id'].__len__()) + " relationships")
df_relationships.to_csv("data/relationships.csv")
df_relationships = []

'''
neo4j_home$ bin/neo4j-admin import --nodes "import/movies_header.csv,import/movies.csv" \
--nodes "import/actors_header.csv import/actors.csv" \
--relationships "import/roles_header.csv,import/roles.csv" 
'''