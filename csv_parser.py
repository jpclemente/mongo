from lxml import etree
import pandas as pd

data_path = "data/"
publication_nodes = {'publication_id:ID': [], 'type:LABEL':[], 'date':[], 'title':[]}
relationships = {'name':[], 'publication_id:END_ID': []}
count = 0
total = 0
publication_id = 0
author_id = 0
limit = 500000
doc = etree.iterparse(
    data_path + "dblp.xml",
    tag = ["article", "inproceedings", "incollection"],
    events = ["end"],
    load_dtd=True,
    dtd_validation=True,
    encoding="ISO-8859-1")
for event, elem in doc:
    count += 1
    total += 1
    publication_id +=1
    publication_nodes['publication_id:ID'].append(publication_id)
    publication_nodes['type:LABEL'].append(elem.tag)
    publication_nodes['date'].append(elem.get('mdate'))
    for child in elem.getchildren():
        if child.tag == 'author':
            relationships['name'].append(child.text)
            relationships['publication_id:END_ID'].append(publication_id)
        elif child.tag == 'title':
            publication_nodes['title'].append(child.text)
    elem.clear()


print("writting: " + str(publication_nodes['title'].__len__()) + " publications")
pd.DataFrame(publication_nodes).to_csv(data_path + "publication_nodes.csv", index = False)
publication_nodes = {}

df_relationships = pd.DataFrame(relationships)
relationships = {}
df_authors = pd.DataFrame(
    {
        'author_id:ID': [x for x in range(publication_id+1,publication_id+list(set(df_relationships['name'])).__len__() + 1)]
        ,'name': list(set(df_relationships['name']))
    }
)
df_relationships = pd.merge(df_relationships, df_authors, on=['name', 'name'])[["author_id:ID", "publication_id:END_ID"]]
df_relationships.columns = ["author_id:START_ID", "publication_id:END_ID"]

print("writting: " + str(df_authors['name'].__len__()) + " authors")
df_authors.to_csv(data_path + "author_nodes.csv", index = False)
df_authors = []
print("writting: " + str(df_relationships['publication_id:END_ID'].__len__()) + " relationships")
df_relationships.to_csv(data_path + "relationships.csv", index = False)