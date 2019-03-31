import time
import pandas as pd
from pymongo import MongoClient
client = MongoClient()
#client = MongoClient('localhost', 27017)
db = client.practica_mongo_def
publications = db.publications

# Pregunta 1.- Listado de todas las publicaciones de un autor determinado.
start = time.time()
answer_1 = db.publications.find({"authors": {"$eq":"Joachim Biskup"}}, {"title":1})
end = time.time()
print('The time taken, in seconds, for query number 1 is ', end - start)
print ('El listado de las publicaciones de Joachim Biskup es:')
list_answer_1 = []
for line in answer_1:
    list_answer_1.append(line)
answer_1_pd = pd.DataFrame.from_records(list_answer_1)
print(answer_1_pd)

# Pregunta 2.- Numero de publicaciones de un autor determinado.
start = time.time()
answer_2 = db.publications.find({"authors": {"$eq":"Joachim Biskup"}}, {"title":1}).count()
end = time.time()
print('The time taken, in seconds, for query number 2 is ', end - start)
print ('El numero de publicaciones de Joachim Biskup es ', answer_2)


# Pregunta 3.- Numero de articulos en revista para el anyo 2017.
start = time.time()
answer_3 = db.publications.find({"$and": [{"date" : {"$regex": "2017"}}, {"type" :"article"}]}).count()
end = time.time()
print('The time taken, in seconds, for query number 3 is ', end - start)
print ('El numero de articulos en revista para el anyo 2017 es ',answer_3)

# Pregunta 4.- Numero de autores ocasionales, es decir, que tengan menos de 5 publicaciones en total.
pipeline_answer_4 = [{"$unwind": "$authors"},
                     {"$sortByCount":"$authors"},
                     { "$match": {"count": { "$lt":5}}},
                     {"$count": "authors"}]
start = time.time()
answer_4 = db.publications.aggregate(pipeline_answer_4, allowDiskUse=True)
end = time.time()
print('The time taken, in seconds, for query number 4 is ', end - start)
print ('El numero de autores con menos de 5 publicaciones es ')
list_answer_4 = []
for line in answer_4:
    list_answer_4.append(line)
answer_4_pd = pd.DataFrame.from_records(list_answer_4)
print(answer_4_pd)

# Pregunta 5.- Numero de articulos de revista (article) y numero de articulos en congresos
# (inproceedings) de los diez autores con mas publicaciones totales.
pipeline_answer_5 =[{"$unwind": "$authors"},
                    {"$group":{"_id": "$authors",
                             "count_all_publications": {"$sum":1},
                             "count_article": {"$sum" : {"$cond" : { "if": { "$eq": ["$type", "article"]}, "then": 1, "else": 0}}},
                             "count_inproceedings": {"$sum" : {"$cond" : { "if": { "$eq": ["$type", "inproceedings"]}, "then": 1, "else": 0}}}}},
                    {"$sort": {"count_all_publications": -1}},
                    {"$limit": 10}]
start = time.time()
answer_5 = db.publications.aggregate(pipeline_answer_5, allowDiskUse=True)
end = time.time()
print('The time taken, in seconds, for query number 5 is ', end - start)
print ('El numero de articulos de revista y numero de articulos en congresos de los diez autores con mas publicaciones totales viene dado a continuacion:')
list_answer_5 = []
for line in answer_5:
    list_answer_5.append(line)
answer_5_pd = pd.DataFrame.from_records(list_answer_5)
print(answer_5_pd)

# Pregunta 6.- Numero medio de autores de todas las publicaciones que tenga en su conjunto de datos.
pipeline_answer_6 =[{"$project": { "numAuthors": { "$size": "$authors" }}},
                    {"$group":{"_id": "null","MeanOfAuthors": {"$avg": "$numAuthors"}}},
                    {"$project" : {"MeanOfAuthors":1, "_id":0}}]
start = time.time()
answer_6 = db.publications.aggregate(pipeline_answer_6)
end = time.time()
print('The time taken, in seconds, for query number 6 is ', end - start)
print ('El numero medio de autores de todas las mublicaciones del conjunto de datos es:')
list_answer_6 = []
for line in answer_6:
    list_answer_6.append(line)
answer_6_pd = pd.DataFrame.from_records(list_answer_6)
print(answer_6_pd)

# Pregunta 7.- Listado de coautores de un autor (Se denomina coautor a cualquier persona que haya
# firmado una publicacion).
pipeline_answer_7 = [{"$project": {'authors': 1}},
                     {"$match": {"authors": 'Joachim Biskup'}},
                     {"$unwind" : '$authors' },
                     {"$group": {"_id":"$authors",
                                 "coauthors": { "$addToSet": {"$cond" : { "if": { "$eq": ["$authors", "Joachim Biskup"]}, "then": "null", "else": "$authors"}}}}},
                     {"$project" : {"_id":0,
                                    "coauthors": { "$cond": {"if": {"$eq": [ "[null]", "$coauthors" ] },"then": "$$REMOVE","else": "$coauthors"}}}}]
start = time.time()
answer_7 = db.publications.aggregate(pipeline_answer_7)
end = time.time()
print('The time taken, in seconds, for query number 7 is ', end - start)
print ('El listado de los coautores de Joachim Biskup es:')
list_answer_7 = []
for line in answer_7:
    list_answer_7.append(line)
answer_7_pd = pd.DataFrame.from_records(list_answer_7)
print(answer_7_pd)

# Pregunta 8.- Edad de los 5 autores con un periodo de publicaciones mas largo (Se considera la Edad
# de un autor al numero de anyos transcurridos desde la fecha de su primera publicacion
# hasta la ultima registrada).
pipeline_answer_8 = [{"$unwind" : '$authors' },
                     {"$group" : {"_id":"$authors",
                                "max_year" : {"$max" : {"$substr": [ "$date", 0, 4 ]}},
                                "min_year" :{"$min" : {"$substr": [ "$date", 0, 4 ]}}}},
                     {"$addFields": {"max_year_int": {"$toInt": "$max_year"},
                                     "min_year_int": {"$toInt": "$min_year"}}},
                     {"$project" : {"max_year":0, "min_year":0}},
                     {"$addFields": {"ageAuthor": {"$subtract": ["$max_year_int","$min_year_int"]}}},
                     {"$sort": {"ageAuthor": -1}},{"$limit": 5}]
start = time.time()
answer_8 = db.publications.aggregate(pipeline_answer_8, allowDiskUse=True)
end = time.time()
print('The time taken, in seconds, for query number 8 is ', end - start)
print ('La edad de los 5 autores con un periodo de publicacion mas largo es:')
list_answer_8 = []
for line in answer_8:
    list_answer_8.append(line)
answer_8_pd = pd.DataFrame.from_records(list_answer_8)
print(answer_8_pd)

# Pregunta 9.- Numero de autores novatos, es decir, que tengan una Edad menor de 5 anyos. Se
# considera la Edad de un autor al numero de anyos transcurridos desde la fecha de su
# primera publicacion hasta la ultima registrada
pipeline_answer_9 = [{"$unwind" : '$authors' },
                     {"$group" :
                          {"_id":"$authors",
                           "max_year" : {"$max" : {"$substr": [ "$date", 0, 4 ]}},
                           "min_year" :{"$min" : {"$substr": [ "$date", 0, 4 ]}}}},
                     {"$addFields": { "max_year_int": {"$toInt": "$max_year"},
                                      "min_year_int": {"$toInt": "$min_year"}}},
                     {"$project" : {"max_year":0, "min_year":0}},
                     {"$addFields": {"ageAuthor": {"$subtract": ["$max_year_int","$min_year_int"]}}},
                     {"$match": { "ageAuthor": { "$lt": 5} }},{"$count": "ageAuthor"}]
start = time.time()
answer_9 = db.publications.aggregate(pipeline_answer_9, allowDiskUse=True)
end = time.time()
print('The time taken, in seconds, for query number 9 is ', end - start)
print ('El numero de autores novatos es el siguiente:')
list_answer_9 = []
for line in answer_9:
    list_answer_9.append(line)
answer_9_pd = pd.DataFrame.from_records(list_answer_9)
print(answer_9_pd)

# Pregunta 10.- Porcentaje de publicaciones en revistas con respecto al total de publicaciones.
pipeline_answer_10 = [{"$project": {"type": 1}},
                      {"$group":{"_id":"null",
                                 "count_article": {"$sum": { "$cond" :  [{ "$eq" : ["$type", "article"]}, 1, 0]}},
                                 "count_total" : {"$sum":1}}},{"$project": { "article_percentage" :{"$multiply": [100,{ "$divide": [ "$count_article", "$count_total"]}]}}},
                      {"$project": {"_id":0}}]
start = time.time()
answer_10 = db.publications.aggregate(pipeline_answer_10)
end = time.time()
print('The time taken, in seconds, for query number 10 is ', end - start)
print('El porcentaje de publicaciones en revistas (articles) con respecto al total de publicaciones es:')
list_answer_10 = []
for line in answer_10:
    list_answer_10.append(line)
answer_10_pd = pd.DataFrame.from_records(list_answer_10)
print(answer_10_pd)


