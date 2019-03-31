from pymongo import MongoClient
client = MongoClient()
#client = MongoClient('localhost', 27017)
db = client.practica_mongo_def
publications = db.publications

# Pregunta 1.- Listado de todas las publicaciones de un autor determinado.
answer_1 = db.publications.find({"authors": {"$eq":"Joachim Biskup"}}, {"title":1})
print 'El listado de las publicaciones de Joachim Biskup es:'
for line in answer_1:
    print(line)

# Pregunta 2.- Numero de publicaciones de un autor determinado.
answer_2 = db.publications.find({"authors": {"$eq":"Joachim Biskup"}}, {"title":1}).count()
print'El numero de publicaciones de Joachim Biskup es ', answer_2

# Pregunta 3.- Numero de articulos en revista para el anyo 2017.
answer_3 = db.publications.find({"$and": [{"date" : {"$regex": "2017"}}, {"type" :"article"}]}).count()
print 'El numero de articulos en revista para el anyo 2017 es ',answer_3

# Pregunta 4.- Numero de autores ocasionales, es decir, que tengan menos de 5 publicaciones en total.
pipeline_answer_4 = [{"$unwind": "$authors"},
                     {"$sortByCount":"$authors"},
                     { "$match": {"count": { "$lt":5}}},
                     {"$count": "authors"}]
answer_4 = db.publications.aggregate(pipeline_answer_4, allowDiskUse=True)
print 'El numero de autores con menos de 5 publicaciones es '
for line in answer_4:
    print(line)
# Pregunta 5.- Numero de articulos de revista (article) y numero de articulos en congresos
# (inproceedings) de los diez autores con mas publicaciones totales.
pipeline_answer_5 =[{"$unwind": "$authors"},
                    {"$group":{"_id": "$authors",
                             "count_all_publications": {"$sum":1},
                             "count_article": {"$sum" : {"$cond" : { "if": { "$eq": ["$type", "article"]}, "then": 1, "else": 0}}},
                             "count_inproceedings": {"$sum" : {"$cond" : { "if": { "$eq": ["$type", "inproceedings"]}, "then": 1, "else": 0}}}}},
                    {"$sort": {"count_all_publications": -1}},
                    {"$limit": 10}]
answer_5 = db.publications.aggregate(pipeline_answer_5, allowDiskUse=True)
print 'El numero de articulos de revista y numero de articulos en congresos de los diez autores con mas publicaciones totales viene dado a continuacion:'
for line in answer_5:
    print(line)

# Pregunta 6.- Numero medio de autores de todas las publicaciones que tenga en su conjunto de datos.
pipeline_answer_6 =[{"$project": { "numAuthors": { "$size": "$authors" }}},
                    {"$group":{"_id": "null","MeanOfAuthors": {"$avg": "$numAuthors"}}},
                    {"$project" : {"MeanOfAuthors":1, "_id":0}}]
answer_6 = db.publications.aggregate(pipeline_answer_6)
print 'El numero medio de autores de todas las mublicaciones del conjunto de datos es:'
for line in answer_6:
    print(line)

# Pregunta 7.- Listado de coautores de un autor (Se denomina coautor a cualquier persona que haya
# firmado una publicacion).
pipeline_answer_7 = [{"$project": {'authors': 1}},
                     {"$match": {"authors": 'Joachim Biskup'}},
                     {"$unwind" : '$authors' },
                     {"$group": {"_id":"$authors",
                                 "coauthors": { "$addToSet": {"$cond" : { "if": { "$eq": ["$authors", "Joachim Biskup"]}, "then": "null", "else": "$authors"}}}}},
                     {"$project" : {"_id":0,
                                    "coauthors": { "$cond": {"if": {"$eq": [ "[null]", "$coauthors" ] },"then": "$$REMOVE","else": "$coauthors"}}}}]
answer_7 = db.publications.aggregate(pipeline_answer_7)
print 'El listado de los coautores de Joachim Biskup es:'
for line in answer_7:
    print(line)

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
answer_8 = db.publications.aggregate(pipeline_answer_8, allowDiskUse=True)
print 'la edad de los 5 autores con un periodo de publicacion mas largo es:'
for line in answer_8:
    print(line)

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
answer_9 = db.publications.aggregate(pipeline_answer_9, allowDiskUse=True)
print 'El numero de autores novatos es el siguiente:'
for line in answer_9:
    print(line)

# Pregunta 10.- Porcentaje de publicaciones en revistas con respecto al total de publicaciones.
pipeline_answer_10 = [{"$project": {"type": 1}},
                      {"$group":{"_id":"null",
                                 "count_article": {"$sum": { "$cond" :  [{ "$eq" : ["$type", "article"]}, 1, 0]}},
                                 "count_total" : {"$sum":1}}},{"$project": { "article_percentage" :{"$multiply": [100,{ "$divide": [ "$count_article", "$count_total"]}]}}},
                      {"$project": {"_id":0}}]
answer_10 = db.publications.aggregate(pipeline_answer_10)
for line in answer_10:
    print(line)


