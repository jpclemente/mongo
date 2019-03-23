""" Ejecuci�n desde l�nea de comandos python
EjemploPymongo.py
mongod debe de estar corriendo"""
#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymongo import MongoClient


#conexion a la Base de Datos
connection = MongoClient ('localhost',27017)
db = connection.prueba

#si no existe, se crea la colecci�n alumnos
col=db.authors
[col.insert(item) for item in []]
# cerrar la conexi�n a mongodb
connection.close()
