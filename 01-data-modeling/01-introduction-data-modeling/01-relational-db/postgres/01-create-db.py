# -*- coding: utf-8 -*-
"""

Created on:
@author: 
Licence,
"""
import psycopg2

#
conn = psycopg2.connect(
    database="postgres",
    user='postgres',
    password='heber',
    host='127.0.0.1', # Local host
    port= '5432' # Defaul port @ installation
)
conn.autocommit = True
cursor = conn.cursor()

sql = '''CREATE database mydb''';

cursor.execute(sql)
print("Database created successfully........")

conn.close()
