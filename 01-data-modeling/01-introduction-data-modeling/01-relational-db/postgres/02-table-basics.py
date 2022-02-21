# -*- coding: utf-8 -*-
"""

Created on:
@author: 
Licence,
"""
import psycopg2
from .utilities import create_db

create_db(db_name='studentdb', user_name='student', password='student')

try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get curser to the Database")
    print(e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS songs (song_title varchar, artist_name varchar, year int, album_name varchar, single boolean);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)

## TO-DO: Finish the INSERT INTO statement with the correct arguments

try:
    cur.execute("INSERT INTO songs (song_title, artist_name, year, album_name, single) \
                 VALUES (%s, %s, %s, %s, %s)", \
                ( "Across The Universe", "The Beatles", 1970, "Let It Be", False))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

song_title="Think For Yourself"
artist_name="The Beatles"
year=1965
album_name="Rubber Soul"
single="False"


try:
    cur.execute(f"INSERT INTO songs (song_title, artist_name, year, album_name, single) VALUES ('{song_title}', '{artist_name}', {year}, '{album_name}', {single})")
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

## TO-DO: Finish the SELECT * Statement
try:
    cur.execute("SELECT * FROM songs;")
except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

cur.close()
conn.close()