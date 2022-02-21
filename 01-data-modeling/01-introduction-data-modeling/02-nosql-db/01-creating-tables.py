# -*- coding: utf-8 -*-
"""

Created on:
@author: 
Licence,
"""
import cassandra

from cassandra.cluster import Cluster


try:
    cluster = Cluster()
    session = cluster.connect()
except Exception as e:
    print(e)

try:
    session.execute("""select * from music_library""")
except Exception as e:
    print(e)

try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity
    WITH REPLICATION =
    {'class' : 'SimpleStrategy', 'replication_factor' : 1}"""
                    )
except Exception as e:
    print(e)

try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

query = "CREATE TABLE IF NOT EXISTS music_library"
query += "(year int, artist_name text, album_name text, single boolean,  PRIMARY KEY (year, artist_name))"

try:
    session.execute(query)
except Exception as e:
    print(e)

query = "select count(*) from music_library"

try:
    count = session.execute(query)
except Exception as e:
    print(e)

print(count.one())


query = "INSERT INTO music_library (year, artist_name, album_name, single)"
query += " VALUES (%s, %s, %s, %s)"

try:
    session.execute(query, (1970, "The Beatles", "Let it Be", False))
except Exception as e:
    print(e)

try:
    session.execute(query, (1965, "The Beatles", "Rubber Soul", False))
except Exception as e:
    print(e)

query = "SELECT * FROM music_library"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row)


query = "select * from music_library WHERE YEAR=1970 AND artist_name='The Beatles'"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row)


query = "drop table music_library"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)


session.shutdown()
cluster.shutdown()