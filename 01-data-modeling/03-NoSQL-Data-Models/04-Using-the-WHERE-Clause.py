# -*- coding: utf-8 -*-
"""

Created on:
@author: 
Licence,
"""
TOTAL_DATA = [
    (1970, "The Beatles", "Let it Be", "Liverpool"),
    (1965, "The Beatles", "Rubber Soul", "Oxford"),
    (1965, "The Who", "My Generation", "London"),
    (1966, "The Monkees", "The Monkees", "Los Angeles"),
    (1970, "The Carpenters", "Close To You", "San Diego")
]

from de_utils.cassandra_utils import \
    create_sesion, \
    create_keyspace, \
    create_table, \
    insert_into, \
    Query, \
    drop_table

cluster, session = create_sesion()

create_keyspace(
    keyspace='udacity',
    session=session,
    set_keyspace=True
)


create_table(
    table_name='music_library',
    session=session,
    table_params={
        'columns': {
            'year': 'int',
            'artist_name': 'text',
            'album_name': 'text',
            'city': 'text'},
        'pk': ['year', 'artist_name', 'album_name'
               ]
    }
)

for record in TOTAL_DATA:

    insert_into(
        table_name='music_library',
        session=session,
        data={
            'year': record[0],
            'artist_name': record[1],
            'album_name': record[2],
            'city': record[3]
        }
    )

# Query 1: Give me every album in my music library that was
#  released in a 1965 year
query = Query(session=session)
rows = query.\
    select('*').\
    from_table('music_library').\
    where("year=1965").\
    execute()

for row in rows:
    print(row)

# Query 2: Give me the album that is in my music library
# that was released in 1965 by "The Beatles"
query = Query(session=session)
rows = query.\
    select('*').\
    from_table('music_library').\
    where("year=1965 and artist_name='The Beatles'").\
    execute()

for row in rows:
    print(row)

# Query 3: Give me all the albums released in a given
# year that was made in London
query = Query(session=session)
rows = query.\
    select('album_name').\
    from_table('music_library').\
    where("year=1965 and city='London'").\
    execute()

for row in rows:
    print(row)

# Query 4: Give me the city that the album "Rubber Soul" was recorded
query = Query(session=session)
rows = query.\
    select('city').\
    from_table('music_library').\
    where("year=1965 and artist_name='The Beatles' and album_name='Rubber Soul'").\
    execute()

for row in rows:
    print(row)


drop_table(
    table_name='music_library',
    session=session
)

session.shutdown()
cluster.shutdown()

