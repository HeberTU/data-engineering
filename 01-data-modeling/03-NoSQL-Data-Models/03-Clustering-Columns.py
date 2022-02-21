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
        'pk': ['album_name', 'artist_name']
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

query = Query(session=session)
rows = query.\
    select('*').\
    from_table('music_library').\
    where("album_name='Close To You'").\
    execute()


for row in rows:
    print(row)
