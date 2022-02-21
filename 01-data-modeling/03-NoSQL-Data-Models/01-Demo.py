# -*- coding: utf-8 -*-
"""

Created on:
@author: 
Licence,
"""
TOTAL_DATA = [
    (1970, "The Beatles", "Let it Be"),
    (1965, "The Beatles", "Rubber Soul"),
    (1965, "The Who", "My Generation"),
    (1966, "The Monkees", "The Monkees"),
    (1970, "The Carpenters", "Close To You")
]


from de_utils.cassandra_utils import \
    create_sesion, \
    create_keyspace, \
    create_table, \
    insert_into, \
    select_table, \
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
    table_params ={
        'columns':{
            'year': 'int',
            'artist_name': 'text',
            'album_name': 'text'},
        'pk':['year', 'artist_name']
    }
)

create_table(
    table_name='album_library',
    session=session,
    table_params={
        'columns':{
            'year': 'int',
            'artist_name': 'text',
            'album_name': 'text'},
        'pk':['artist_name', 'year']
    }
)

for record in TOTAL_DATA:

    insert_into(
        table_name='album_library',
        session=session,
        data={
            'year': record[0],
            'artist_name': record[1],
            'album_name': record[2]
        }
    )

rows = select_table(
    table_name='album_library',
    session=session,
    where={
        'artist_name': 'The Beatles'
    }
)

for row in rows:
    print(row)

drop_table(
    table_name='album_library',
    session=session
)

drop_table(
    table_name='music_library',
    session=session
)

session.shutdown()
cluster.shutdown()