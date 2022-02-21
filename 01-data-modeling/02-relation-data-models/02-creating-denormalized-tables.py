# -*- coding: utf-8 -*-
"""

Created on:
@author: 
Licence,
"""
from de_utils.postgres_utils import \
    connect_to_db, \
    create_table, \
    insert_values_in_table, \
    execute_query


conn = connect_to_db()

# Tables in 3rd Normal Form (3NF)

# Creating Tables
create_table(
    table_name='album_library',
    conn=conn,
    **{
        'album_id': 'int',
        'album_name': 'varchar',
        'artist_id': 'int',
        'year': 'int'
    }
)

create_table(
    table_name='artist_library',
    conn=conn,
    **{
        'artis_id': 'int',
        'artist_name': 'varchar'
    }
)

create_table(
    table_name='song_library',
    conn=conn,
    **{
        'song_id': 'int',
        'album_id': 'int',
        'song_name': 'varchar',
    }
)

create_table(
    table_name='song_length',
    conn=conn,
    **{
        'song_id': 'int',
        'song_length': 'int'
    }
)

# Inserting values in tables

insert_values_in_table(
    table_name='album_library',
    conn=conn,
    **{
        'album_id': 1,
        'album_name': 'Rubber Soul',
        'artist_id': 1,
        'year': 1965
    }
)

insert_values_in_table(
    table_name='album_library',
    conn=conn,
    **{
        'album_id': 2,
        'album_name': 'Let it Be',
        'artist_id': 1,
        'year': 1970
    }
)

insert_values_in_table(
    table_name='artist_library',
    conn=conn,
    **{
        'artis_id': 1,
        'artist_name': 'The Beatles'
    }
)

insert_values_in_table(
    table_name='song_library',
    conn=conn,
    **{
        'song_id': 1,
        'album_id': 1,
        'song_name': 'Michelle',
    }
)

insert_values_in_table(
    table_name='song_library',
    conn=conn,
    **{
        'song_id': 2,
        'album_id': 1,
        'song_name': 'Think For Yourself',
    }
)

insert_values_in_table(
    table_name='song_library',
    conn=conn,
    **{
        'song_id': 3,
        'album_id': 1,
        'song_name': 'In My Life',
    }
)

insert_values_in_table(
    table_name='song_library',
    conn=conn,
    **{
        'song_id': 4,
        'album_id': 2,
        'song_name': 'Let it Be',
    }
)

insert_values_in_table(
    table_name='song_library',
    conn=conn,
    **{
        'song_id': 5,
        'album_id': 2,
        'song_name': 'Across The Universe',
    }
)

insert_values_in_table(
    table_name='song_length',
    conn=conn,
    **{
        'song_id': 1,
        'song_length': 163
    }
)

insert_values_in_table(
    table_name='song_length',
    conn=conn,
    **{
        'song_id': 2,
        'song_length': 137
    }
)

insert_values_in_table(
    table_name='song_length',
    conn=conn,
    **{
        'song_id': 3,
        'song_length': 145
    }
)

insert_values_in_table(
    table_name='song_length',
    conn=conn,
    **{
        'song_id': 4,
        'song_length': 240
    }
)

insert_values_in_table(
    table_name='song_length',
    conn=conn,
    **{
        'song_id': 5,
        'song_length': 227
    }
)

# Join Tables
query = """SELECT artist_library.artis_id, artist_name, album_library.album_id,
album_name, year, song_library.song_id, song_name, song_length
FROM ((artist_library JOIN album_library ON 
artist_library.artis_id = album_library.artist_id) JOIN 
song_library ON album_library.album_id=song_library.album_id) JOIN
song_length ON song_library.song_id=song_length.song_id; 
"""

rows = execute_query(
    query=query,
    err_msg='Could not execute query',
    conn=conn,
    fetch=True
)

conn.close()