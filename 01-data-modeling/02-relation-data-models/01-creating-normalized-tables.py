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
    read_table, \
    join_tables


conn = connect_to_db()

# Bad Way
create_table(
    'music_library',
    conn=conn,
    **{
        'album_id': 'int',
        'album_name': 'varchar',
        'artist_name': 'varchar',
        'year': 'int',
        'songs': 'text[]'
    }
)


insert_values_in_table(
    table_name='music_library',
    conn=conn,
    **{
        'album_id': 1,
        'album_name': 'Rubber Soul',
        'artist_name': 'The Beatles',
        'year': 1965,
        'songs': ['Michelle', 'Think for yourself', 'In my life']
    }
)


insert_values_in_table(
    table_name='music_library',
    conn=conn,
    **{
        'album_id': 2,
        'album_name': 'Let Tt Be',
        'artist_name': 'The Beatles',
        'year': 1970,
        'songs': ["Let It Be", "Across de Universe"]
    }
)

rows = read_table(
    table_name='music_library',
    conn=conn,
)

# Moving into 1st Normal Form (1NF)

create_table(
    'music_library_2',
    conn=conn,
    **{
        'album_id': 'int',
        'album_name': 'varchar',
        'artist_name': 'varchar',
        'year': 'int',
        'song_name': 'varchar'
    }
)

insert_values_in_table(
    table_name='music_library_2',
    conn=conn,
    **{
        'album_id': 1,
        'album_name': 'Rubber Soul',
        'artist_name': 'The Beatles',
        'year': 1965,
        'song_name': 'Michelle'
    }
)

insert_values_in_table(
    table_name='music_library_2',
    conn=conn,
    **{
        'album_id': 2,
        'album_name': 'Rubber Soul',
        'artist_name': 'The Beatles',
        'year': 1965,
        'song_name': 'Think for yourself'
    }
)

insert_values_in_table(
    table_name='music_library_2',
    conn=conn,
    **{
        'album_id': 3,
        'album_name': 'Rubber Soul',
        'artist_name': 'The Beatles',
        'year': 1965,
        'song_name': 'In my life'
    }
)

insert_values_in_table(
    table_name='music_library_2',
    conn=conn,
    **{
        'album_id': 4,
        'album_name': 'Let Tt Be',
        'artist_name': 'The Beatles',
        'year': 1970,
        'song_name': "Let It Be"
    }
)

insert_values_in_table(
    table_name='music_library_2',
    conn=conn,
    **{
        'album_id': 5,
        'album_name': 'Let Tt Be',
        'artist_name': 'The Beatles',
        'year': 1970,
        'song_name': "Across de Universe"
    }
)

rows = read_table(
    table_name='music_library_2',
    conn=conn
)

# Moving into 2nd Normal Form (2NF)

create_table(
    'album_library',
    conn=conn,
    **{
        'album_id': 'int',
        'album_name': 'varchar',
        'artist_name': 'varchar',
        'year': 'int'
    }
)

create_table(
    'song_library',
    conn=conn,
    **{
        'song_id': 'int',
        'album_id': 'int',
        'song_name': 'varchar'
    }
)

insert_values_in_table(
    table_name='album_library',
    conn=conn,
    **{
        'album_id': 1,
        'album_name': 'Rubber Soul',
        'artist_name': 'The Beatles',
        'year': 1965
    }
)

insert_values_in_table(
    table_name='album_library',
    conn=conn,
    **{
        'album_id': 2,
        'album_name': 'Let Tt Be',
        'artist_name': 'The Beatles',
        'year': 1970
    }
)

for song in ['Think for yourself', 'In my life']:

    rows = read_table(
        table_name='song_library',
        conn=conn,
    )

    if len(rows) == 0:
        song_id = 1
    else:
        song_id = rows[-1][0] + 1

    insert_values_in_table(
        table_name='song_library',
        conn=conn,
        **{
            'song_id': song_id,
            'album_id': 1,
            'song_name': song
        }
    )


for song in ["Let It Be", "Across de Universe"]:

    rows = read_table(
        table_name='song_library',
        conn=conn,
    )

    if len(rows) == 0:
        song_id = 1
    else:
        song_id = rows[-1][0] + 1

    insert_values_in_table(
        table_name='song_library',
        conn=conn,
        **{
            'song_id': song_id,
            'album_id': 2,
            'song_name': song
        }
    )

rows = read_table(
        table_name='song_library',
        conn=conn,
    )

rows = join_tables(
    table_name_1='album_library',
    table_name_2='song_library',
    on_1='album_id',
    on_2='album_id',
    conn=conn
)