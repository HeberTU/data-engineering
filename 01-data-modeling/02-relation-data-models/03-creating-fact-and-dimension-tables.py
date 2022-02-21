# -*- coding: utf-8 -*-
"""

Created on:
@author: 
Licence,
"""
from de_utils.postgres_utils import \
    connect_to_db, \
    create_db, \
    create_table

conn = connect_to_db()

conn = create_db(
    data_base='sparkifydb',
    conn=conn,
    switch=True
)


create_table(
    table_name='store',
    conn=conn,
    columns={
        'store_id': {
            'type': 'int',
            'constrain': 'PRIMARY KEY'},
        'state':  {
            'type': 'varchar'}

    }
)