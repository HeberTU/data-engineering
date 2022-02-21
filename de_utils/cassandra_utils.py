# -*- coding: utf-8 -*-
"""

Created on:
@author: 
Licence,
"""
from typing import List, Tuple, Union
import cassandra

from cassandra.cluster import Cluster


def create_sesion(
        ips: List[str] = ['127.0.0.1']
) -> Tuple[Cluster, cassandra.cluster.Session]:
    try:
        cluster = Cluster(ips)
        session = cluster.connect()
    except Exception as e:
        print(e)

    return cluster, session


def create_keyspace(
        keyspace: str,
        session: cassandra.cluster.Session,
        set_keyspace: bool
) -> int:
    query = f"CREATE KEYSPACE IF NOT EXISTS {keyspace}"
    query += " WITH REPLICATION = "
    query += "{'class': 'SimpleStrategy', 'replication_factor': 1}"
    try:
        session.execute(query)
    except Exception as e:
        print(e)

    if set_keyspace:
        try:
            session.set_keyspace(keyspace)
        except Exception as e:
            print(e)

    return 0


def create_table(
        table_name: str,
        session: cassandra.cluster.Session,
        table_params: dict[str: dict[str: Union[str, List[str]]]]
) -> int:
    query = f"CREATE TABLE IF NOT EXISTS {table_name}"

    columns = table_params.get('columns', None)

    if columns is None:
        print("Columns definition is missing")
        raise ValueError

    columns = [col + ' ' + col_type for col, col_type in columns.items()]
    columns = ', '.join(columns)

    query += '(' + columns

    pk = table_params.get('pk', None)

    if pk is None:
        print("PK Constrains is missing")
        raise ValueError

    pk = ', '.join(pk)

    query += ', PRIMARY KEY (' + pk + '));'

    try:
        session.execute(query)
    except Exception as e:
        print(e)

    return 0


def insert_into(
        table_name: str,
        session: cassandra.cluster.Session,
        data: dict[str: str]
):
    query = f'INSERT INTO {table_name} '

    column = ', '.join([col for col in data.keys()])

    query += '(' + column + ')'

    values = tuple([val for val in data.values()])

    query += ' VALUES (' + ('%s, ' * len(values))[:-2] + ')'

    try:
        session.execute(query, values)
    except Exception as e:
        print(e)

    return 0


def select_table(
        table_name: str,
        session: cassandra.cluster.Session,
        where: dict[str: Union[str, int]] = None
) -> cassandra.cluster.ResultSet:
    query = f"SELECT * FROM {table_name}"

    if where:
        where = "AND ".join(
            [col + "='" + val + "'" for col, val in where.items()])

        query += ' WHERE ' + where

    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    return rows


def drop_table(
        table_name: str,
        session: cassandra.cluster.Session
) -> int:
    query = f'DROP TABLE {table_name}'

    try:
        session.execute(query)
    except Exception as e:
        print(e)

    return 0


class Query:

    def __init__(self, session: cassandra.cluster.Session):

        self.select_columns = ''
        self.table_name = ''
        self.where_value = ''
        self.session = session

    def select(self, cols: str):

        self.select_columns = cols

        return self

    def from_table(self, table_name: str):
        self.table_name = table_name

        return self

    def where(self, conditions: str):
        self.where_value = conditions

        return self

    def build(self) -> str:
        where_clause = ''

        if self.where_value:
            where_clause = f"WHERE {self.where_value}"

        return f"SELECT {self.select_columns} FROM {self.table_name} {where_clause}"

    def execute(self):

        query = self.build()

        try:
            rows = self.session.execute(query)
        except Exception as e:
            print(e)

        return rows
