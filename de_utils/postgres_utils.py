# -*- coding: utf-8 -*-
"""

Created on:
@author: 
Licence,
"""
import psycopg2


def connect_to_db(
        database: str = 'postgres',
        user: str = 'postgres',
        password: str = 'heber',
        host: str = '127.0.0.1',
        port: str = '5432'):
    """

    :type database: object
    """
    try:
        conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,  # Local host
            port=port  # Default port @ installation
        )

    except psycopg2.Error as e:
        print("Could not connecto to db")
        print(e)

    print(f"Connected to {database}")

    conn.set_session(autocommit=True)

    return conn




def execute_query(query: str, err_msg: str, conn, fetch: bool=False):

    with conn.cursor() as cur:
        try:
            cur.execute(query)

        except psycopg2.Error as e:
            print(err_msg)
            print(e)

        if fetch:

            rows = cur.fetchall()

            return rows


def create_db(data_base: str, conn, switch: bool = True):

    query = f"CREATE database {data_base}"

    execute_query(
        query=query,
        err_msg=f"Could not create db: {data_base}",
        conn=conn,
        fetch=False
    )

    if switch:
        conn = connect_to_db(
            database = data_base,
            user='postgres',
            password='heber',
            host='127.0.0.1',
            port='5432')

        return conn


def create_table(table_name:str, conn, **columns):

    query = f"CREATE TABLE IF NOT EXISTS {table_name}"
    columns = ', '.join(
        [str(k) + ' ' + str(v) for k, v in columns.items()]
    )
    query += '(' + columns + ')'
    print("Creating Table as:")
    print(query)

    execute_query(
        query=query,
        err_msg="Could not create table",
        conn=conn)


    return 1

def insert_values_in_table(table_name: str, conn, **kwargs):

    columns = ', '.join([str(k) for k in kwargs.keys()])
    values = tuple([v for v in kwargs.values()])

    place_holders = ', '.join(
        [ "%s" for i in range(len(values))])


    query = f"INSERT INTO {table_name} ({columns})"+\
                f" VALUES ({place_holders});"


    with conn.cursor() as cur:
        try:
            cur.execute(
                query=query,
                vars=values
            )
        except psycopg2.Error as e:
            print("Could not insert values")
            print(e)

    return 1

def read_table(table_name: str, conn):

    query = f"SELECT * FROM {table_name}"

    rows = execute_query(
        query=query,
        err_msg=f"Could not read table {table_name}",
        conn=conn,
        fetch=True
    )

    return rows


def join_tables(
        table_name_1: str,
        table_name_2:str,
        on_1: str,
        on_2: str,
        conn):

    query = f"SELECT * FROM {table_name_1} JOIN {table_name_2}"

    query += f" ON {table_name_1}.{on_1} ="

    query += f" {table_name_2}.{on_2}"

    rows = execute_query(
        query=query,
        err_msg=f"Could not join tables",
        conn=conn,
        fetch=True
    )

    return rows

def delet_table():
    pass

