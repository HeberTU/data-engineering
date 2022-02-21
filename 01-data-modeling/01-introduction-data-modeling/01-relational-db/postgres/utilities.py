# -*- coding: utf-8 -*-
"""

Created on:
@author: 
Licence,
"""
from psycopg2 import connect, Error

def create_db(db_name:str, user_name: str, password:str)->int:

    try:
        conn = connect(
            database="postgres",
            user='postgres',
            password='heber',
            host='127.0.0.1',  # Local host
            port='5432'  # Defaul port @ installation
        )
    except Error as e:
        print("Cold not connect to data base server")
        print(e)

    conn.autocommit = True
    cursor = conn.cursor()

    sql = f'CREATE database {db_name};'

    try:
        cursor.execute(sql)
        print(f"Database {db_name} created successfully........")
    except Error as e:
        print("Could not create database")
        print(e)

    sql = f"CREATE USER {user_name} WITH PASSWORD '{password}';"

    try:
        cursor.execute(sql)
    except Error as e:
        print(f"could not create user {user_name}")
        print(e)

    sql = f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {user_name};"

    try:
        cursor.execute(sql)
    except Error as e:
        print(f"could not give privileges on {db_name} to {user_name}")
        print(e)

    conn.close()

    return 1