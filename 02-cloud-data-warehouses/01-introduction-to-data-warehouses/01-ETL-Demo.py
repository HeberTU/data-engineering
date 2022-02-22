# -*- coding: utf-8 -*-
"""

Created on:
@author: 
Licence,
"""
import pandas as pd
from de_utils.postgres_utils import \
    connect_to_db, \
    execute_query


conn = connect_to_db(
    database='pagila'
)


queries = [
    "select count(*) from store;",
    "select count(*) from film;",
    "select count(*) from customer;",
    "select count(*) from rental;",
    "select count(*) from payment;",
    "select count(*) from staff;",
    "select count(*) from city;",
    "select count(*) from country;",
    "select min(payment_date) as start, max(payment_date) as end from payment;"
]

for query in queries:
    result = execute_query(
        query=query,
        err_msg="KO",
        conn=conn,
        fetch=True
    )
    print(query)
    print(result)

query="select district, count(address) as n from address group by district order by n DESC, district LIMIT 10"
result = execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=True
)

pd.DataFrame(result, columns=['district', 'n'])