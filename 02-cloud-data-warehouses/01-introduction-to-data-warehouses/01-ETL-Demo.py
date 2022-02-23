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

query = """
SELECT f.title, p.amount, p.payment_date, p.customer_id 
from payment p 
join rental r on (p.rental_id=r.rental_id)
join inventory i on (r.inventory_id=i.inventory_id)
join film f on (i.film_id = f.film_id)
limit 5;
"""
result = execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=True
)

print(pd.DataFrame(result))


query = """
SELECT f.title, sum(p.amount) as revenue
from payment p 
join rental r on (p.rental_id=r.rental_id)
join inventory i on (r.inventory_id=i.inventory_id)
join film f on (i.film_id = f.film_id)
group by title
order by revenue desc
limit 5;
"""
result = execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=True
)

print(pd.DataFrame(result))


query = """
SELECT ci.city, sum(p.amount) as revenue
from payment p 
join customer c on (p.customer_id=c.customer_id)
join address a on (c.address_id=a.address_id)
join city ci on (a.city_id = ci.city_id)
group by ci.city
order by revenue desc
limit 5;
"""
result = execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=True
)

print(pd.DataFrame(result))


query = """
SELECT 
    sum(p.amount) as revenue,
    EXTRACT(month from p.payment_date) as month
from payment p 
group by month
order by revenue desc 
limit 10;
"""
result = execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=True
)

print(pd.DataFrame(result))
