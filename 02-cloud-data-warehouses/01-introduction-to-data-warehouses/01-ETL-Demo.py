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

query = """
SELECT 
    f.title,
    ci.city,
    EXTRACT (month from p.payment_date) as month,
    sum(p.amount) as revenue
from payment p
join rental r on (p.rental_id=r.rental_id)
join inventory i on (r.inventory_id = i.inventory_id)
join film f on (i.film_id=f.film_id)
join customer c on (p.customer_id=c.customer_id)
join address a on (c.address_id=a.address_id)
join city ci on (a.city_id = ci.city_id)
group by (f.title, ci.city, month)
order by month, revenue desc 
limit 10;
"""
result = execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=True
)

print(pd.DataFrame(result))

# Step 4

query = """
CREATE TABLE dimDate (
date_key INTEGER  NOT NULL PRIMARY KEY,
date DATE NOT NULL,
year SMALLINT NOT NULL,
quarter SMALLINT NOT NULL,
month SMALLINT NOT NULL,
day SMALLINT NOT NULL,
week SMALLINT NOT NULL,
is_weekend BOOLEAN NOT NULL      
);
"""

execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=False
)

query = """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name   = 'dimdate'
"""
result = execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=True
)
print(pd.DataFrame(result))

query = """
INSERT INTO dimDate (date_key, date, year, quarter, month, day, week, is_weekend)
SELECT DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer) AS date_key,
       date(payment_date)                                           AS date,
       EXTRACT(year FROM payment_date)                              AS year,
       EXTRACT(quarter FROM payment_date)                           AS quarter,
       EXTRACT(month FROM payment_date)                             AS month,
       EXTRACT(day FROM payment_date)                               AS day,
       EXTRACT(week FROM payment_date)                              AS week,
       CASE WHEN                                                    
        EXTRACT(ISODOW FROM payment_date) IN (6, 7) THEN true       
         ELSE false                                                 
         END                                                        AS is_weekend
FROM payment;
"""

execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=False
)
