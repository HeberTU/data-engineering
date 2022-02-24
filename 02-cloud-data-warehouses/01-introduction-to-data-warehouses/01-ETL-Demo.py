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

# Step 4: creating start schema.

## dimDate
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

## Customer Dimension
query = """
CREATE TABLE dimCustomer
(
  customer_key SERIAL PRIMARY KEY,
  customer_id  smallint NOT NULL,
  first_name   varchar(45) NOT NULL,
  last_name    varchar(45) NOT NULL,
  email        varchar(50),
  address      varchar(50) NOT NULL,
  address2     varchar(50),
  district     varchar(20) NOT NULL,
  city         varchar(50) NOT NULL,
  country      varchar(50) NOT NULL,
  postal_code  varchar(10),
  phone        varchar(20) NOT NULL,
  active       smallint NOT NULL,
  create_date  timestamp NOT NULL,
  start_date   date NOT NULL,
  end_date     date NOT NULL
);
"""

execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=False
)

## Movie Dimension
query = """
CREATE TABLE dimMovie
(
  movie_key          SERIAL PRIMARY KEY,
  film_id            smallint NOT NULL,
  title              varchar(255) NOT NULL,
  description        text,
  release_year       year,
  language           varchar(20) NOT NULL,
  original_language  varchar(20),
  rental_duration    smallint NOT NULL,
  length             smallint NOT NULL,
  rating             varchar(5) NOT NULL,
  special_features   varchar(60) NOT NULL
);
"""

execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=False
)


## Store Dimension
query = """
CREATE TABLE dimStore
(
  store_key           SERIAL PRIMARY KEY,
  store_id            smallint NOT NULL,
  address             varchar(50) NOT NULL,
  address2            varchar(50),
  district            varchar(20) NOT NULL,
  city                varchar(50) NOT NULL,
  country             varchar(50) NOT NULL,
  postal_code         varchar(10),
  manager_first_name  varchar(45) NOT NULL,
  manager_last_name   varchar(45) NOT NULL,
  start_date          date NOT NULL,
  end_date            date NOT NULL
);
"""

execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=False
)

## Sales Fact
query = """
CREATE TABLE factSales
(
  sales_key SERIAL PRIMARY KEY,
  date_key INTEGER NOT NULL REFERENCES dimDate(date_key),
  customer_key INTEGER NOT NULL REFERENCES dimCustomer(customer_key),
  movie_key INTEGER NOT NULL REFERENCES dimMovie(movie_key),
  store_key INTEGER NOT NULL REFERENCES dimStore(store_key),
  sales_amount NUMERIC NOT NULL
);
"""

execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=False
)

# Insert into Tables
## Date Dimension
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


## Customer Dimension
query = """
INSERT INTO dimCustomer (customer_key, customer_id, first_name, last_name, email, address, 
                         address2, district, city, country, postal_code, phone, active, 
                         create_date, start_date, end_date)
SELECT 
    c.customer_id as customer_key,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    a.address,
    a.address2,
    a.district,
    ci.city,
    co.country,
    a.postal_code,
    a.phone,
    c.active,
    c.create_date,
       now()         AS start_date,
       now()         AS end_date
FROM customer c
JOIN address a  ON (c.address_id = a.address_id)
JOIN city ci    ON (a.city_id = ci.city_id)
JOIN country co ON (ci.country_id = co.country_id);
"""

execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=False
)


## Customer Dimension
query = """
INSERT INTO dimMovie (movie_key, film_id, title, description, release_year, language, 
original_language, rental_duration, length, rating, special_features)
SELECT
    f.film_id as movie_key,
    f.film_id,
    f.title,
    f.description,
    f.release_year,
    l.name as language,
    orig_lang.name as original_language,
    f.rental_duration,
    f.length,
    f.rating,
    f.special_features
FROM film f
JOIN language l              ON (f.language_id=l.language_id)
LEFT JOIN language orig_lang ON (f.original_language_id = orig_lang.language_id);
"""

execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=False
)