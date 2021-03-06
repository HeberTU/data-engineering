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


query = """
SELECT d.day, m.rating, c.city, sum(s.sales_amount) as revenue  
from factsales s 
LEFT join dimdate d on (s.date_key=d.date_key)
LEFT join dimmovie m on (s.movie_key=m.movie_key)
LEFT join dimcustomer c on (s.customer_key=c.customer_key)
GROUP BY day, rating, city
ORDER BY revenue desc
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
SELECT d.day, m.rating, c.city, sum(s.sales_amount) as revenue  
from factsales s 
LEFT join dimdate d on (s.date_key=d.date_key)
LEFT join dimmovie m on (s.movie_key=m.movie_key)
LEFT join dimcustomer c on (s.customer_key=c.customer_key)
WHERE rating = 'PG-13'
GROUP BY day, rating, city
ORDER BY revenue desc
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
SELECT d.day, m.rating, c.city, sum(s.sales_amount) as revenue  
from factsales s 
LEFT join dimdate d on (s.date_key=d.date_key)
LEFT join dimmovie m on (s.movie_key=m.movie_key)
LEFT join dimcustomer c on (s.customer_key=c.customer_key)
WHERE 
    rating IN ('PG-13', 'PG') 
    AND city IN ('Lancaster', 'Bellevue')
    AND day IN (1, 15, 30)
GROUP BY day, rating, city
ORDER BY revenue desc
limit 5;
"""
result = execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=True
)

print(pd.DataFrame(result))

# Roll-up
query = """
SELECT d.day, m.rating, c.country, sum(s.sales_amount) as revenue  
from factsales s 
LEFT join dimdate d on (s.date_key=d.date_key)
LEFT join dimmovie m on (s.movie_key=m.movie_key)
LEFT join dimcustomer c on (s.customer_key=c.customer_key)
GROUP BY day, rating, country
ORDER BY revenue desc
limit 5;
"""
result = execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=True
)

print(pd.DataFrame(result))

# Drill-down
query = """
SELECT d.day, m.rating, c.district, sum(s.sales_amount) as revenue  
from factsales s 
LEFT join dimdate d on (s.date_key=d.date_key)
LEFT join dimmovie m on (s.movie_key=m.movie_key)
LEFT join dimcustomer c on (s.customer_key=c.customer_key)
GROUP BY day, rating, district
ORDER BY revenue desc
limit 10;
"""
result = execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=True
)

print(pd.DataFrame(result))


# Grouping Sets
query = """
SELECT d.month, st.country, sum(s.sales_amount) as revenue  
from factsales s 
LEFT join dimdate d on (s.date_key=d.date_key)
LEFT join dimstore st on (s.store_key=st.store_key)
GROUP BY 
    GROUPING SETS (
        (),
        (month),
        (country),
        (month, country)   
    )
ORDER BY month, country, revenue desc;
"""
result = execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=True
)

result = pd.DataFrame(result)
result.columns = ['month', 'country', 'revenue']
result[result.country == 'Australia']

# CUBE
query = """
SELECT d.month, st.country, sum(s.sales_amount) as revenue  
from factsales s 
LEFT join dimdate d on (s.date_key=d.date_key)
LEFT join dimstore st on (s.store_key=st.store_key)
GROUP BY cube(month, country)
ORDER BY month, country, revenue desc;
"""
result = execute_query(
    query=query,
    err_msg="KO",
    conn=conn,
    fetch=True
)

result = pd.DataFrame(result)
result.columns = ['month', 'country', 'revenue']
result[result.country == 'Australia']


conn.close()