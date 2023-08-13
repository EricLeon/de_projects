# NHL Player Data Engineering Project

## Summary:
- Built an extract/load system in Python which collects NHL Team, Player & Statistics information using their public (undocumented) API and then stores it in respective PostgreSQL tables on my local machine.
- Utlised the collected data to create various stored procedures & views to represent business use-cases
- Created various Tableau dashboards to visualise the data and ensure it's in the correct form for analysis


## Project Goal(s):
- The reason for this project was to practise basic data engineering skills as part of the self-created online data engineering course
- The main skills that I wanted to practise during this project were:
	a) Dimensional Data Modelling (Data warehousing & DBMS concepts)
	b) Collecting data via an API using Python
	c) Storing data in a DBMS (PostgreSQL) using Python
	d) SQL queries (Using the data to practise mainly stored procedures, creating views, etc.)


## Data Collection

I created a Python script which, once orchestrated, is able to be run periodically to collect NHL Team, Player & Statistics information and store the data in SQL tables.

*The data_collection.py file is the file that would be orchestrated and ran periodically, while the data_scrapers.py file contains the actual functions I wrote to scrape the data*


## Next Steps

- Continue to utilise the data to practise SQL admin/RDMBS tasks (creating stored procedures, views) as well as more complex queries (CTE, window functions) etc.
- Scraper / data can be reused for another project. Once I learn more advanced data engineering tools I plan to test them out using this already created pipeline.


## Code & Tools Used
- **Python Version:** 3.11
- **Python Libraries:** Requests, Psycopg2, Pandas, Numpy, PostgreSQL, SQL
