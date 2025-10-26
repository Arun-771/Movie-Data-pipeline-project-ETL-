Movie Data Analysis ETL Pipeline
1. Overview of the Solution
This project implements a complete Extract, Transform, Load (ETL) pipeline designed to ingest raw movie and rating data from the MovieLens dataset,
enrich it with detailed movie metadata from the OMDb API, and store the final, cleaned data in a MySQL relational database (movies_db)
for subsequent analytical querying.

Project Deliverables
*etl.py: Python script that performs data extraction, cleaning, API enrichment, transformation, and database loading.
*schema.sql: SQL script defining the required movies and ratings table structures.
*queries.sql: SQL script containing the four analytical queries used to extract insights.
*README.md: This document.

2. Setup and Execution
 python3.verion
 3.xMySQL Server (running locally)OMDb API Key: A free key is required (limited to 1,000 daily requests).
 Data Files:The small version of movies.csv (\approx 1,000 rows) and ratings.csv must be in the project root.

3.Environment Setup
VScode,mysql,
git clone https://github.com/Arun-771/Movie-Data-pipeline-project
cd Movie-Data-pipeline-project-ETL
""" python libraries used in etl
import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import time
import re
from urllib.parse import quote
""""

4.ETL design
The ETL pipeline was built with a focus on robust data quality and efficient database loading. 
A key design choice was using aggressive title cleaning via 
Python's Regular Expressions to normalize MovieLens titles (e.g., removing aliases and reversing articles like 'Title, The'), 
which maximized the success rate of the subsequent API calls. To ensure stability, a mandatory one-second delay was programmed 
into the API loop to strictly adhere to OMDb's rate limits. Data type integrity was maintained by explicitly casting string data. 
Finally, a bonus feature was added by engineering a decade column from the release year, 
providing a valuable dimension for temporal analysis within the MySQL database.

Challenges and SolutionsThis ETL project faced three primary technical hurdles. 
The most critical was managing the OMDb API rate limits, which was solved by implementing a time.sleep(1) delay before each request 
to ensure the \approx 1,000 calls completed successfully. 
Next, a persistent Foreign Key error was fixed by reversing the table load order in etl.py (loading ratings before movies) to resolve database constraints. 
Finally, accurately analyzing average ratings required handling the pipe-separated genre string; 
for submission, the simple grouping method was used, though the need for more complex SQL string-splitting logic was acknowledged for truly accurate individual genre analysis.
