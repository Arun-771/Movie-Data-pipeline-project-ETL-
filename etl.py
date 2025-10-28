import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import time
import re
from urllib.parse import quote


API_KEY = "293e033e" 
MYSQL_PASSWORD = "Aras1229#" 
DATABASE_URL = f"mysql+pymysql://root:{MYSQL_PASSWORD}@localhost/movies_db"


# HELPER FUNCTIONS 
def clean_movielens_title(title):
    """Cleans up common MovieLens title formats by moving articles and removing aliases."""
    
    # Handle articles: 'Title, The' -> 'The Title' (Includes Les, La, Le, etc.)
    match = re.search(r',\s(The|A|An|La|Le|Les|El|L\')$', title, re.IGNORECASE)
    
    if match:
        article = match.group(1) 
        title_base = title[:match.start()].strip()
        title = article + ' ' + title_base

    # Aggressively remove text in parentheses at the end (foreign titles, aliases)
    title = re.sub(r'\s\(.*?\)$', '', title).strip() 
    return title.strip()



# MYSQL Database Connection

def connect_db(db_url):
    """Establishes and tests the database connection."""
    try:
        engine = create_engine(db_url)
        with engine.connect():
            print(" Successfully connected to the MySQL database.")
        return engine
    except OperationalError as e:
        print(f" FATAL ERROR connecting to MySQL: {e}")
        print("   -> Please ensure MySQL server is running and database 'movies_db' exists.")
        exit(1)
    except Exception as e:
        print(f" An unexpected error occurred during DB connection: {e}")
        exit(1)


# OMDb API Fetch and Transform
def fetch_movie_data(title, year):
    """Fetches and cleans data from the OMDb API with retry logic and rate limiting."""
    
    encoded_title = quote(title) 
    url = f"http://www.omdbapi.com/?apikey={API_KEY}&t={encoded_title}"
    if year:
        url += f"&y={year}"

    # Rate limiting pause
    time.sleep(1) 
    
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status() 
        data = response.json()
        
        if data.get('Response') == 'True':
            """Data Cleaning and Conversion"""
            box_office_str = data.get('BoxOffice', 'N/A')
            box_office_int = int(re.sub(r'[$,]', '', box_office_str).replace('N/A', '0'))
            
            release_year_str = data.get('Year', '0').split('–')[0]
            release_year_int = int(release_year_str) if release_year_str.isdigit() else 0
            
            return {
                'director': data.get('Director', None),
                'plot': data.get('Plot', None),
                'box_office': box_office_int,
                'release_year': release_year_int,
            }
        
        # Fallback if search portion fails without the year 
        if year and data.get('Error') == 'Movie not found!':
            time.sleep(1)
            
            retry_url = f"http://www.omdbapi.com/?apikey={API_KEY}&t={encoded_title}"
            retry_response = requests.get(retry_url, timeout=5)
            retry_data = retry_response.json()
            
            if retry_data.get('Response') == 'True':
                 box_office_str = retry_data.get('BoxOffice', 'N/A')
                 box_office_int = int(re.sub(r'[$,]', '', box_office_str).replace('N/A', '0'))
                 
                 release_year_str = retry_data.get('Year', '0').split('–')[0]
                 release_year_int = int(release_year_str) if release_year_str.isdigit() else 0

                 return {
                    'director': retry_data.get('Director', None),
                    'plot': retry_data.get('Plot', None),
                    'box_office': box_office_int,
                    'release_year': release_year_int,
                }
        return None

    except requests.exceptions.RequestException:
        return None
    except Exception:
        return None


# Main ETL Logic
def run_etl(engine):
    print("\n--- Starting ETL Pipeline ---")
    
    # Read CSV Files
    try:
        movies_df = pd.read_csv('movies.csv')
        ratings_df = pd.read_csv('ratings.csv')
        print(" CSV files loaded successfully.")
    except FileNotFoundError:
        print(" ERROR: movies.csv or ratings.csv not found.")
        return

    # Enrichment and Transformation
    print(" Starting OMDb enrichment and transformation...")

    # Initialize new columns
    movies_df['director'] = None
    movies_df['plot'] = None
    movies_df['box_office'] = 0
    movies_df['release_year'] = 0 
    movies_df['decade'] = 0 

    for index, row in movies_df.iterrows():
        full_title = row['title']
        
        # Parse Year
        match = re.search(r'\s\((\d{4})\)$', full_title)
        
        if match:
            year_str = match.group(1)
            raw_title = full_title[:match.start()]
        else:
            raw_title = full_title
            year_str = None

        # Title Cleaning
        cleaned_title = clean_movielens_title(raw_title)

        omdb_data = fetch_movie_data(cleaned_title, year_str)
        
        if omdb_data:
            # Load enriched data
            for key, value in omdb_data.items():
                movies_df.at[index, key] = value
            
            # Feature Engineering (Decade)
            if omdb_data['release_year'] != 0:
                 movies_df.at[index, 'decade'] = (omdb_data['release_year'] // 10) * 10
        
        if (index + 1) % 500 == 0:
            print(f"   -> Processed {index + 1} movies...")

    print(" OMDb enrichment and transformation complete.")

    # Final Data Type Cleanup
    movies_df['box_office'] = movies_df['box_office'].fillna(0).astype('int64')
    movies_df['release_year'] = movies_df['release_year'].fillna(0).astype('int16')
    movies_df['decade'] = movies_df['decade'].fillna(0).astype('int16')
    
    ratings_df['rating'] = ratings_df['rating'].astype('float32')
    ratings_df = ratings_df.rename(columns={'timestamp': 'unix_timestamp'})
    
    # Load to Database
    print("💾 Loading final data to MySQL...")

    # 1. Load RATINGS table first [Child table]
    ratings_df[['userId', 'movieId', 'rating', 'unix_timestamp']].to_sql(
        'ratings', 
        con=engine, 
        if_exists='replace', 
        index=False
    )

    # 2. Load MOVIES table second [Parent table]
    movies_df[['movieId', 'title', 'genres', 'director', 'plot', 'box_office', 'release_year', 'decade']].to_sql(
        'movies', 
        con=engine, 
        if_exists='replace',
        index=False
    )

    print(" ETL pipeline finished successfully!")
    print("---")


if __name__ == "__main__":
    db_engine = connect_db(DATABASE_URL)
    run_etl(db_engine)
