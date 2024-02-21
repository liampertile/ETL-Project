from decouple import config
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import sqlite3
import sqlalchemy
from datetime import datetime, timedelta


scope = "user-read-recently-played"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    
    client_id = config('CLIENT_ID')
    ,client_secret = config('CLIENT_SECRET')
    ,redirect_uri = config('SPOTIPY_REDIRECT_URI')
    ,scope = scope
    
))


def extract(date, limit = 50):
    """
   
    datetime (ds) -> date to query 
    limit (int) -> the limit of elements to query

    """
    
    ds = int(date.timestamp()) * 1000 # that's because spotipy uses unix format
    
    return sp.current_user_recently_played(limit=limit, after=ds)

    
def transform(raw_data , date):
    data = []
    
    for r in raw_data["items"]:
        data.append(
            {
                "played_at":r["played_at"],
                "artist":r["track"]["artists"][0]["name"],
                "track":r["track"]["name"]
            }
        )
    
    df = pd.DataFrame(data)
    # clean_df = df[pd.to_datetime(df["played_at"]).dt.date == date.date()]
    # uncomment, in case you want the songs played specifically in this day!
    
    #validation
    
    if not df["played_at"].is_unique:
        raise Exception("the data is corrupt, is impossible to have more than one played_at")
    
    if df.isnull().values.any():
        raise Exception("the data is corrupt, a value in df is null")
    
    return df
    
def load(df):
    
    engine = sqlalchemy.create_engine(config('DATABASE_LOCATION'))
    conn = sqlite3.connect("played_songs")
    cursor = conn.cursor()  
    
    query = """
    CREATE TABLE IF NOT EXISTS played_tracks(
        song_name VARCHAR(100),
        artist_name VARCHAR(100),
        played_at VARCHAR(100),
        timestamp VARCHAR(100),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    cursor.execute(query)
    
    try:
        df.to_sql("played_songs" , engine , index = False , if_exists = 'append' )
    except:
        print("already exist in the db")
    
    

if __name__ == "__main__":
    
    date = datetime.today() - timedelta(days=50) #put days=1 if you want specifically this day
    
    #extract
    data_raw = extract(date)
    print(f"Extracted {len(data_raw["items"])} registers")
    
    #transform
    clean_df = transform(data_raw,date)
    print(f"{clean_df.shape[0]} regs after transform")
    
    #load
    load(clean_df)
    print("work done")