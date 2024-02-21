from decouple import Config
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import sqlite3
import datetime
import sqlalchemy
import timedelta

scope = "user-read-recently-played"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    
    client_id = Config('CLIENT_ID')
    ,client_secret = Config('CLIENT_SECRET')
    ,redirect_uri = Config('SPOTIPY_REDIRECT_URI')
    ,scope = scope
    
))

def extract(date, limit = 30):
    """
   
    datetime (ds) -> date to query 
    limit (int) -> the limit of elements to query

    """
    
    ds = int(date.timestamp()) * 1000 # that's because spotipy uses unix format
    
    return sp.current_user_recently_played(limit = limit , after = ds)

    pass
    
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
    
    clean_df = df[pd.to_datetime(df["played_at"]).dt.date == date.date()]
    
    #validation
    
    if not df["played_at"].is_unique:
        raise Exception("the data is corrupt, is impossible to have more than one played_at")
    
    if df.isnull().values.any():
        raise Exception("the data is corrupt, a value in df is null")
    
    return clean_df
    
def load(df):
    pass

if __name__ == "__main__":
    date = datetime.today() - timedelta(days = 1)
    
    #extract
    data_raw = extract(date)
    print(f"Extracted {len(data_raw["items"])} registers")
    
    #transform
    clean_df = transform(data_raw,date)
    print(f"{clean_df.shape[0]} regs after transform")
    