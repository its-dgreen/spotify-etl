import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import requests
import json
from datetime import datetime
import datetime
from decouple import config
import psycopg2

DB_CONN = config('CONN_STRING')
USER_ID = "its-dgreen"
TOKEN = config('SPOTIFY_TOKEN')

def check_validity(df: pd.DataFrame) -> bool:
  if df.empty:
    print("No songs downloaded")
    return False
  
  if pd.Series(df["played_at"]).is_unique:
    pass
  else:
    raise Exception("Primary Key Validation failed")

  if df.isnull().values.any():
    raise Exception("Null values found")

  yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
  yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

  timestamps = df["timestamp"].tolist()
  for timestamp in timestamps:
    if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
      raise Exception("At least one of the songs is not from the requested time period (last 24 hours)")
  
  return True

if __name__ == "__main__":
  
  headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": "Bearer {token}".format(token=TOKEN)
  }

  today = datetime.datetime.now()
  yesterday = today - datetime.timedelta(days=1)
  timestamp = int(yesterday.timestamp()) * 1000

  r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={timestamp}".format(timestamp=timestamp),
                    headers = headers)
  data = r.json()

  songs = []
  artists = []
  played_at = []
  timestamps = []

  for song in data["items"]:
    songs.append(song["track"]["name"])
    artists.append(song["track"]["album"]["artists"][0]["name"])
    played_at.append(song["played_at"])
    timestamps.append(song["played_at"][0:10])

  song_dict = {
    "song": songs,
    "artist": artists,
    "played_at": played_at,
    "timestamp": timestamps
  }

  song_df = pd.DataFrame(song_dict, columns = ["song", "artist", "played_at", "timestamp"])

  if check_validity(song_df):
    print("Data is valid, loading to database")
  
  engine = sqlalchemy.create_engine(DB_CONN)
  conn = psycopg2.connect(DB_CONN)
  cursor = conn.cursor()

  sql_query = """
  CREATE TABLE IF NOT EXISTS recently_played_songs (
      song VARCHAR(255),
      artist VARCHAR(255),
      played_at VARCHAR(255) PRIMARY KEY,
      timestamp VARCHAR(255)
  )
  """

  cursor.execute(sql_query)
  conn.commit()
  print("Opened database successfully")

  try:
      song_df.to_sql("recently_played_songs", engine, index=False, if_exists='append')
  except:
      print("Data already exists in the database")

  conn.close()
  print("Close database successfully")
