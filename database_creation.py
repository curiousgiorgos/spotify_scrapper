import json
import re 
from time import sleep
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd 
import numpy as np
import requests 
import os 
from sqlalchemy.orm import sessionmaker
import sqlalchemy
import sqlite3

def database_creation():
    # DB LOCATION
    DATABASE_LOCATION = ""

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    # SQLITE LOCATION
    conn = sqlite3.connect('')
    cursor = conn.cursor()


    sql_query = """
    CREATE TABLE IF NOT EXISTS spotify_charts(
        track VARCHAR,
        artist VARCHAR,
        feat VARCHAR,
        current_Pos SMALLINT,
        peak SMALLINT,
        streak SMALLINT,
        streams INT,
        country VARCHAR,
        country_short VARCHAR,
        track_uri VARCHAR,
        artist_uri VARCHAR,
        danceability DECIMAL,
        energy DECIMAL,
        key DECIMAL,
        loudness DECIMAL,
        mode DECIMAL,
        speechiness DECIMAL,
        acousticness DECIMAL,
        damceability DECIMAL,
        instrumentalness DECIMAL,
        liveness DECIMAL,
        valence DECIMAL,
        tempo TINYINT,
        time_signature INT
    )
    """

    # create table if it doesn't exist 
    cursor.execute(sql_query)
    print("Opened database successfully")

    # location of data to 
    df = pd.read_csv('', encoding='utf-8-sig')

    # append will add ALL of the table to the data 
    df.to_sql("spotify_charts", con=engine, index=False, if_exists='replace')
    print("Database created")

    conn.commit()
    conn.close()

database_creation()