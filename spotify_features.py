# script to extract features songs 
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



def features_creation():
    # spotipy connection 

    # app credentials
    CLIENT_SECRET = ""
    CLIENT_ID = ""


    def spotify_login(cid, secret):
        client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret) 
        return spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    # put above outside?
    sp = spotify_login(CLIENT_ID, CLIENT_SECRET)
    # path to scrapped data 
    full_path = os.path.expanduser('')
    df = pd.read_csv(full_path, encoding='utf-8-sig')

    # reduce data to song artist and uri to keep unique entries 
    df_songs = df.drop(["feat", "peak_pos","current_pos","prev_pos","streak","streams","country", "country_short"],axis=1).drop_duplicates()

    df_artists = df.drop(["track","track_uri", "feat", "peak_pos","current_pos","prev_pos","streak","streams","country", "country_short"],axis=1).drop_duplicates()
    
    print(f"unique artists: {df_artists.shape}")

    features = ['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 
            'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature']
    addition = []
    for i, batch in df_songs.groupby(np.arange(len(df_songs)) // 50):
        # batch uris to the allowed maximum call size
        batch_uris = list(batch['track_uri'])
        artists_uris = list(batch['artist_uri'])
        # get audio features for each song 
        results = sp.audio_features(batch_uris) 
        results_art = sp.artists(artists_uris)
        features_extracted = []
        for index, song in enumerate(results):
            if song:
                # FIX GENRE, an idea could be to look for the majority genre song
                if len(results_art['artists'][index]['genres']) > 0:
                    genre = [results_art['artists'][index]['genres'][0]]
                else:
                    genre = []

                features_extracted.append([batch_uris[index]]+[song[feature] for feature in features] + genre)

        addition.extend(features_extracted)

    print(f"found features for {len(addition)} out of {df_songs.shape[0]}")

    df_features = pd.DataFrame(addition, columns=['track_uri','danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 
                'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature', 'genre'])


    # inner join to remove songs that we weren't able to extract features 
    df_final = pd.merge(df, df_features, on='track_uri', how='inner')

    print("Completed feature gathering, extracting to csv")
    # path to extraction location
    full_path = os.path.expanduser('')
    #export
    df_final.to_csv(full_path, index=False, encoding='utf-8-sig')
    print("Data extracted")



features_creation()