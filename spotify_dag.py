#script that creates the Airflow DAG 


from datetime import datetime, timedelta
from textwrap import dedent 

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator

from spotify_scrapper import extract 
from spotify_features import features_creation
from database_creation import database_creation


with DAG(
    'spotify_dag',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=10),
        'execution_timeout': timedelta(minutes=10)
    },
    description='DAG to pull data from the spotify API ',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2022, 8, 24),
    tags=['spotify'],
) as dag:
    
    t1 = PythonOperator(
            task_id='extract_data',
            python_callable=extract,
            dag=dag)
    t2 = PythonOperator(
            task_id='add_features',
            python_callable=features_creation,
            dag=dag)
    t3 = PythonOperator(
            task_id='create_database',
            python_callable=database_creation,
            dag=dag)

    drop_artist_table = SqliteOperator(
        task_id='drop_artist_dim',
        sqlite_conn_id="spotify_db_sqlite",

        sql='''
            DROP TABLE IF EXISTS dim_artists;
        ''' 
    )
    create_artist_table = SqliteOperator(
        task_id='create_artist_dim',
        sqlite_conn_id="spotify_db_sqlite",

        sql='''
            CREATE TABLE dim_artists AS
                SELECT sp.artist,
                       sp.artist_uri,
                       sp.genre
                FROM spotify_charts sp
                group by sp.artist_uri
        ''' 
    )
    drop_song_table = SqliteOperator(
        task_id='drop_song_dim',
        sqlite_conn_id="spotify_db_sqlite",

        sql='''
            DROP TABLE IF EXISTS dim_songs;
        ''' 
    )
    create_song_table = SqliteOperator(
        task_id='create_song_dim',
        sqlite_conn_id="spotify_db_sqlite",

        sql='''
            CREATE TABLE dim_songs AS
                SELECT sp.track, 
                       sp.track_uri,
                       sp.artist,
                       sp.artist_uri,
                       sp.feat,
                       sp.energy,
                       sp.loudness,
                       sp.mode,
                       sp.key,
                       sp.danceability,
                       sp.speechiness,
                       sp.acousticness,
                       sp.instrumentalness,
                       sp.liveness,
                       sp.valence,
                       sp.tempo,
                       sp.duration_ms,
                       sp.time_signature
                FROM spotify_charts sp
                group by sp.track_uri ;
        ''' 
    )
    drop_fact_table = SqliteOperator(
        task_id='drop_fact_table',
        sqlite_conn_id="spotify_db_sqlite",

        sql='''
           DROP TABLE IF EXISTS fact_charts;
        ''' 
    )
    create_fact_table = SqliteOperator(
        task_id='create_fact_table',
        sqlite_conn_id="spotify_db_sqlite",

        sql='''
           CREATE TABLE fact_charts AS
                SELECT sp.track, 
                       sp.track_uri,
                       sp.artist,
                       sp.artist_uri,
                       sp.current_pos,
                       sp.peak_pos,
                       sp.prev_pos,
                       sp.streak,
                       sp.streams,
                       sp.country,
                       sp.country_short
                FROM spotify_charts sp;
        ''' 
    )
    drop_count_table = SqliteOperator(
        task_id='drop_count_table',
        sqlite_conn_id="spotify_db_sqlite",

        sql='''
           DROP TABLE IF EXISTS dim_count;
        ''' 
    )
    create_count_table = SqliteOperator(
        task_id='create_count_dim',
        sqlite_conn_id="spotify_db_sqlite",

        sql='''
           CREATE TABLE IF NOT EXISTS dim_count AS
                SELECT sp.country,
                       sp.country_short
                FROM spotify_charts sp
                group by sp.country;
        ''' 
    )
    drop_genre_table = SqliteOperator(
        task_id='drop_genre_table',
        sqlite_conn_id="spotify_db_sqlite",

        sql='''
           DROP TABLE IF EXISTS dim_genre;
        ''' 
    )
    create_genre_table = SqliteOperator(
        task_id='create_genre_dim',
        sqlite_conn_id="spotify_db_sqlite",

        sql='''
           CREATE TABLE IF NOT EXISTS dim_genre AS
                SELECT sp.genre
                FROM spotify_charts sp
                group by sp.genre;
        ''' 
    )
    # create dependencies
    t1 >> t2 >> t3 >> [create_count_table<<drop_genre_table, create_genre_table<<drop_count_table,create_artist_table<<drop_artist_table, create_song_table<<drop_song_table, create_fact_table<<drop_fact_table]