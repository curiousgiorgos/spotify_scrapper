# spotify_scrapper
## An Apache Airflow Spotify charts scrapper

The spotify_dag script specifies an Apache Airflow weekly jobs that scrapes the Spotify Charts website.
The data are then expanded by quering the Spotify API for each songs characterististics.
Lastly the job creates a sqlite database and laods the data.

This repository also includes the database with the weekly data for week 09/13/2022-09/19/2022 as well as a PBI file showcasing the data.
