# nba-etl

This repository hosts an automated ETL pipeline I built in order to learn Apache Airflow. The pipeline itself is fairly simple:
First, I scrape some tabular NBA data from the Basketball Reference website (https://www.basketball-reference.com/leagues/NBA_2022_per_game.html) using 
Python's Beautiful Soup library. Then, I clean and perform a few transformations on it using pandas. Finally, I load the transformed data into a Postgres
database.
