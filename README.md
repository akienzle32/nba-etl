# nba-etl

This repository hosts the code for an automated ETL pipeline I developed in order to learn Apache Airflow. The data is first extracted from a table 
on Basketball Reference (https://www.basketball-reference.com/leagues/NBA_2022_per_game.html) using the Beautiful Soup webscraper. Then I clean the raw
data and perform a few simple transformations on it using the Pandas library. Finally, the transformed data is loaded into a Postgres table. 
