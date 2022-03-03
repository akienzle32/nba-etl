from airflow.decorators import dag, task
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import numpy
import pandas
import requests
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup

@dag(
    schedule_interval="0 0 * * *",
    start_date=datetime.today() - timedelta(days=1),
    dagrun_timeout=timedelta(minutes=60),
)

def Etl():
    dataframe = pandas.DataFrame()

    @task
    def get_data():
        # Import the xml
        url = 'https://www.basketball-reference.com/leagues/NBA_2022_per_game.html'
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'lxml')

        # Extract the column headings
        header = soup.find('thead')
        headings = [heading.text for heading in header.find_all('th')]
        headings = headings[1:]

        # Extract the data from the rows and load it into a data frame
        table = soup.find('tbody')
        raw_data = [[data.text for data in row.find_all('td')] for row in table.find_all('tr')]
        nba_df = pandas.DataFrame(raw_data, columns=headings)

        # Drop empty rows and replace cells with empty strings with null values
        nba_df = nba_df.dropna(how='all')
        nba_df = nba_df.replace(r'^\s*$', numpy.nan, regex=True)

        # Convert 'object' data types to ints and floats where necessary
        nba_df[['Age', 'G', 'GS']] = nba_df[['Age', 'G', 'GS']].astype(str).astype(int)
        nba_df[['MP','FG','FGA','FG%','3P','3PA','3P%','2P','2PA','2P%','eFG%','FT','FTA','FT%','ORB','DRB','TRB','AST','STL','BLK','TOV','PF','PTS']] = nba_df[['MP','FG','FGA','FG%','3P','3PA','3P%','2P','2PA','2P%','eFG%','FT','FTA','FT%','ORB','DRB','TRB','AST','STL','BLK','TOV','PF','PTS']].astype(str).astype(float)
        
        # Limit rows to players who have played at least half of the season and at least 10 minutes per game
        games_played = nba_df['G'].max()
        games_played_min = games_played / 2
        nba_df = nba_df[nba_df['G'] >= games_played_min]
        nba_df = nba_df[nba_df['MP'] >= 10]

        # Add columns for 'stocks' - steals plus blocks - and assist-to-turnover ratio
        nba_df['STK'] = nba_df['STL'] + nba_df['BLK']
        nba_df['AST/TOV'] = round(nba_df['AST'] / nba_df['TOV'], 1)

        # SQL didn't like the percent sign, so I'm renaming those columns
        nba_df = nba_df.rename(columns={'FG%': 'FGpct', '3P%': '3Ppct', '2P%': '2Ppct', 'eFG%': 'eFGpct', 'FT%': 'FTpct'})
        nba_df.to_csv('nba-per-game.csv')

    @task
    def load_data():
        try:
            load_dotenv()
            USERNAME = os.getenv('DB_USERNAME')
            PASSWORD = os.getenv('DB_PASSWORD')
            engine = create_engine(f'postgresql://{USERNAME}:{PASSWORD}@localhost:5432/nba')
            dataframe = pandas.read_csv('nba-per-game.csv')
            dataframe.to_sql('per_game', con=engine, if_exists='replace')
            return 0
        except Exception as e:
            return -1
    
    get_data() >> load_data()

dag = Etl()

