import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup as bs
import pandas as pd
import mysql.connector
from database import cursor, pw
import sqlalchemy

# Set database and logfile

DB_NAME = 'NBA_Stats'
engine = sqlalchemy.create_engine('mysql+pymysql://root:{}@localhost:3306/{}'.format(pw, DB_NAME))
df_player = pd.DataFrame()
df_team = pd.DataFrame()
logfile = "NBA_ETL_logfile.txt"

# Set paths / URLS for extraction

player_totals_url = r"https://www.nba.com/stats/players/traditional/?sort=PTS&dir=-1&Season=2021-22&SeasonType=Regular%20Season&PerMode=Totals"
team_totals_url = r"https://www.nba.com/stats/teams/traditional/?sort=W_PCT&dir=-1&Season=2021-22&SeasonType=Regular%20Season&PerMode=Totals"


# For running the log file

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("NBA_ETL_logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')


# Creates the database if not available already

def create_database():
    cursor.execute(
        "CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))


# To check if data is valid

def valid_data(df_player, df_team) -> bool:

    # Check if dataframe is empty
    if df_player.empty:
        print("Player DateFrame Empty")
        return False 
    if df_team.empty:
        print("Team DateFrame Empty")
        return False 

    # Primary Key Check
    if pd.Series(df_player['Player']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    if pd.Series(df_team['Team']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df_player.isnull().values.any():
        raise Exception("Null values found")

    if df_team.isnull().values.any():
        raise Exception("Null values found")

    return True


# Extract Function

def extract():

    # web driver
    path = "C:\Program Files (x86)\chromedriver.exe" # Needs to be path to the web driver on your machine. 
    driver = webdriver.Chrome(path)

    # Player stats extraction
    driver.get(player_totals_url)
    time.sleep(5)
    select_players = Select(driver.find_element_by_xpath(r"/html/body/main/div/div/div[2]/div/div/nba-stat-table/div[1]/div/div/select"))
    time.sleep(5)
    select_players.select_by_index(0)   
    time.sleep(5)
    element_players = driver.page_source
    player_soup = bs(element_players, 'lxml')

    # Team stats extraction
    driver.get(team_totals_url)
    time.sleep(5)
    element_teams = driver.page_source
    team_soup = bs(element_teams, 'lxml')

    driver.close()
    driver.quit()

    # Finding stat tables from soup objects
    player_table = player_soup.find("div", attrs = {"class": "nba-stat-table__overflow"})
    team_table = team_soup.find("div", attrs = {"class": "nba-stat-table__overflow"})

    # Soup to DataFrame
    df_player = pd.read_html(str(player_table))[0]
    df_team = pd.read_html(str(team_table))[0]

    return df_player, df_team


# Transform Function

def transform(df_player, df_team):

    # Drop unnessary data
    df_player = df_player.drop(columns=[
        'Unnamed: 0','+/-', 'GP RANK', 'W RANK', 'L RANK', 'MIN RANK', 'PTS RANK',
        'FGM RANK', 'FGA RANK', 'FG% RANK', '3PM RANK', '3PA RANK', '3P% RANK',
        'FTM RANK', 'FTA RANK', 'FT% RANK', 'OREB RANK', 'DREB RANK', 'REB RANK',
        'AST RANK', 'TOV RANK', 'STL RANK', 'BLK RANK', 'PF RANK', 'FP RANK',
        'DD2 RANK', 'TD3 RANK', '+/- RANK'
    ])

    # Drop unnessary data
    df_team = df_team.drop(columns=[
        'Unnamed: 0','+/-', 'GP RANK', 'W RANK', 'L RANK', 'WIN% RANK', 'MIN RANK',
        'PTS RANK', 'FGM RANK', 'FGA RANK', 'FG% RANK', '3PM RANK', '3PA RANK',
        '3P% RANK', 'FTM RANK', 'FTA RANK', 'FT% RANK', 'OREB RANK', 'DREB RANK',
        'REB RANK', 'AST RANK', 'TOV RANK', 'STL RANK', 'BLK RANK', 'BLKA RANK',
        'PF RANK', 'PFD RANK', '+/- RANK'
    ])

    # Rename columns to match our database
    df_player = df_player.rename(columns = { 
        'PLAYER': 'Player', 'TEAM': 'Team', 'AGE': 'Age', 'GP': 'Games_Played', 
        'W': 'Wins', 'L': 'Losses', 'MIN': 'Minutes_Played', 'PTS': 'Points', 
        'FGM': 'Field_Goals_Made', 'FGA': 'Field_Goals_Attempted', 
        'FG%': 'Field_Goals_perc', '3PM': 'Threes_Made', '3PA': 'Threes_Attempted', 
        '3P%': 'Threes_perc', 'FTM': 'Free_Throws_Made', 'FTA': 'Free_Throws_Attempted', 
        'FT%': 'Free_Throws_perc', 'OREB': 'O_Rebounds', 'DREB': 'D_Rebounds', 
        'REB': 'Rebounds', 'AST': 'Assists', 'TOV': 'Turnovers', 'STL': 'Steals', 
        'BLK': 'Blocks', 'PF': 'Personal_Fouls', 'FP': 'Fantasy_Points', 
        'DD2': 'Double_Doubles', 'TD3': 'Triple_Doubles'
    })

    # Rename columns to match our database
    df_team = df_team.rename(columns = { 
        'TEAM': 'Team', 'GP': 'Games_Played', 'W': 'Wins', 'L': 'Losses', 
        'WIN%': 'Win_perc', 'MIN': 'Minutes_Played', 'PTS': 'Points', 
        'FGM': 'Field_Goals_Made', 'FGA': 'Field_Goals_Attempted', 
        'FG%': 'Field_Goals_perc', '3PM': 'Threes_Made', '3PA': 'Threes_Attempted', 
        '3P%': 'Threes_perc', 'FTM': 'Free_Throws_Made', 'FTA': 'Free_Throws_Attempted', 
        'FT%': 'Free_Throws_perc', 'OREB': 'O_Rebounds', 'DREB': 'D_Rebounds', 
        'REB': 'Rebounds', 'AST': 'Assists', 'TOV': 'Turnovers', 'STL': 'Steals', 
        'BLK': 'Blocks', 'BLKA': 'Blocked_Field_Goal_Attempts', 'PF': 'Personal_Fouls', 
        'PFD': 'Personal_Fouls_Drawn'
    })

    return df_player, df_team


# Load Function

def load(df_player, df_team):
    
    create_database()

    # Creates Player table if not created yet
    player_table_query = """
        CREATE TABLE IF NOT EXISTS `player_stats` (
            `Player` varchar(100) NOT NULL,
            `Team` varchar(100) NOT NULL,
            `Age` int NOT NULL,
            `Games_Played` int DEFAULT NULL,
            `Wins` int DEFAULT NULL,
            `Losses` int DEFAULT NULL,
            `Minutes_Played` int DEFAULT NULL,
            `Points` int DEFAULT NULL,
            `Field_Goals_Made` int DEFAULT NULL,
            `Field_Goals_Attempted` int DEFAULT NULL,
            `Field_Goals_perc` float DEFAULT NULL,
            `Threes_Made` int DEFAULT NULL,
            `Threes_Attempted` int DEFAULT NULL,
            `Threes_perc` float DEFAULT NULL,
            `Free_Throws_Made` int DEFAULT NULL,
            `Free_Throws_Attempted` int DEFAULT NULL,
            `Free_Throws_perc` float DEFAULT NULL,
            `O_Rebounds` int DEFAULT NULL,
            `D_Rebounds` int DEFAULT NULL,
            `Rebounds` int DEFAULT NULL,
            `Assists` int DEFAULT NULL,
            `Turnovers` int DEFAULT NULL,
            `Steals` int DEFAULT NULL,
            `Blocks` int DEFAULT NULL,
            `Personal_Fouls` int DEFAULT NULL,
            `Fantasy_Points` float DEFAULT NULL,
            `Double_Doubles` int DEFAULT NULL,
            `Triple_Doubles` int DEFAULT NULL,
            PRIMARY KEY (`Player`),
            UNIQUE KEY `Player_UNIQUE` (`Player`)
        )
    """

    # Creates Team table if not created yet
    team_table_query = """
        CREATE TABLE IF NOT EXISTS `team_stats` (
            `Team` varchar(50) NOT NULL,
            `Games_Played` int DEFAULT NULL,
            `Wins` int DEFAULT NULL,
            `Losses` int DEFAULT NULL,
            `Win_perc` float DEFAULT NULL,
            `Minutes_Played` int DEFAULT NULL,
            `Points` int DEFAULT NULL,
            `Field_Goals_Made` int DEFAULT NULL,
            `Field_Goals_Attempted` int DEFAULT NULL,
            `Field_Goals_perc` float DEFAULT NULL,
            `Threes_Made` int DEFAULT NULL,
            `Threes_Attempted` int DEFAULT NULL,
            `Threes_perc` float DEFAULT NULL,
            `Free_Throws_Made` int DEFAULT NULL,
            `Free_Throws_Attempted` int DEFAULT NULL,
            `Free_Throws_perc` float DEFAULT NULL,
            `O_Rebounds` int DEFAULT NULL,
            `D_Rebounds` int DEFAULT NULL,
            `Rebounds` int DEFAULT NULL,
            `Assists` int DEFAULT NULL,
            `Turnovers` int DEFAULT NULL,
            `Steals` int DEFAULT NULL,
            `Blocks` int DEFAULT NULL,
            `Blocked_Field_Goal_Attempts` int DEFAULT NULL,
            `Personal_Fouls` int DEFAULT NULL,
            `Personal_Fouls_Drawn` int DEFAULT NULL,
            PRIMARY KEY (`Team`),
            UNIQUE KEY `Team_UNIQUE` (`Team`)
        )
    """

    # Selects database and adds tables if needed. 
    cursor.execute("USE {}".format(DB_NAME))
    cursor.execute(player_table_query)
    cursor.execute(team_table_query)

    # Loading DataFrames to MySql
    df_player.to_sql('player_stats', con=engine, index=False, if_exists='replace')
    df_team.to_sql('team_stats', con=engine, index=False, if_exists='replace')

    cursor.close()
    return


# ETL process

def run_nba_etl():
    log("ETL Job Started")
    log("Extract phase Started")

    df_player, df_team = extract()

    log("Extract phase Ended")
    log("Transform phase Started")

    df_player, df_team = transform(df_player, df_team)

    log("Transform phase Ended")
    log("Load phase Started")

    if valid_data(df_player, df_team):

        print("DataFrames valid")
        load(df_player, df_team)
        log("Load phase Ended")
    else:
        print("DataFrames not valid")

    log("ETL Job Ended")

run_nba_etl()
