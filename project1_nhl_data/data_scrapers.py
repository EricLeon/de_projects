# Required Imports
import pandas as pd
import psycopg2
import psycopg2.extras as extras
import requests

import config

# ----- DATA COLLECTION & STORAGE ----- #


def scrape_teams(table_name="team"):
    """
    Retrieves all NHL Team data and stores it in PostgreSQL.

    This function shouldn't need to be run often, as team information rarely changes.
    Table should be mostly static.

    Parameters
    ----------
    table_name : String
        SQL Table name -> Default "team"

    Returns
    ------

    """

    teams = get_teams()
    con = connect_to_db()
    create_table_team(db_conn=con, close_after=False, table=table_name)
    insert_data(db_conn=con, df=teams, table=table_name)


def scrape_players(table_name="player"):
    """
    Retrieves all NHL Player data and stores it in PostgreSQL.

    This function is built to be run on a regular basis to pick up and roster
    changes, trades, new palyers, etc.

    Parameters
    ----------
    table_name : String
        SQL Table name -> Default "player"

    Returns
    ------

    """

    players = get_players()
    con = connect_to_db()
    create_table_player(db_conn=con, close_after=False, table=table_name)
    insert_data(db_conn=con, df=players, table=table_name)


def scrape_stats(table_name="stats"):
    """
    Retrieves all NHL Player Stats and stores it in PostgreSQL.

    This function is built to be run on a regular basis so that the SQL table
    always has the most up to date statistics.

    Parameters
    ----------
    table_name : String
        SQL Table name -> Default "player"

    Returns
    ------

    """

    stats = get_stats()
    con = connect_to_db()
    create_table_stats(db_conn=con, close_after=False, table=table_name)
    insert_data(db_conn=con, df=stats, table=table_name)


# ----- CHILD FUNCTIONS ----- #


def connect_to_db(
    db=config.DB, user=config.USER, pw=config.PW, host=config.HOST, port=config.PORT
):
    """
    Connects to PostgreSQL NHL database and returns database connection object.

    Parameters
    ----------
    config parameters : String
        Config variables defined in configuration file

    Returns
    ------
    conn : Database Connection
        Database connection object to PostgreSQL NHL DB
    """

    try:
        conn = psycopg2.connect(
            database=db, user=user, password=pw, host=host, port=port
        )
        return conn
    except:
        print("Unable to connect to the database!")


def insert_data(db_conn, df, table):
    """
    Inserts Pandas DataFrame to PostgreSQL database.

    Parameters
    ----------
    conn : Database Connection
        Connection object for NHL Database

    df : DataFrame
        Data to insert

    table : String
        Target table name

    Returns
    ------

    """

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ",".join(list(df.columns))

    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = db_conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        db_conn.commit()
        db_conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        db_conn.rollback()
        cursor.close()
        return 0
    print(f"{df.shape[0]} rows inserted to SQL.")
    print()
    cursor.close()


def create_table_team(db_conn, close_after, table="team"):
    """
    Uses the DB connection and creates a TEAMS table to hold information
    returned via NHL API.

    I replace the table if it exists since the dataset is tiny, and it's
    rare that new teams are added or teams are moved.

    If this does happen, the Team ID will remain the same, but the attribute
    data will change, so I'd just want to overwrite the existing entry,
    hence replacing the entire table.

    Parameters
    ----------
    db_conn : Database Connection
        Database connection object returned from calling "connect_to_db"

    close_after : Bool
        Specifies whether or not to close database connection after creating table

    table : String
        Table name for new table -> Default: team

    Returns
    ------

    """

    cur = db_conn.cursor()
    try:
        cur.execute(
            f"""
        DROP TABLE IF EXISTS {table};         
        CREATE TABLE {table} (
        team_id         INTEGER PRIMARY KEY,
        name            VARCHAR(50), 
        arena_name      VARCHAR(50),
        arena_city      VARCHAR(50), 
        abbr            VARCHAR(3),
        location        VARCHAR(50), 
        initial_year    VARCHAR(4),
        division_name   VARCHAR(50),
        conference_name VARCHAR(50),
        active          BOOLEAN
        );"""
        )
        db_conn.commit()
        cur.close()
        if close_after:
            db_conn.close()

    except:
        print(f"Unable to create {table} table!")


def create_table_player(db_conn, close_after, table="player"):
    """
    Uses the DB connection and creates a PLAYERS table to hold information
    returned via NHL API.

    This is designed to be ran periodically to pick up any roster changes, or new players coming
    into the league.

    I've chosen for this project to re-write the entire table each time as there are <1000 active players at
    a given time so the data set is tiny. But as data grows I would check first using the player id and team id and
    only add new players. I'd also check for any players that have retired are inactive, and remove them at the same time.
    This is all taken care of automatically however when re-writing the entire table.

    Parameters
    ----------
    db_conn : Database Connection
        Database connection object returned from calling "connect_to_db"

    close_after : Bool
        Specifies whether or not to close database connection after creating table

    table : String
        Table name for new table -> Default: player

    Returns
    ------

    """

    cur = db_conn.cursor()
    try:
        cur.execute(
            f"""
        DROP TABLE IF EXISTS {table};         
        CREATE TABLE {table} (
        player_id       INTEGER PRIMARY KEY,
        team_id         INTEGER, 
        fname           VARCHAR(50),
        lname           VARCHAR(50),
        number          INTEGER,
        birthdate       DATE,
        birth_city      VARCHAR(50),
        birth_country   VARCHAR(50),
        nationality     VARCHAR(50),
        height_cm       REAL,
        weight_kg       REAL,
        handedness      VARCHAR(10),
        captain         BOOLEAN,
        alternate       BOOLEAN,
        position        VARCHAR(10),
        active          BOOLEAN,
        rookie          BOOLEAN
        );"""
        )
        db_conn.commit()
        cur.close()
        if close_after:
            db_conn.close()

    except:
        print(f"Unable to create {table} table!")


def create_table_stats(db_conn, close_after, table="stats"):
    """
    Uses the DB connection and creates a STATS table to hold information
    returned via NHL API.

    This is designed to be ran yearly (for now) as I am using the season by season API.
    I chose to rewrite the entire table as it's relatively quick to do with this amount of data, and it will also
    pick up any stat adjustments that are possibly made to historical data.

    If business need I could change to daily stats, but structure of table would remain, and then
    I'd just run this daily and append new rows only instead of re-writing the entire table.

    Parameters
    ----------
    db_conn : Database Connection
        Database connection object returned from calling "connect_to_db"

    close_after : Bool
        Specifies whether or not to close database connection after creating table

    table : String
        Table name for new table -> Default: stats

    Returns
    ------

    """

    cur = db_conn.cursor()
    try:
        cur.execute(
            f"""
        DROP TABLE IF EXISTS {table};         
        CREATE TABLE {table} (  
        player_id       INTEGER,
        season          VARCHAR(8),
        league_name     VARCHAR(30),
        goals           INTEGER,
        assists         INTEGER,
        team_id         INTEGER,
        pim             INTEGER,
        shots           INTEGER,
        games           INTEGER,
        pp_goals        INTEGER,
        pp_points       INTEGER,
        pp_toi_seconds  INTEGER,
        gwg             INTEGER,
        ot_goals        INTEGER,
        sh_goals        INTEGER,
        sh_points       INTEGER,
        sh_toi_seconds  INTEGER,
        plus_minus      INTEGER,
        shifts          INTEGER,
        blocked         INTEGER
        );"""
        )
        db_conn.commit()
        cur.close()
        if close_after:
            db_conn.close()

    except:
        print(f"Unable to create {table} table!")


def get_teams():
    """
    Uses the NHL API to request all current NHL teams and attributes.

    This function returns a DataFrame which is meant to represent the "TEAMS" SQL table.

    Any franchies that have moved or changed their names will keep their team ID, however the attributes
    will change. Because of this, the TEAM ID is the primary key for this table.

    Parameters
    ----------

    Returns
    ------
    df : DataFrame
        A Pandas Dataframe to represent the "TEAMS" table.
    """

    # Get all current teams
    print("Getting NHL TEAM data...")
    temp_data = []
    team_response = requests.get("https://statsapi.web.nhl.com/api/v1/teams")
    if team_response.status_code == 200:
        teams = team_response.json()["teams"]
        for team in teams:
            temp_data.append(
                {
                    "team_id": team["id"],
                    "name": team["name"],
                    "arena_name": team["venue"]["name"],
                    "arena_city": team["venue"]["city"],
                    "abbr": team["abbreviation"],
                    "location": team["locationName"],
                    "initial_year": team["firstYearOfPlay"],
                    "division_name": team["division"]["name"],
                    "conference_name": team["conference"]["name"],
                    "active": team["active"],
                }
            )
        return pd.DataFrame.from_dict(temp_data)

    else:
        print("Unable to query teams")


def get_players():
    """
    Uses the NHL API to request all current NHL Players on an active roster.

    This function returns a DataFrame which is meant to represent the "PLAYERS" SQL table.

    Parameters
    ----------

    Returns
    ------
    df : DataFrame
        A Pandas Dataframe to represent the "PLAYERS" table.
    """

    # Request team data to get all team IDs before querying each one
    print("Getting NHL PLAYER data...")
    temp_data = []
    team_response = requests.get("https://statsapi.web.nhl.com/api/v1/teams")
    if team_response.status_code == 200:
        teams = team_response.json()["teams"]
        for team in teams:
            tid = team["id"]

            # Using each team ID, request current roster and get each player ID
            # Then for each player on the roster, request data
            # I need to convert the weight from lbs to kg, and height from FT' IN" to cm
            roster_response = requests.get(
                f"https://statsapi.web.nhl.com/api/v1/teams/{tid}/roster"
            )
            if roster_response.status_code == 200:
                roster = roster_response.json()["roster"]

                for player in roster:
                    pid = player["person"]["id"]
                    pdata_response = requests.get(
                        f"https://statsapi.web.nhl.com/api/v1/people/{pid}"
                    )
                    if pdata_response.status_code == 200:
                        pdata = pdata_response.json()
                        pdict = {
                            "player_id": pid,
                            "team_id": tid,
                            "fname": pdata["people"][0]["firstName"],
                            "lname": pdata["people"][0]["lastName"],
                            "birthdate": pdata["people"][0]["birthDate"],
                            "birth_city": pdata["people"][0]["birthCity"],
                            "birth_country": pdata["people"][0]["birthCountry"],
                            "nationality": pdata["people"][0]["nationality"],
                            "height_cm": (
                                (
                                    int(
                                        pdata["people"][0]["height"]
                                        .split("'")[0]
                                        .strip()
                                    )
                                )
                                * 30.48
                            )
                            + (
                                (
                                    int(
                                        pdata["people"][0]["height"]
                                        .split("'")[1]
                                        .replace('"', "")
                                        .strip()
                                    )
                                )
                                * 2.54
                            ),
                            "weight_kg": round(pdata["people"][0]["weight"] * 0.453, 2),
                            "handedness": pdata["people"][0]["shootsCatches"],
                            "position": pdata["people"][0]["primaryPosition"]["code"],
                            "rookie": pdata["people"][0]["rookie"],
                            "active": pdata["people"][0]["active"],
                        }
                        try:
                            captain = pdata["people"][0]["captain"]
                        except KeyError:
                            captain = None
                        try:
                            alternate = pdata["people"][0]["alternateCaptain"]
                        except KeyError:
                            alternate = None
                        try:
                            number = pdata["people"][0]["primaryNumber"]
                        except KeyError:
                            number = None
                        pdict |= {
                            "captain": captain,
                            "alternate": alternate,
                            "number": number,
                        }
                        temp_data.append(pdict)
            else:
                print(f"Unable to query roster for team id {tid}!")
    else:
        print("Unable to query teams!")
    return pd.DataFrame.from_dict(temp_data)


def get_stats(player_table_name="player"):
    """
    Uses the NHL API to request all stats on NHL players by ID.

    This function returns a DataFrame which is meant to represent the "STATS" SQL table.

    Parameters
    ----------
    player_table_name : String
        Name of the SQL table that has the player information

    Returns
    ------
    df : DataFrame
        A Pandas Dataframe to represent the "STATS" table.
    """

    # Get all player ID's from the database and transform
    # the result (List of Tuples) into a set
    # TEAM ID CAN BE NULL HERE - MEANS INTERNATIONAL PLAY
    print("Getting NHL STATS data...")
    temp_data = []
    con = connect_to_db()
    cur = con.cursor()
    cur.execute(f"SELECT DISTINCT player_id FROM {player_table_name}")
    result = cur.fetchall()
    pids = set(item for p in result for item in p)

    for pid in pids:
        stat_response = requests.get(
            f"https://statsapi.web.nhl.com/api/v1/people/{pid}/stats?stats=yearByYear"
        )
        if stat_response.status_code == 200:
            stats = stat_response.json()["stats"][0]
            for season in stats["splits"]:
                sdict = {
                    "player_id": pid,
                    "season": season["season"],
                    "league_name": season["league"]["name"],
                }
                try:
                    goals = (season["stat"]["goals"],)
                except KeyError:
                    goals = None
                try:
                    assists = (season["stat"]["assists"],)
                except KeyError:
                    assists = None
                try:
                    team_id = (season["stat"]["id"],)
                except KeyError:
                    team_id = None
                try:
                    pim = (season["stat"]["pim"],)
                except KeyError:
                    pim = None
                try:
                    shots = (season["stat"]["shots"],)
                except KeyError:
                    shots = None
                try:
                    games = (season["stat"]["games"],)
                except KeyError:
                    games = None
                try:
                    pp_goals = (season["stat"]["powerPlayGoals"],)
                except KeyError:
                    pp_goals = None
                try:
                    pp_points = (season["stat"]["powerPlayPoints"],)
                except KeyError:
                    pp_points = None
                try:
                    pp_toi = season["stat"]["powerPlayTimeOnIce"]
                    pp_toi_seconds = (
                        (int(pp_toi.split(":")[0]) * 60) + (int(pp_toi.split(":")[1])),
                    )
                except KeyError:
                    pp_toi_seconds = None
                try:
                    gwg = (season["stat"]["gameWinningGoals"],)
                except KeyError:
                    gwg = None
                try:
                    ot_goals = (season["stat"]["overTimeGoals"],)
                except KeyError:
                    ot_goals = None
                try:
                    sh_goals = (season["stat"]["shortHandedGoals"],)
                except KeyError:
                    sh_goals = None
                try:
                    sh_points = (season["stat"]["shortHandedPoints"],)
                except KeyError:
                    sh_points = None
                try:
                    sh_toi = season["stat"]["shortHandedTimeOnIce"]
                    sh_toi_seconds = (
                        (int(sh_toi.split(":")[0]) * 60) + (int(sh_toi.split(":")[1])),
                    )
                except KeyError:
                    sh_toi_seconds = None
                try:
                    plus_minus = (season["stat"]["plusMinus"],)
                except KeyError:
                    plus_minus = None
                try:
                    shifts = (season["stat"]["shifts"],)
                except KeyError:
                    shifts = None
                try:
                    blocked = (season["stat"]["blocked"],)
                except KeyError:
                    blocked = None

                sdict |= {
                    "goals": goals,
                    "assists": assists,
                    "team_id": team_id,
                    "pim": pim,
                    "shots": shots,
                    "games": games,
                    "pp_goals": pp_goals,
                    "pp_points": pp_points,
                    "pp_toi_seconds": pp_toi_seconds,
                    "gwg": gwg,
                    "ot_goals": ot_goals,
                    "sh_goals": sh_goals,
                    "sh_points": sh_points,
                    "sh_toi_seconds": sh_toi_seconds,
                    "plus_minus": plus_minus,
                    "shifts": shifts,
                    "blocked": blocked,
                }
                temp_data.append(sdict)
        else:
            print(f"Unable to query stats for player id {pid}")
    return pd.DataFrame.from_dict(temp_data)
