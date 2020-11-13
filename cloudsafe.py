import sqlite3
from sqlite3 import Error

import pandas as pd


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn


def filter_by_hour(df, start, end):
    start = pd.to_datetime(start, format='%H:%M')
    end = pd.to_datetime(end, format='%H:%M')
    return df[start >= df['time'] < end]


def select_all_departures(conn):
    return pd.read_sql("SELECT * FROM departures", conn)


def select_all_stations(conn):
    return pd.read_sql("SELECT * FROM stations", conn)


def select_stations_join_departures(conn):
    df = pd.read_sql("SELECT * FROM stations INNER JOIN departures d on stations.id = d.station_id", conn)
    df['time'] = pd.to_datetime(df['time'], format='%H:%M')
    return df


def get_coords(row):
    if row['coordinates_here']:
        coords = row['coordinates_here'].split(',')
        return coords[0], coords[1]
    return None, None


# Only for testing (Use the Jupyter Notebook for Visualization)
if __name__ == "__main__":
    database = 'here-api/main.db'
    conn = create_connection(database)
    stations = select_stations_join_departures(conn)
    stations_between_10_and_11 = filter_by_hour(stations, "10:00", "11:00")
    print(stations_between_10_and_11)
