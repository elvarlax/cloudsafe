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


def text_to_datetime(text):
    return pd.to_datetime(text, format='%H:%M')


def filter_by_hour(df, start, end):
    start = text_to_datetime(start)
    end = text_to_datetime(end)
    return df[(df['time'] >= start) & (df['time'] < end)]


def select_all_departures(conn):
    return pd.read_sql("SELECT * FROM departures", conn)


def select_all_stations(conn):
    return pd.read_sql("SELECT * FROM stations", conn)


def select_stations_join_departures(conn):
    df = pd.read_sql("SELECT * FROM stations INNER JOIN departures d on stations.id = d.station_id", conn)
    df['time'] = text_to_datetime(df['time'])
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
    stations_filtered = filter_by_hour(stations, "08:00", "09:00")
    print(stations_filtered)
