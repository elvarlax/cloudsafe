import pandas as pd
import geopandas as gpd
import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn


def select_all_departures(conn):
    return pd.read_sql("SELECT * FROM departures", conn)


def select_all_stations(conn):
    return pd.read_sql("SELECT * FROM stations", conn)


def select_all_bus_stops():
    return gpd.read_file('here-api/bus-stops.geojson')


def frequency(df):
    return df.value_counts()


# Only for testing (Use the Jupyter Notebook for Visualization)
if __name__ == "__main__":
    database = 'here-api/main.db'
    conn = create_connection(database)
    departures = select_all_departures(conn)
    stations = select_all_stations(conn)
    bus_stops = select_all_bus_stops()
    freq_dep = frequency(departures)
    freq_sta = frequency(stations)
    conn.close()
