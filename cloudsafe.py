import sqlite3
from sqlite3 import Error

import folium
import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from utils.folium_utils import get_geojson_grid


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


def get_coords(row):
    if row['coordinates_here']:
        coords = row['coordinates_here'].split(',')
        return coords[0], coords[1]
    return None, None


# Only for testing (Use the Jupyter Notebook for Visualization)
if __name__ == "__main__":
    pass
