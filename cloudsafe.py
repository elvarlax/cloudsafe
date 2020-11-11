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


def frequency(df):
    return df.value_counts()


def get_coords(row):
    if row['coordinates_here']:
        coords = row['coordinates_here'].split(',')
        return coords[0], coords[1]
    return None, None


# Only for testing (Use the Jupyter Notebook for Visualization)
if __name__ == "__main__":
    database = 'here-api/main.db'
    conn = create_connection(database)
    departures = select_all_departures(conn)
    stations = select_all_stations(conn)
    bus_stops = select_all_bus_stops()

    coords = stations.iloc[0].coordinates_here.split(',')
    m = folium.Map([coords[0], coords[1]], zoom_start=10)
    stations[['latitude', 'longitude']] = stations.apply(get_coords, axis=1, result_type="expand")

    # convert to (n, 2) nd-array format for heatmap
    stationArr = stations[['latitude', 'longitude']].to_numpy()

    # choropleth map
    frequencies = stations.name_overpass.value_counts()

    # stations
    rows = []
    for station in frequencies.keys():
        first = stations.loc[stations['name_overpass'] == station].iloc[0]
        rows.append({'station_name': station,
                     'longitude': first.longitude,
                     'latitude': first.latitude,
                     'bus_count': frequencies.get(station)})

    final = pd.DataFrame(rows)
    final['longitude'] = final['longitude'].astype(float)
    final['latitude'] = final['latitude'].astype(float)

    top_right = [final['latitude'].max(), final['longitude'].max()]
    bottom_left = [final['latitude'].min(), final['longitude'].min()]
    grid = get_geojson_grid(top_right, bottom_left)

    counts = []

    for box in grid:
        upper_right = box["properties"]["upper_right"]
        lower_left = box["properties"]["lower_left"]
        items = final.loc[(final['latitude'] <= upper_right[1]) &
                          (final['longitude'] <= upper_right[0]) &
                          (final['longitude'] >= lower_left[0]) &
                          (final['latitude'] >= lower_left[1])]
        counts.append(len(items))
    max_count = max(counts)

    for i, geo_json in enumerate(grid):
        color = plt.cm.Reds(counts[i] / max_count)
        color = mpl.colors.to_hex(color)
        gj = folium.GeoJson(geo_json,
                            style_function=lambda feature, color=color: {
                                'fillColor': color,
                                'color': "black",
                                'weight': 2,
                                'dashArray': '5, 5',
                                'fillOpacity': 0.55,
                            })

        m.add_child(gj)

    conn.close()
