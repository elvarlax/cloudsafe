"""
This script contains functions that retrieve data from the db in order to be further processed or displayed.
"""

import sqlite3
import pandas as pd
import folium
import matplotlib.pyplot as plt
import matplotlib as mpl
# https://towardsdatascience.com/interactive-controls-for-jupyter-notebooks-f5c94829aee6
import ipywidgets as widgets
from ipywidgets import interact, interact_manual
from utils.folium_utils import get_copenhagen_grid, get_geojson_grid, get_coords


def get_bus_ids():
    """
    Retrieve the list of all buses ids.

    Returns:
        list: The list of bus ids.
    """
    conn = sqlite3.connect('data/main.db')
    cursor = conn.cursor()

    stmt = 'SELECT id FROM buses;'
    bus_ids = cursor.execute(stmt).fetchall()

    cursor.close()
    conn.close()

    return [bus_id[0] for bus_id in bus_ids]


def get_routes_geojson(bus_ids=None, flip_coordinates=True):
    """
    Generate the routes GeoJson for the provided bus ids.
    This can be used to overlay it on top of a map in the jupyter notebooks.

    Args:
        bus_ids (list, optional): List of the bus ids. If not provided all buses will be considered. Defaults to None.
        flip_coordinates (bool, optional): Flip the coordinates to comply with GeoJson. Defaults to True.

    Returns:
        dict: The dict that represents the GeoJson.
    """
    conn = sqlite3.connect('data/main.db')
    cursor = conn.cursor()

    geo_json = {
        "type": "FeatureCollection",
        "properties": {
            # "routes_ids": 
        },
        "features": []
    }

    if bus_ids is None:
        stmt = 'SELECT id, name, from_station_name, to_station_name FROM buses;'
        buses = cursor.execute(stmt).fetchall()
    else:
        stmt = f"""SELECT id, name, from_station_name, to_station_name FROM buses 
            WHERE id IN ({','.join(['?'] * len(bus_ids))});"""
        buses = cursor.execute(stmt, bus_ids).fetchall()

    for bus in buses:
        stmt = 'SELECT DISTINCT segment FROM routes_points WHERE bus_id = ? ORDER BY segment;'
        segments = cursor.execute(stmt, [bus[0]])
        coordinates_list = []
        for segment in segments:
            stmt_sub = 'SELECT coordinates FROM routes_points WHERE bus_id = ? AND segment = ? ORDER BY segment, seq;'
            coordinates = cursor.execute(stmt_sub, (bus[0], segment[0])).fetchall()
            coordinates_list.append(
                [list(map(float, row[0].split(',')[::-1] if flip_coordinates else row[0].split(','))) for row in
                 coordinates])

        route = {
            "type": "Feature",
            "properties": {
                "name": bus[1],
                "from": bus[2],
                "to": bus[3],
                "stroke": "#ef500a",
                "stroke-width": 3
            },
            "geometry": {
                "type": "MultiLineString",
                "coordinates": coordinates_list,
            }
        }
        geo_json['features'].append(route)

    cursor.close()
    conn.close()

    return geo_json


def get_grid_geojson(flip_coordinates=True):
    """
    Generate the grid GeoJson based on the values saved in grid_cells table.
    This can be used to overlay it on top of the routes in the jupyter notebooks.

    Args:
        flip_coordinates (bool, optional): Flip the coordinates to comply with GeoJson. Defaults to True.

    Returns:
        dict: The dict that represents the GeoJson.
    """
    conn = sqlite3.connect('data/main.db')
    cursor = conn.cursor()

    geo_json = {
        "type": "FeatureCollection",
        "properties": {
            # "routes_ids": 
        },
        "features": []
    }

    stmt = 'SELECT x_axis, y_axis, upper_left, upper_right, lower_right, lower_left FROM grid_cells;'

    for cell in cursor.execute(stmt).fetchall():
        coordinates_list = [
            list(map(float, cell[2].split(',')[::-1] if flip_coordinates else cell[2].split(','))),
            list(map(float, cell[3].split(',')[::-1] if flip_coordinates else cell[3].split(','))),
            list(map(float, cell[4].split(',')[::-1] if flip_coordinates else cell[4].split(','))),
            list(map(float, cell[5].split(',')[::-1] if flip_coordinates else cell[5].split(',')))
        ]
        cell = {
            "type": "Feature",
            "properties": {
                "matrix_coordinates": f"({cell[0]},{cell[1]})",
                "stroke": "#002244",
                "stroke-width": 2
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [coordinates_list]
            }
        }
        geo_json['features'].append(cell)

    cursor.close()
    conn.close()

    return geo_json


def get_route_cells(bus_ids=None):
    """
    Get the list with all the cells in the grid that the buses pass through.
    The cells are in a tuple format where the first element is the x-axis and the second is the y-axis in the grid matrix.
    It can be used to calculate the covered area in the optimization algorithm by generating the grid matrix first.

    Args:
        bus_ids (list, optional): List of the bus ids. If not provided all buses will be considered. Defaults to None.

    Returns:
        dict: The dict that contains a list of cells for each bus id. The cells are not necessarily ordered.
    """
    conn = sqlite3.connect('data/main.db')
    cursor = conn.cursor()

    if bus_ids is None:
        stmt = """SELECT bus_id, x_axis, y_axis FROM routes_cells rc, grid_cells gc 
            WHERE rc.cell_id = gc.id;"""
        route_cells = cursor.execute(stmt).fetchall()
    else:
        stmt = f"""SELECT bus_id, x_axis, y_axis FROM routes_cells rc, grid_cells gc 
            WHERE rc.cell_id = gc.id AND bus_id IN ({','.join(['?'] * len(bus_ids))});"""
        route_cells = cursor.execute(stmt, bus_ids).fetchall()

    route_cells_agg = {}
    for route_cell in route_cells:
        if route_cell[0] not in route_cells_agg.keys():
            route_cells_agg[route_cell[0]] = []
        route_cells_agg[route_cell[0]].append((route_cell[1], route_cell[2]))

    cursor.close()
    conn.close()

    return route_cells_agg


def merge_geojson(geo_routes, geo_grid):
    map = folium.Map(location=[55.676098, 12.568337], zoom_start=12)  # Starting map
    folium.GeoJson(geo_routes).add_to(map)  # Routes layer
    folium.GeoJson(geo_grid).add_to(map)  # Grid layer
    return map


def text_to_datetime(text):
    return pd.to_datetime(text, format='%H:%M')


def filter_by_hour(df, start, end):
    start = text_to_datetime(start)
    end = text_to_datetime(end)
    return df[(df['time'] >= start) & (df['time'] < end)]


def select_all_departures():
    conn = sqlite3.connect('data/main.db')
    all_departures = pd.read_sql("SELECT * FROM departures", conn)
    conn.close()
    return all_departures


def select_all_stations():
    conn = sqlite3.connect('data/main.db')
    all_stations = pd.read_sql("SELECT * FROM stations", conn)
    conn.close()
    return all_stations


def select_stations_join_departures():
    conn = sqlite3.connect('data/main.db')
    stations_departures = pd.read_sql("SELECT * FROM stations s INNER JOIN departures d on s.id = d.station_id", conn)
    stations_departures['time'] = text_to_datetime(stations_departures['time'])
    conn.close()
    return stations_departures


if __name__ == '__main__':
    """
    For testing.
    """
    bus_ids = [51, 50, 31, 32, 4, 38, 30, 26, 13, 49, 40, 6, 29, 20, 10, 41, 3, 58, 7]
    stations = select_stations_join_departures()
    stations_filtered = filter_by_hour(stations, "08:00", "09:00")
    print(get_route_cells(bus_ids))
    print(stations_filtered)
