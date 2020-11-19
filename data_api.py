"""
This script contains functions that retrieve data from the db in order to be further processed or displayed.
"""

import sqlite3
from tqdm import tqdm
import json
import pandas as pd
import folium
import matplotlib as mpl
import matplotlib.pyplot as plt


from pathlib import Path

db = Path.cwd() / 'data/main.db'


def get_all_bus_ids():
    """
    Retrieve the list of all buses ids.

    Returns:
        list: The list of bus ids.
    """

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    stmt = 'SELECT id FROM buses;'
    bus_ids = cursor.execute(stmt).fetchall()

    cursor.close()
    conn.close()

    return [bus_id[0] for bus_id in bus_ids]


def get_routes_geojson(bus_ids, flip_coordinates=True):
    """
    Generate the routes GeoJson for the provided bus ids.
    This can be used to overlay it on top of a map in the jupyter notebooks.

    Args:
        bus_ids (list): List of the bus ids.
        flip_coordinates (bool, optional): Flip the coordinates to comply with GeoJson. Defaults to True.

    Returns:
        dict: The dict that represents the GeoJson.
    """

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    geo_json = {
        "type": "FeatureCollection",
        "properties": {
            "bus_ids": bus_ids
        },
        "features": []
    }

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
                [list(map(float, row[0].split(',')[::-1] if flip_coordinates else row[0].split(','))) for row in coordinates]
            )

        route = {
            "type": "Feature",
            "properties": {
                "bus_id": bus[0],
                "name": bus[1],
                "from": bus[2],
                "to": bus[3],
                # "stroke": "#ef500a",
                # "stroke-width": 3
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


def get_grid_geojson(bus_ids, time_range, flip_coordinates=True, stations_source='overpass'):
    """
    Generate the grid GeoJson based on the values saved in 'grid_cells' table.
    This can be used to overlay it on top of the routes in the jupyter notebooks.
    It also maps the bus stations to each cell in order to provide 'buses_count_subset' and
    'buses_count_total' in the returned GeoJson.

    Args:
        bus_ids (list): List of the bus ids.
        time_range (tuple): Time range of format ('saturday', '10:03', '11:03'). 
        flip_coordinates (bool, optional): Flip the coordinates. Defaults to True.
        stations_source (str, optional): The source of the stations' coordinates. 
        Valid values: 'overpass', 'here'. Defaults to 'overpass'.

    Returns:
        dict: The dict that represents the GeoJson.
    """

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    geo_json = {
        "type": "FeatureCollection",
        "properties": {
            "time_range": f"{time_range[0]}: {time_range[1]} - {time_range[2]}",
            "bus_ids": bus_ids
        },
        "features": []
    }
    
    if stations_source == 'overpass':
        stmt_buses_subset = f"""SELECT cell_id, count(*) FROM departures d, stations_cells_overpass sc WHERE d.station_id = sc.station_id 
            AND bus_id IN ({','.join(['?'] * len(bus_ids))}) AND day = ? AND time BETWEEN ? AND ? GROUP BY cell_id;"""
        stmt_buses_all = f"""SELECT cell_id, count(*) FROM departures d, stations_cells_overpass sc WHERE d.station_id = sc.station_id
            AND day = ? GROUP BY cell_id;""" # AND time BETWEEN ? AND ?
    
    elif stations_source == 'here':
        stmt_buses_subset = f"""SELECT cell_id, count(*) FROM departures d, stations_cells_here sc WHERE d.station_id = sc.station_id 
            AND bus_id IN ({','.join(['?'] * len(bus_ids))}) AND day = ? AND time BETWEEN ? AND ? GROUP BY cell_id;"""
        stmt_buses_all = f"""SELECT cell_id, count(*) FROM departures d, stations_cells_here sc WHERE d.station_id = sc.station_id
            AND day = ? GROUP BY cell_id;""" # AND time BETWEEN ? AND ?
   
    else:
        raise Exception('Invalid stations_source!')

    buses_count_subset = dict(cursor.execute(stmt_buses_subset, (*bus_ids, *time_range)).fetchall())

    buses_count_total = dict(cursor.execute(stmt_buses_all, [time_range[0]]).fetchall())

    stmt = 'SELECT id, x_axis, y_axis, upper_left, upper_right, lower_right, lower_left FROM grid_cells;'

    for cell in cursor.execute(stmt).fetchall():
        coordinates_list = [
            list(map(float, cell[3].split(',')[::-1] if flip_coordinates else cell[3].split(','))),
            list(map(float, cell[4].split(',')[::-1] if flip_coordinates else cell[4].split(','))),
            list(map(float, cell[5].split(',')[::-1] if flip_coordinates else cell[5].split(','))),
            list(map(float, cell[6].split(',')[::-1] if flip_coordinates else cell[6].split(',')))
        ]

        subset = buses_count_subset[cell[0]] if cell[0] in buses_count_subset else 0
        total = buses_count_total[cell[0]] if cell[0] in buses_count_total else 0

        cell_color = plt.cm.get_cmap('Blues')(subset / total if total else 0)
        cell_color = mpl.colors.to_hex(cell_color)

        cell = {
            "type": "Feature",
            "properties": {
                "matrix_coordinates": f"({cell[1]},{cell[2]})",
                "buses_count_subset": subset,
                "buses_count_total": total,
                "fillColor": cell_color, #"#67000d"
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


def get_route_cells(bus_ids):
    """
    Get the list with all the cells in the grid that the buses pass through.
    The cells are in a tuple format where the first element is the x-axis and the second is the y-axis in the grid matrix.
    It can be used to calculate the covered area in the optimization algorithm by generating the grid matrix first.

    Args:
        bus_ids (list): List of the bus ids.

    Returns:
        dict: The dict that contains a list of cells for each bus id. The cells are not necessarily ordered.
    """

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

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
    conn = sqlite3.connect(db)
    all_departures = pd.read_sql("SELECT * FROM departures", conn)
    conn.close()
    return all_departures


def select_all_stations():
    conn = sqlite3.connect(db)
    all_stations = pd.read_sql("SELECT * FROM stations", conn)
    conn.close()
    return all_stations


def select_stations_join_departures():
    conn = sqlite3.connect(db)
    stations_departures = pd.read_sql("SELECT * FROM stations s INNER JOIN departures d on s.id = d.station_id", conn)
    stations_departures['time'] = text_to_datetime(stations_departures['time'])
    conn.close()
    return stations_departures


if __name__ == '__main__':
    bus_ids = ['23']
    # bus_ids = [51]
    print(json.dumps(get_grid_geojson(bus_ids, ('weekday', '05:00', '10:00'))))
    # print(json.dumps(get_routes_geojson(bus_ids)))
    # bus_ids = [51, 50, 31, 32, 4, 38, 30, 26, 13, 49, 40, 6, 29, 20, 10, 41, 3, 58, 7]
    # stations = select_stations_join_departures()
    # stations_filtered = filter_by_hour(stations, "08:00", "09:00")
    # print(get_route_cells(bus_ids))
    # print(stations_filtered)
