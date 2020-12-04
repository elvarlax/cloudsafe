"""
This script contains functions that retrieve data from the db in order to be further processed or displayed.
"""

import datetime
import json
import sqlite3
from pathlib import Path

import folium
import matplotlib as mpl
import matplotlib.pyplot as plt
from datetimerange import DateTimeRange
from folium.plugins import TimestampedGeoJson

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

    for bus_id, name, from_station, to_station in buses:
        stmt = 'SELECT DISTINCT segment FROM routes_points WHERE bus_id = ? ORDER BY segment;'
        segments = cursor.execute(stmt, [bus_id]).fetchall()
        coordinates_list = []
        for segment in segments:
            stmt_sub = 'SELECT coordinates FROM routes_points WHERE bus_id = ? AND segment = ? ORDER BY segment, seq;'
            coordinates = cursor.execute(stmt_sub, (bus_id, segment[0])).fetchall()
            coordinates_list.append(
                [list(map(float, row[0].split(',')[::-1] if flip_coordinates else row[0].split(','))) for row in coordinates]
            )

        route = {
            "type": "Feature",
            "properties": {
                "bus_id": bus_id,
                "name": name,
                "from": from_station,
                "to": to_station
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
    For each hour in the time interval it will generate a grid.
    It also maps the bus stations to each cell in order to provide 'buses_count_subset' and
    'buses_count_total' in the returned GeoJson, that are the frequency of buses (counted at a single station)
    for the given bus_ids in each cell and the frequency of all buses in each cell. This can be used as a ratio
    to show the percentage of the subset in terms of max frequencies.

    Args:
        bus_ids (list): List of the bus ids.
        time_range (tuple): Time range in format ('saturday', '10:03', '11:03').
        flip_coordinates (bool, optional): Flip the coordinates. Defaults to True.
        stations_source (str, optional): The source of the stations' coordinates. 
        Valid values: 'overpass', 'here'. Defaults to 'overpass'.

    Returns:
        dict: The dict that represents the GeoJson.
    """

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    if stations_source == 'overpass':
        stmt_buses_subset = f"""SELECT cell_id, substr(time, 0, 3) AS interval, count(*) FROM departures d, stations_cells_overpass sc 
            WHERE d.station_id = sc.station_id AND bus_id IN ({','.join(['?'] * len(bus_ids))}) AND day = ? AND time BETWEEN ? AND ? 
            GROUP BY cell_id, interval;"""

        stmt_buses_all = f"""SELECT cell_id, substr(time, 0, 3) as interval, count(*) FROM departures d, stations_cells_overpass sc 
            WHERE d.station_id = sc.station_id AND day = ? AND time BETWEEN ? AND ? GROUP BY cell_id, interval;"""

    elif stations_source == 'here':
        stmt_buses_subset = f"""SELECT cell_id, substr(time, 0, 3) as interval, count(*) FROM departures d, stations_cells_here sc 
            WHERE d.station_id = sc.station_id AND bus_id IN ({','.join(['?'] * len(bus_ids))}) AND day = ? AND time BETWEEN ? AND ? 
            GROUP BY cell_id, interval;"""

        stmt_buses_all = f"""SELECT cell_id, substr(time, 0, 3) as interval, count(*) FROM departures d, stations_cells_here sc 
            WHERE d.station_id = sc.station_id AND day = ? AND time BETWEEN ? AND ? GROUP BY cell_id, interval;"""

    else:
        cursor.close()
        conn.close()
        raise Exception('Invalid stations_source!')

    buses_count_subset = {}
    for cell, interval, count in cursor.execute(stmt_buses_subset, (*bus_ids, *time_range)).fetchall():
        d = {}
        d[interval] = count
        buses_count_subset.setdefault(cell, {}).update(d)

    buses_count_total = {}
    for cell, interval, count in cursor.execute(stmt_buses_all, [*time_range]).fetchall():
        d = {}
        d[interval] = count
        buses_count_total.setdefault(cell, {}).update(d)

    geo_json = {
        "type": "FeatureCollection",
        "properties": {
            "time_range": f"{time_range[0]}: {time_range[1]} - {time_range[2]}",
            "bus_ids": bus_ids
        },
        "features": []
    }

    tr = DateTimeRange(*time_range[-2:])

    stmt = 'SELECT id, x_axis, y_axis, upper_left, upper_right, lower_right, lower_left FROM grid_cells;'

    for cell_id, x_axis, y_axis, u_left, u_right, l_right, l_left in cursor.execute(stmt).fetchall():
        coordinates_list = [
            list(map(float, u_left.split(',')[::-1] if flip_coordinates else u_left.split(','))),
            list(map(float, u_right.split(',')[::-1] if flip_coordinates else u_right.split(','))),
            list(map(float, l_right.split(',')[::-1] if flip_coordinates else l_right.split(','))),
            list(map(float, l_left.split(',')[::-1] if flip_coordinates else l_left.split(',')))
        ]

        for time in tr.range(datetime.timedelta(hours=1)):
            time_hours = time.strftime('%H')
            if cell_id in buses_count_subset and time_hours in buses_count_subset[cell_id]:
                subset = buses_count_subset[cell_id][time_hours]
            else:
                subset = 0

            if cell_id in buses_count_total and time_hours in buses_count_total[cell_id]:
                total = buses_count_total[cell_id][time_hours]
            else:
                total = 0

            perc = subset / total if total else 1
            cell_color = plt.cm.get_cmap('Reds')(perc * 5)  # 5 - to increase the shade
            cell_color = mpl.colors.to_hex(cell_color)
            perc = round(perc * 100, 2)

            cell = {
                "type": "Feature",
                "properties": {
                    "matrix_coordinates": f"({x_axis},{y_axis})",
                    "buses_count_subset": subset,
                    "buses_count_total": total,
                    "popup": f'({x_axis}, {y_axis}) {subset}/{total} ({perc}%)',
                    # "fillColor": cell_color,
                    "style": {
                        'fillColor': cell_color,
                        'color': 'black',
                        'weight': 0.5,
                        'dashArray': '5',
                        'fillOpacity': 0.5
                    },
                    "time": "2020-10-10T" + time.strftime('%H:%M') + ":00"
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
    for bus_id, x_axis, y_axis in route_cells:
        if bus_id not in route_cells_agg.keys():
            route_cells_agg[bus_id] = []
        route_cells_agg[bus_id].append((x_axis, y_axis))

    cursor.close()
    conn.close()

    return route_cells_agg


if __name__ == '__main__':
    bus_ids = [13, 6]
    get_grid_geojson(bus_ids, ('weekday', '01:00', '10:00'))
    data = get_routes_geojson([10])

    with open('anotherone.json', 'w') as f:
        json.dump(data, f)

    print(json.dumps(get_routes_geojson(bus_ids)))

    bus_ids = [51, 50, 31, 32, 4, 38, 30, 26, 13, 49, 40, 6, 29, 20, 10, 41, 3, 58, 7]
    routes_gj = get_routes_geojson(bus_ids)
    grid_gj = get_grid_geojson(bus_ids, ('weekday', '00:00', '23:59'), stations_source='here')
    m = folium.Map(location=[55.6795535, 12.542231], zoom_start=13, tiles='OpenStreetMap')
    routes = folium.GeoJson(routes_gj, name='routes', style_function=lambda feature: {
        'color': 'blue'
    })

    grid = TimestampedGeoJson(
        grid_gj,
        period='PT1H',
        duration='PT1M',
        date_options='HH:mm',
        auto_play=False,
        max_speed=1,
        loop=False,
        time_slider_drag_update=True,
        loop_button=True
    )

    routes.add_to(m)
    grid.add_to(m)

    folium.features.GeoJsonTooltip(fields=['bus_id', 'name'], aliases=['id', 'bus']).add_to(routes)
    folium.LayerControl().add_to(m)

    print(m)
