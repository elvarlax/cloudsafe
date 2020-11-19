import geojson
from tqdm import tqdm
import sqlite3
from pathlib import Path

db = Path.cwd().parent / 'data/main.db'


def get_routes():
    """
    Extracts the buses and routes from bus-routes.geojson and saves them 
    to the db in the tables 'buses' and 'routes_points'.

    Args:
        None
    
    Returns:
        None
    """

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    stmt_delete = 'DELETE FROM buses;'
    cursor.execute(stmt_delete)
    stmt_delete = 'DELETE FROM routes_points;'
    cursor.execute(stmt_delete)
    
    with open('../data/bus-routes.geojson', 'r') as f:
        gj = geojson.load(f)

    buses = []

    for item in gj['features']:
        try:
            if item['properties']['network'] not in ['Movia', 'Takst Sjælland']:
                continue
            if item['properties']['route'] != 'bus':
                continue
            bus = {}
            bus['name'] = item['properties']['ref']
        except:
            continue
        try:
            bus['from'] = item['properties']['from']
        except:
            bus['from'] = None
        try:
            bus['to'] = item['properties']['to']
        except:
            bus['to'] = None
        bus['route'] = []
        
        if item['geometry']['type'] == 'LineString':
            segment = []
            for point in item['geometry']['coordinates']:
                segment.append([point[1], point[0]])
            bus['route'].append(segment)
        elif item['geometry']['type'] == 'MultiLineString':
            for subroute in item['geometry']['coordinates']:
                segment = []
                for point in subroute:
                    segment.append(point[::-1])
                bus['route'].append(segment)
        buses.append(bus)

    stmt_buses = """INSERT INTO buses (name, from_station_name, to_station_name)
            VALUES (?, ?, ?);"""

    stmt_routes = """INSERT INTO routes_points (bus_id, segment, coordinates, seq)
            VALUES (?, ?, ?, ?);"""

    for bus in tqdm(buses):
        data = (bus['name'], bus['from'], bus['to'])
        cursor.execute(stmt_buses, data)
        bus_id = cursor.lastrowid
        for segment_index, segment in enumerate(bus['route']):
            for point_index, point in enumerate(segment):
                data = (bus_id, segment_index+1, ','.join(map(str, point)), point_index+1)
                cursor.execute(stmt_routes, data)

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    get_routes()
