import sqlite3
import numpy
from shapely.geometry import Point, Polygon
from tqdm import tqdm
from pathlib import Path

db = Path.cwd().parent / 'data/main.db'


def generate_grid(upper_right, lower_left, n):
    """
    Generates the coordinates for the cells in the grid based on the provided number of cells (n), 
    upper right and lower left coordinates and saves them to db in the table 'grid_cells'.
    It also maps the bus routes and the bus stations to the grid cells and saves them in the 
    table 'routes_cells' and 'stations_cells_*' respectively.
    Since the stations are linked to 2 (sometimes) different sets of (nearby) coordinates because of the data 
    merge between Overpass and Here, there are 2 table 'stations_cells_overpass' and 'stations_cells_here'.
    They can be both used in get_grid_geojson() of data_api.py.

    Args:
        upper_right (list of float): The upper right GPS coordinates of the grid.
        lower_left (list of float): The lower left GPS coordinates of the grid.
        n (int): The granularity of the grid expressed in total number of cells.
    
    Returns:
        None
    
    Ref:
        https://www.jpytr.com/post/analysinggeographicdatawithfolium/
    """

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    stmt_delete = 'DELETE FROM grid_cells;'
    cursor.execute(stmt_delete)
    stmt_delete = 'DELETE FROM routes_cells;'
    cursor.execute(stmt_delete)
    stmt_delete = 'DELETE FROM stations_cells_overpass;'
    cursor.execute(stmt_delete)
    stmt_delete = 'DELETE FROM stations_cells_here;'
    cursor.execute(stmt_delete)

    stmt_insert = """INSERT INTO grid_cells (x_axis, y_axis, upper_left, upper_right, lower_right, lower_left) 
        VALUES (?, ?, ?, ?, ?, ?);"""

    lat_steps = numpy.linspace(lower_left[0], upper_right[0], n + 1)
    lon_steps = numpy.linspace(lower_left[1], upper_right[1], n + 1)

    lat_stride = lat_steps[1] - lat_steps[0]
    lon_stride = lon_steps[1] - lon_steps[0]

    for lat_index, lat in enumerate(lat_steps[:-1]):
        for lon_index, lon in enumerate(lon_steps[:-1]):
            upper_left = ','.join(map(str, [lat + lat_stride, lon]))
            upper_right = ','.join(map(str, [lat + lat_stride, lon + lon_stride]))
            lower_right = ','.join(map(str, [lat, lon + lon_stride]))
            lower_left = ','.join(map(str, [lat, lon]))

            data = (lat_index, lon_index, upper_left, upper_right, lower_right, lower_left)
            cursor.execute(stmt_insert, data)

    stmt_routes = 'SELECT bus_id, coordinates FROM routes_points ORDER BY bus_id, segment AND seq;'
    routes = cursor.execute(stmt_routes).fetchall()

    stmt_cells = 'SELECT id, upper_left, upper_right, lower_right, lower_left FROM grid_cells;'
    cells = cursor.execute(stmt_cells).fetchall()

    stmt_check = 'SELECT count(*) FROM routes_cells WHERE bus_id = ? AND cell_id = ?;'
    stmt_insert = 'INSERT INTO routes_cells (bus_id, cell_id, seq) VALUES (?, ?, ?);'

    for point in enumerate(tqdm(routes)):
        for cell in cells:
            point_obj = Point(map(float, point[1].split(',')))
            poly_obj = Polygon(map(list, [
                map(float, cell[1].split(',')), 
                map(float, cell[2].split(',')), 
                map(float, cell[3].split(',')), 
                map(float, cell[4].split(','))
            ]))
            
            if point_obj.within(poly_obj):
                data = (point[0], cell[0])
                counter = cursor.execute(stmt_check, data).fetchone()[0]
                if counter == 0:
                    # index += 1
                    data = (point[0], cell[0], 1) # sets seq = 1 because the order of segments is wrong anyway
                    cursor.execute(stmt_insert, data)
                break

    stmt_stations_overpass = 'SELECT id, coordinates_overpass FROM stations WHERE no_data = 0 AND duplicate = 0;'
    stations_overpass = cursor.execute(stmt_stations_overpass).fetchall()

    stmt_insert_overpass = 'INSERT INTO stations_cells_overpass (station_id, cell_id) VALUES (?, ?);'

    for station in tqdm(stations_overpass):
        for cell in cells:
            point_obj = Point(map(float, station[1].split(',')))
            poly_obj = Polygon(map(list, [
                map(float, cell[1].split(',')), 
                map(float, cell[2].split(',')), 
                map(float, cell[3].split(',')), 
                map(float, cell[4].split(','))
            ]))

            if point_obj.within(poly_obj):
                data = (station[0], cell[0])
                cursor.execute(stmt_insert_overpass, data)
                break

    stmt_stations_here = 'SELECT id, coordinates_here FROM stations WHERE no_data = 0 AND duplicate = 0;'
    stations_here = cursor.execute(stmt_stations_here).fetchall()

    stmt_insert_here = 'INSERT INTO stations_cells_here (station_id, cell_id) VALUES (?, ?);'

    for station in tqdm(stations_here):
        for cell in cells:
            point_obj = Point(map(float, station[1].split(',')))
            poly_obj = Polygon(map(list, [
                map(float, cell[1].split(',')), 
                map(float, cell[2].split(',')), 
                map(float, cell[3].split(',')), 
                map(float, cell[4].split(','))
            ]))

            if point_obj.within(poly_obj):
                data = (station[0], cell[0])
                cursor.execute(stmt_insert_here, data)
                break

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    upper_right = [55.716668, 12.583234]
    lower_left = [55.642439, 12.501228]
    n = 10
    generate_grid(upper_right, lower_left, n)
