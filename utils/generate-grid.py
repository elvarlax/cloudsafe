"""
Generates the coordinates for the cells in the grid based on the provided number of cells (n), 
upper right and lower left coordinates and saves them to db in the table 'grid_cells'.
It also maps the bus routes to the grid cells and saves them in the table 'routes_cells'.
Adjust upper_right, lower_left and n based on your needs.
"""
# https://www.jpytr.com/post/analysinggeographicdatawithfolium/

import numpy
from shapely.geometry import Point, Polygon
import sqlite3
from tqdm import tqdm

upper_right = [55.716668, 12.583234]
lower_left = [55.642439, 12.501228]
n = 10

conn = sqlite3.connect('../data/main.db')
cursor = conn.cursor()
stmt = """INSERT INTO grid_cells (x_axis, y_axis, upper_left, upper_right, lower_right, lower_left) 
    VALUES (?, ?, ?, ?, ?, ?);"""

lat_steps = numpy.linspace(lower_left[0], upper_right[0], n+1)
lon_steps = numpy.linspace(lower_left[1], upper_right[1], n+1)

lat_stride = lat_steps[1] - lat_steps[0]
lon_stride = lon_steps[1] - lon_steps[0]

for lat_index, lat in enumerate(lat_steps[:-1]):
    for lon_index, lon in enumerate(lon_steps[:-1]):

        upper_left = ','.join(map(str, [lat + lat_stride, lon]))
        upper_right = ','.join(map(str, [lat + lat_stride, lon + lon_stride]))
        lower_right = ','.join(map(str, [lat, lon + lon_stride]))
        lower_left = ','.join(map(str, [lat, lon]))

        data = (lat_index, lon_index, upper_left, upper_right, lower_right, lower_left)
        cursor.execute(stmt, data)

conn.commit()

stmt_routes = 'SELECT bus_id, coordinates FROM routes_points ORDER BY bus_id, segment AND seq;'
routes = cursor.execute(stmt_routes).fetchall()

stmt_cells = 'SELECT id, upper_left, upper_right, lower_right, lower_left FROM grid_cells;'
cells = cursor.execute(stmt_cells).fetchall()

stmt_check = 'SELECT count(*) FROM routes_cells WHERE bus_id = ? AND cell_id = ?;'
stmt_insert = 'INSERT INTO routes_cells (bus_id, cell_id, seq) VALUES (?, ?, ?);'

for point_index, point in enumerate(tqdm(routes)):
    for cell in cells:
        point_obj = Point(map(float, point[1].split(',')))
        poly_obj = Polygon(map(list, [map(float, cell[1].split(',')), map(float, cell[2].split(',')), 
            map(float, cell[3].split(',')), map(float, cell[4].split(','))]))
        
        if not point_obj.within(poly_obj):
            continue

        data = (point[0], cell[0])
        counter = cursor.execute(stmt_check, data).fetchone()[0]

        if counter == 0:
            # index += 1
            data = (point[0], cell[0], 1)
            cursor.execute(stmt_insert, data)

conn.commit()
cursor.close()
conn.close()
