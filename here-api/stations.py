import sqlite3
import geojson
from tqdm import tqdm

with open('bus-stops.geojson', 'r') as f:
    gj = geojson.load(f)

conn = sqlite3.connect('main.db')
cursor = conn.cursor()

stmt = """INSERT INTO stations (id_overpass, name_overpass, coordinates_overpass)
        VALUES (?, ?, ?);"""

for item in tqdm(gj['features']):
    try:
        name = item['properties']['name']
    except:
        name = None
    data = (item['id'], name, ','.join(map(str, item['geometry']['coordinates'][::-1])))
    cursor.execute(stmt, data)

conn.commit()
cursor.close()
conn.close()
