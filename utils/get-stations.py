import sqlite3
import geojson
from tqdm import tqdm
from pathlib import Path

db = Path.cwd().parent / 'data/main.db'


def get_stations():
    """
    Extracts the bus stations from bus-stations.geojson and saves them to db in table 'stations'.

    Args:
        None
    
    Returns:
        None
    """

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    stmt_delete = 'DELETE FROM stations;'
    cursor.execute(stmt_delete)

    stmt_insert = """INSERT INTO stations (id_overpass, name_overpass, coordinates_overpass)
            VALUES (?, ?, ?);"""

    with open('../data/bus-stations.geojson', 'r') as f:
        gj = geojson.load(f)

    for item in tqdm(gj['features']):
        try:
            name = item['properties']['name']
        except:
            name = None
        data = (item['id'], name, ','.join(map(str, item['geometry']['coordinates'][::-1])))
        cursor.execute(stmt_insert, data)

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    get_stations()
