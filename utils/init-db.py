import sqlite3
from pathlib import Path

db = Path.cwd().parent / 'data/main.db'


def init_db():
    """
    Creates the needed tables in the sqlite db main.db.

    Args:
        None
    
    Returns:
        None
    """

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    stmt_drop = 'DROP TABLE IF EXISTS stations;'
    cursor.execute(stmt_drop)

    stmt_create = """CREATE TABLE stations (id INTEGER PRIMARY KEY, id_overpass TEXT NOT NULL UNIQUE, 
            name_overpass TEXT, coordinates_overpass TEXT NOT NULL UNIQUE, id_here TEXT UNIQUE, 
            name_here TEXT, coordinates_here TEXT UNIQUE, no_data INTEGER, duplicate INTEGER);"""

    cursor.execute(stmt_create)

    stmt_drop = 'DROP TABLE IF EXISTS buses;'
    cursor.execute(stmt_drop)

    stmt_create = """CREATE TABLE buses (id INTEGER PRIMARY KEY, name TEXT NOT NULL, 
            from_station_id INTEGER, to_station_id INTEGER, from_station_name TEXT,
            to_station_name TEXT, frequency_weekday INTEGER, frequency_saturday INTEGER,
            frequency_sunday INTEGER,
            FOREIGN KEY(from_station_id) REFERENCES stations(id),
            FOREIGN KEY(to_station_id) REFERENCES stations(id));"""

    cursor.execute(stmt_create)

    stmt_drop = 'DROP TABLE IF EXISTS departures;'
    cursor.execute(stmt_drop)

    stmt_create = """CREATE TABLE departures (id INTEGER PRIMARY KEY, station_id INTEGER NOT NULL, 
            bus TEXT NOT NULL, headsign TEXT, bus_id INTEGER REFERENCES buses(id), day TEXT NOT NULL, 
            time TEXT NOT NULL, FOREIGN KEY(station_id) REFERENCES stations(id));"""

    cursor.execute(stmt_create)

    stmt_drop = 'DROP TABLE IF EXISTS routes_points;'
    cursor.execute(stmt_drop)

    stmt_create = """CREATE TABLE routes_points (id INTEGER PRIMARY KEY, bus_id INTEGER NOT NULL, 
            segment INTEGER NOT NULL, coordinates TEXT NOT NULL, seq INTEGER NOT NULL,
            FOREIGN KEY(bus_id) REFERENCES buses(id));"""

    cursor.execute(stmt_create)

    stmt_drop = 'DROP TABLE IF EXISTS grid_cells;'
    cursor.execute(stmt_drop)

    stmt_create = """CREATE TABLE grid_cells (id INTEGER PRIMARY KEY, x_axis INTEGER NOT NULL, 
            y_axis INTEGER NOT NULL, upper_left TEXT NOT NULL, upper_right TEXT NOT NULL, 
            lower_right TEXT NOT NULL, lower_left TEXT NOT NULL);"""

    cursor.execute(stmt_create)

    stmt_drop = 'DROP TABLE IF EXISTS routes_cells;'
    cursor.execute(stmt_drop)

    stmt_create = """CREATE TABLE routes_cells (id INTEGER PRIMARY KEY, bus_id INTEGER NOT NULL, 
            cell_id INTEGER NOT NULL, seq INTEGER NOT NULL,
            FOREIGN KEY(bus_id) REFERENCES buses(id),
            FOREIGN KEY(cell_id) REFERENCES grid_cells(id));"""

    cursor.execute(stmt_create)

    stmt_drop = 'DROP TABLE IF EXISTS stations_cells_overpass;'
    cursor.execute(stmt_drop)

    stmt_create = """CREATE TABLE stations_cells_overpass (id INTEGER PRIMARY KEY, station_id INTEGER NOT NULL, 
            cell_id INTEGER NOT NULL, FOREIGN KEY(station_id) REFERENCES stations(id),
            FOREIGN KEY(cell_id) REFERENCES grid_cells(id));"""

    cursor.execute(stmt_create)

    stmt_drop = 'DROP TABLE IF EXISTS stations_cells_here;'
    cursor.execute(stmt_drop)

    stmt_create = """CREATE TABLE stations_cells_here (id INTEGER PRIMARY KEY, station_id INTEGER NOT NULL, 
            cell_id INTEGER NOT NULL, FOREIGN KEY(station_id) REFERENCES stations(id),
            FOREIGN KEY(cell_id) REFERENCES grid_cells(id));"""

    cursor.execute(stmt_create)

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    init_db()
