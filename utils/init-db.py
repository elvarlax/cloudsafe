"""
Creates the needed tables in the sqlite db main.db.
"""

import sqlite3

conn = sqlite3.connect('../data/main.db')
cursor = conn.cursor()

stmt = """CREATE TABLE stations (id INTEGER PRIMARY KEY, id_overpass TEXT NOT NULL UNIQUE, 
        name_overpass TEXT, coordinates_overpass TEXT NOT NULL UNIQUE, id_here TEXT UNIQUE, 
        name_here TEXT, coordinates_here TEXT UNIQUE, no_data INTEGER, duplicate INTEGER);"""

cursor.execute(stmt)

stmt = """CREATE TABLE departures (id INTEGER PRIMARY KEY, station_id INTEGER NOT NULL, 
        bus TEXT NOT NULL, headsign TEXT, day TEXT NOT NULL, time TEXT NOT NULL, 
        FOREIGN KEY(station_id) REFERENCES stations(id));"""

cursor.execute(stmt)

stmt = """CREATE TABLE buses (id INTEGER PRIMARY KEY, name TEXT NOT NULL, 
        from_station_id INTEGER, to_station_id INTEGER, from_station_name TEXT,
        to_station_name TEXT, frequency_weekday INTEGER, frequency_saturday INTEGER,
        frequency_sunday INTEGER,
        FOREIGN KEY(from_station_id) REFERENCES stations(id),
        FOREIGN KEY(to_station_id) REFERENCES stations(id));"""

cursor.execute(stmt)

stmt = """CREATE TABLE routes_points (id INTEGER PRIMARY KEY, bus_id INTEGER NOT NULL, 
        segment INTEGER NOT NULL, coordinates TEXT NOT NULL, seq INTEGER NOT NULL,
        FOREIGN KEY(bus_id) REFERENCES buses(id));"""

cursor.execute(stmt)

stmt = """CREATE TABLE grid_cells (id INTEGER PRIMARY KEY, x_axis INTEGER NOT NULL, 
        y_axis INTEGER NOT NULL, upper_left TEXT NOT NULL, upper_right TEXT NOT NULL, 
        lower_right TEXT NOT NULL, lower_left TEXT NOT NULL);"""

cursor.execute(stmt)

stmt = """CREATE TABLE routes_cells (id INTEGER PRIMARY KEY, bus_id INTEGER NOT NULL, 
        cell_id INTEGER NOT NULL, seq INTEGER NOT NULL,
        FOREIGN KEY(bus_id) REFERENCES buses(id),
        FOREIGN KEY(cell_id) REFERENCES grid_cells(id));"""

cursor.execute(stmt)

conn.commit()
cursor.close()
conn.close()
